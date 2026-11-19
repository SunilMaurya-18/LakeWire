package lakewire.verify;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lakewire.verify.checker.AckLog;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Idempotent producers and assigned consumers that append JSONL histories. */
public final class Workload implements AutoCloseable {

    public static final String TOPIC = "verify";
    public static final int PARTITIONS = 3;
    public static final int PRODUCERS = 3;

    private final String bootstrap;
    private final Path acksFile;
    private final Path readsFile;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final List<HistoryEvent> acks = new CopyOnWriteArrayList<>();
    private final List<HistoryEvent> reads = new CopyOnWriteArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    public Workload(String bootstrap, Path dir) throws IOException {
        this.bootstrap = bootstrap;
        Files.createDirectories(dir);
        this.acksFile = dir.resolve("acks.jsonl");
        this.readsFile = dir.resolve("reads.jsonl");
        Files.writeString(acksFile, "");
        Files.writeString(readsFile, "");
    }

    public void start() {
        for (int i = 0; i < PRODUCERS; i++) {
            int producer = i;
            threads.add(Thread.ofVirtual().name("verify-producer-" + i).start(() -> produce(producer)));
        }
        threads.add(Thread.ofVirtual().name("verify-consumer").start(this::consume));
    }

    public List<AckLog> acked() {
        return acks.stream().map(HistoryEvent::toAckLog).toList();
    }

    public List<AckLog> read() {
        return reads.stream().map(HistoryEvent::toAckLog).toList();
    }

    /** Reads until each partition has records through {@code highWatermarks.get(p) - 1}. */
    public List<AckLog> finalRead(Map<Integer, Long> highWatermarks) {
        List<AckLog> found = new ArrayList<>();
        Map<Integer, Long> next = new HashMap<>();
        for (int part = 0; part < PARTITIONS; part++) {
            next.put(part, 0L);
        }
        List<TopicPartition> parts = partitions();
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        int empty = 0;
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig());
        try {
            assign(consumer, parts, next);
            while (System.nanoTime() < deadline) {
                if (caughtUp(next, highWatermarks)) {
                    break;
                }
                try {
                    var batch = consumer.poll(Duration.ofMillis(500));
                    if (batch.isEmpty()) {
                        empty++;
                        if (empty % 5 == 0) {
                            consumer.close();
                            consumer = new KafkaConsumer<>(consumerConfig());
                            assign(consumer, parts, next);
                        }
                        continue;
                    }
                    empty = 0;
                    for (ConsumerRecord<String, byte[]> record : batch) {
                        found.add(decode(record).toAckLog());
                        next.merge(record.partition(), record.offset() + 1, Math::max);
                    }
                } catch (RuntimeException ignored) {
                    consumer.close();
                    consumer = new KafkaConsumer<>(consumerConfig());
                    assign(consumer, parts, next);
                }
            }
        } finally {
            consumer.close();
        }
        return found;
    }

    private static void assign(
        KafkaConsumer<String, byte[]> consumer,
        List<TopicPartition> parts,
        Map<Integer, Long> next
    ) {
        consumer.assign(parts);
        for (TopicPartition part : parts) {
            consumer.seek(part, next.getOrDefault(part.partition(), 0L));
        }
    }

    private static boolean caughtUp(Map<Integer, Long> next, Map<Integer, Long> highWatermarks) {
        for (int part = 0; part < PARTITIONS; part++) {
            long hw = highWatermarks.getOrDefault(part, 0L);
            if (next.getOrDefault(part, 0L) < hw) {
                return false;
            }
        }
        return true;
    }

    private void produce(int producer) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "verify-p" + producer + ",az=a");
        config.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        config.put(ProducerConfig.RETRIES_CONFIG, 8);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 45_000);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        AtomicInteger seq = new AtomicInteger();
        Semaphore inFlight = new Semaphore(8);
        try (KafkaProducer<String, byte[]> kafka = new KafkaProducer<>(config)) {
            while (running.get()) {
                try {
                    inFlight.acquire();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
                int next = seq.getAndIncrement();
                String key = producer + "-" + next;
                byte[] payload = (producer + ":" + next + ":" + key).getBytes(StandardCharsets.UTF_8);
                byte[] value = encode(producer, next, payload);
                int hash = hash(payload);
                kafka.send(new ProducerRecord<>(TOPIC, null, key, value), (metadata, error) -> {
                    inFlight.release();
                    if (error != null) {
                        return;
                    }
                    HistoryEvent event = new HistoryEvent(
                        "ack", key, metadata.partition(), metadata.offset(), producer, next, hash);
                    acks.add(event);
                    append(acksFile, event);
                });
            }
            kafka.flush();
        }
    }

    private void consume() {
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig())) {
            List<TopicPartition> parts = partitions();
            consumer.assign(parts);
            consumer.seekToBeginning(parts);
            while (running.get()) {
                try {
                    for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(300))) {
                        HistoryEvent event = decode(record);
                        reads.add(event);
                        append(readsFile, event);
                    }
                } catch (RuntimeException ignored) {
                    // nemeses close connections; the next poll retries
                }
            }
        }
    }

    private Map<String, Object> consumerConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "verify-c");
        config.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 1_000);
        config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 20_000);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        return config;
    }

    private static List<TopicPartition> partitions() {
        List<TopicPartition> parts = new ArrayList<>();
        for (int i = 0; i < PARTITIONS; i++) {
            parts.add(new TopicPartition(TOPIC, i));
        }
        return parts;
    }

    static byte[] encode(int producer, int seq, byte[] payload) {
        String header = producer + ":" + seq + ":" + sha256(payload) + ":";
        byte[] prefix = header.getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[prefix.length + payload.length];
        System.arraycopy(prefix, 0, value, 0, prefix.length);
        System.arraycopy(payload, 0, value, prefix.length, payload.length);
        return value;
    }

    static HistoryEvent decode(ConsumerRecord<String, byte[]> record) {
        String text = new String(record.value(), StandardCharsets.UTF_8);
        int one = text.indexOf(':');
        int two = text.indexOf(':', one + 1);
        int three = text.indexOf(':', two + 1);
        int producer = Integer.parseInt(text.substring(0, one));
        int seq = Integer.parseInt(text.substring(one + 1, two));
        byte[] payload = record.value();
        int header = three + 1;
        byte[] body = java.util.Arrays.copyOfRange(payload, header, payload.length);
        return new HistoryEvent("read", record.key(), record.partition(), record.offset(), producer, seq, hash(body));
    }

    private static int hash(byte[] payload) {
        return java.util.Arrays.hashCode(payload);
    }

    private static String sha256(byte[] payload) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static synchronized void append(Path file, HistoryEvent event) {
        try {
            Files.writeString(file, event.toJson() + "\n", java.nio.file.StandardOpenOption.APPEND);
        } catch (IOException failure) {
            throw new IllegalStateException(failure);
        }
    }

    @Override
    public void close() {
        running.set(false);
        for (Thread thread : threads) {
            thread.interrupt();
            try {
                thread.join(5_000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
