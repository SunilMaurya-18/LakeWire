package lakewire.bench.e2e;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Produce → consume timestamps. Output is a laptop-labelled CSV. */
public final class E2eLatencyProbe {

    private E2eLatencyProbe() {
    }

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static Path run(String[] args) throws Exception {
        String bootstrap = "127.0.0.1:9092";
        String topic = "bench-e2e";
        int records = 2000;
        Path output = Path.of("bench", "e2e", "laptop-e2e-latency.csv");
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bootstrap" -> bootstrap = args[++i];
                case "--topic" -> topic = args[++i];
                case "--records" -> records = Integer.parseInt(args[++i]);
                case "--output" -> output = Path.of(args[++i]);
                default -> throw new IllegalArgumentException(args[i]);
            }
        }
        ensureTopic(bootstrap, topic);
        List<Long> produceMs = new ArrayList<>(records);
        List<Long> e2eMs = new ArrayList<>(records);
        Properties producer = new Properties();
        producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer.put(ProducerConfig.ACKS_CONFIG, "all");
        producer.put(ProducerConfig.LINGER_MS_CONFIG, "200");
        producer.put(ProducerConfig.CLIENT_ID_CONFIG, "bench-e2e");
        producer.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        long sentAt0 = System.currentTimeMillis();
        try (KafkaProducer<String, String> kafka = new KafkaProducer<>(producer)) {
            for (int i = 0; i < records; i++) {
                long start = System.nanoTime();
                long now = System.currentTimeMillis();
                RecordMetadata meta = kafka.send(new ProducerRecord<>(topic, 0, now, null, "p-" + i)).get();
                produceMs.add((System.nanoTime() - start) / 1_000_000L);
                if (meta == null) {
                    throw new IllegalStateException("missing metadata");
                }
            }
        }
        Properties consumer = new Properties();
        consumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer.put(ConsumerConfig.CLIENT_ID_CONFIG, "bench-e2e-c");
        consumer.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        TopicPartition partition = new TopicPartition(topic, 0);
        int got = 0;
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        try (KafkaConsumer<String, String> kafka = new KafkaConsumer<>(consumer)) {
            kafka.assign(List.of(partition));
            kafka.seekToBeginning(List.of(partition));
            while (got < records && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : kafka.poll(Duration.ofSeconds(2))) {
                    e2eMs.add(Math.max(0, System.currentTimeMillis() - record.timestamp()));
                    got++;
                    if (got >= records) {
                        break;
                    }
                }
            }
        }
        if (got < records) {
            throw new IllegalStateException("read " + got + " of " + records);
        }
        Files.createDirectories(output.getParent());
        String csv = csv(bootstrap, topic, records, produceMs, e2eMs, sentAt0);
        Files.writeString(output, csv);
        return output;
    }

    static String csv(
        String bootstrap,
        String topic,
        int records,
        List<Long> produceMs,
        List<Long> e2eMs,
        long startedEpochMs
    ) {
        Collections.sort(produceMs);
        Collections.sort(e2eMs);
        return """
            # label=laptop
            # tool=lakewire.bench.e2e.E2eLatencyProbe
            # kafka-clients=4.4.0
            # bootstrap=%s
            # topic=%s
            # records=%s
            # acks=all linger.ms=200
            # hardware=%s %s
            # os=%s
            # started_utc=%s
            metric,p50_ms,p99_ms,max_ms,count
            produce_ack,%s,%s,%s,%s
            e2e_consume,%s,%s,%s,%s
            """.formatted(
            bootstrap,
            topic,
            records,
            System.getProperty("os.arch"),
            Runtime.getRuntime().availableProcessors() + " cpus",
            System.getProperty("os.name") + " " + System.getProperty("os.version"),
            Instant.ofEpochMilli(startedEpochMs),
            percentile(produceMs, 0.50),
            percentile(produceMs, 0.99),
            produceMs.get(produceMs.size() - 1),
            produceMs.size(),
            percentile(e2eMs, 0.50),
            percentile(e2eMs, 0.99),
            e2eMs.get(e2eMs.size() - 1),
            e2eMs.size()
        );
    }

    static long percentile(List<Long> sorted, double rank) {
        if (sorted.isEmpty()) {
            return 0;
        }
        int index = Math.min(sorted.size() - 1, (int) Math.ceil(rank * sorted.size()) - 1);
        return sorted.get(Math.max(0, index));
    }

    private static void ensureTopic(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000",
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "20000"
        ))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();
        } catch (Exception ignored) {
            // topic may already exist
        }
    }
}
