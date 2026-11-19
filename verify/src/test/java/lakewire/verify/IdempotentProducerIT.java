package lakewire.verify;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import lakewire.broker.BrokerRuntime;
import lakewire.broker.config.BrokerConfig;
import lakewire.broker.write.WriteSettings;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.storage.LocalFsObjectStore;
import lakewire.testkit.LakeWireContainers;
import lakewire.verify.checker.I3SequenceOrderChecker;
import lakewire.verify.checker.I5NoDuplicateChecker;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

import static org.assertj.core.api.Assertions.assertThat;

class IdempotentProducerIT {

    @Test
    void theDefaultProducerSurvivesDropsAndPostgresLatency() throws Exception {
        requireDocker();
        int brokerPort = freePort();
        Path data = Files.createTempDirectory("lakewire-idem");
        try (PostgreSQLContainer postgres = LakeWireContainers.postgres()) {
            postgres.start();
            org.testcontainers.Testcontainers.exposeHostPorts(brokerPort, postgres.getMappedPort(5432));
            try (ToxiproxyContainer toxiproxy = LakeWireContainers.toxiproxy()) {
                toxiproxy.start();
                ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
                Proxy pg = client.createProxy(
                    "pg", "0.0.0.0:8666", "host.testcontainers.internal:" + postgres.getMappedPort(5432));
                Proxy broker = client.createProxy(
                    "broker", "0.0.0.0:8667", "host.testcontainers.internal:" + brokerPort);
                String jdbc = "jdbc:postgresql://127.0.0.1:" + toxiproxy.getMappedPort(8666)
                    + "/lakewire?connectTimeout=30&socketTimeout=30";
                int advertised = toxiproxy.getMappedPort(8667);
                LocalFsObjectStore store = new LocalFsObjectStore(data);
                BrokerConfig config = new BrokerConfig(
                    "0.0.0.0", brokerPort, 4_000_000, 1, "a", "127.0.0.1", advertised, data);
                try (BrokerRuntime runtime = new BrokerRuntime(
                    config,
                    CoordinatorConfig.of(jdbc, postgres.getUsername(), postgres.getPassword()),
                    store,
                    new WriteSettings(20, WriteSettings.DEFAULT_OBJECT_MAX_BYTES, WriteSettings.DEFAULT_BUFFER_MAX_BYTES, 4, "")
                )) {
                    runtime.start();
                    String bootstrap = "127.0.0.1:" + advertised;
                    try (Admin admin = Admin.create(Map.of(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000",
                        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "20000"
                    ))) {
                        admin.createTopics(List.of(new NewTopic("idem", 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
                    }
                    pg.toxics().latency("pg-lag", ToxicDirection.DOWNSTREAM, 80);
                    AtomicBoolean running = new AtomicBoolean(true);
                    Thread faults = Thread.ofPlatform().name("idem-faults").start(() -> drop(broker, running));
                    try {
                        produce(bootstrap, 20_000);
                    } finally {
                        running.set(false);
                        faults.interrupt();
                        faults.join(5_000);
                        broker.enable();
                    }
                    assertThat(consume(bootstrap, 20_000)).isEqualTo(20_000);
                    I3SequenceOrderChecker.assertMonotonic(
                        store, jdbc, postgres.getUsername(), postgres.getPassword(), "idem");
                    I5NoDuplicateChecker.assertUnique(
                        store, jdbc, postgres.getUsername(), postgres.getPassword(), "idem");
                }
            }
        } finally {
            delete(data);
        }
    }

    private static void produce(String bootstrap, int records) {
        List<Exception> errors = new CopyOnWriteArrayList<>();
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        config.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
        config.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 200);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 20_000);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "idem");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            for (int i = 0; i < records; i++) {
                producer.send(new ProducerRecord<>("idem", 0, "k-" + i, "v-" + i), (metadata, failure) -> {
                    if (failure != null) {
                        errors.add(failure);
                    }
                });
            }
            producer.flush();
        }
        assertThat(errors).isEmpty();
    }

    private static int consume(String bootstrap, int expected) {
        List<TopicPartition> assigned = List.of(new TopicPartition("idem", 0));
        int read = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
            ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000"
        ))) {
            consumer.assign(assigned);
            consumer.seekToBeginning(assigned);
            long deadline = System.nanoTime() + Duration.ofMinutes(1).toNanos();
            while (read < expected && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> ignored : consumer.poll(Duration.ofMillis(500))) {
                    read++;
                }
            }
        }
        return read;
    }

    /** Healthy for 2s, then a short drop. Backoff stays under the healthy window so metadata can return. */
    private static void drop(Proxy broker, AtomicBoolean running) {
        try {
            Thread.sleep(2_000);
            while (running.get()) {
                broker.disable();
                Thread.sleep(80);
                broker.enable();
                Thread.sleep(2_000);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
            // the proxy closes with the test
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void requireDocker() {
        try {
            DockerClientFactory.instance().client();
        } catch (IllegalStateException failure) {
            throw new IllegalStateException("Docker is required for IdempotentProducerIT. Start Docker and rerun.", failure);
        }
    }

    private static void delete(Path root) throws Exception {
        if (root == null) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            });
        }
    }
}
