package lakewire.verify;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lakewire.broker.BrokerRuntime;
import lakewire.broker.config.BrokerConfig;
import lakewire.broker.write.WriteSettings;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.storage.LocalFsObjectStore;
import lakewire.testkit.LakeWireContainers;
import lakewire.verify.checker.I7NoDanglingRefChecker;
import lakewire.verify.checker.I8NoLeakChecker;
import lakewire.workers.WorkerConfig;
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
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

class RetentionStormIT {

    @Test
    @Timeout(180)
    void shortRetentionYieldsDataOrOffsetOutOfRangeAndI7I8Hold() throws Exception {
        requireDocker();
        try (PostgreSQLContainer postgres = LakeWireContainers.postgres()) {
            postgres.start();
            Path dataDir = Files.createTempDirectory("lakewire-storm");
            LocalFsObjectStore store = new LocalFsObjectStore(dataDir);
            WorkerConfig workers = new WorkerConfig(
                true, 30_000, 150, 150, 100, 200, 50,
                7L * 24 * 60 * 60 * 1000, 60_000, 50, 100, "");
            BrokerConfig config = new BrokerConfig(
                "127.0.0.1", 0, 4_000_000, 1, "a", "127.0.0.1", 0, dataDir);
            try (BrokerRuntime runtime = new BrokerRuntime(
                config,
                CoordinatorConfig.of(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()),
                store,
                new WriteSettings(10, WriteSettings.DEFAULT_OBJECT_MAX_BYTES, WriteSettings.DEFAULT_BUFFER_MAX_BYTES, 2, ""),
                workers
            )) {
                runtime.start();
                String bootstrap = "127.0.0.1:" + runtime.port();
                NewTopic topic = new NewTopic("storm", 1, (short) 1);
                topic.configs(Map.of("retention.ms", "8000"));
                try (Admin admin = Admin.create(Map.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000",
                    AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "20000"
                ))) {
                    admin.createTopics(List.of(topic)).all().get(20, TimeUnit.SECONDS);
                }
                produce(bootstrap, "old-", System.currentTimeMillis() - Duration.ofHours(1).toMillis(), 12);
                produce(bootstrap, "new-", System.currentTimeMillis(), 12);
                Awaitility.await().atMost(Duration.ofSeconds(20)).until(() ->
                    logStart(postgres) > 0);
                Set<String> values = readSurviving(bootstrap);
                assertThat(values).isNotEmpty();
                assertThat(values).allSatisfy(value -> assertThat(value).startsWith("new-"));
                I7NoDanglingRefChecker.assertHolds(
                    store, postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                I8NoLeakChecker.assertHolds(
                    store,
                    postgres.getJdbcUrl(),
                    postgres.getUsername(),
                    postgres.getPassword(),
                    "",
                    Duration.ofMillis(workers.orphanTtlMs()),
                    Duration.ofMillis(workers.gcPeriodMs())
                );
            }
        }
    }

    private static void produce(String bootstrap, String prefix, long timestamp, int records) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            for (int i = 0; i < records; i++) {
                producer.send(new ProducerRecord<>(
                    "storm", 0, timestamp, prefix + i, prefix + i));
            }
            producer.flush();
        }
    }

    private static Set<String> readSurviving(String bootstrap) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 20_000);
        TopicPartition partition = new TopicPartition("storm", 0);
        Set<String> values = new HashSet<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.assign(List.of(partition));
            consumer.seek(partition, 0);
            try {
                collect(consumer, values);
            } catch (OffsetOutOfRangeException ignored) {
                consumer.seekToBeginning(List.of(partition));
                collect(consumer, values);
            }
        }
        return values;
    }

    private static void collect(KafkaConsumer<String, String> consumer, Set<String> values) {
        for (int i = 0; i < 8 && values.isEmpty(); i++) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                values.add(record.value());
            }
        }
    }

    private static long logStart(PostgreSQLContainer postgres) throws Exception {
        try (var connection = java.sql.DriverManager.getConnection(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             var statement = connection.createStatement();
             var rows = statement.executeQuery(
                 "SELECT log_start_offset FROM partitions LIMIT 1")) {
            if (!rows.next()) {
                return 0;
            }
            return rows.getLong(1);
        }
    }

    private static void requireDocker() {
        try {
            if (!DockerClientFactory.instance().isDockerAvailable()) {
                fail("Docker is required for RetentionStormIT but is not available. Start Docker Desktop and rerun.");
            }
        } catch (RuntimeException failure) {
            fail("Docker is required for RetentionStormIT but is not available. Start Docker Desktop and rerun.",
                failure);
        }
    }
}
