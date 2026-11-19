package lakewire.broker;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lakewire.broker.config.BrokerConfig;
import lakewire.broker.write.WriteSettings;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.storage.ObjectHead;
import lakewire.storage.ObjectStore;
import lakewire.storage.S3ObjectStore;
import lakewire.storage.S3StorageConfig;
import lakewire.storage.lkw1.Lkw1Chunk;
import lakewire.storage.lkw1.Lkw1Object;
import lakewire.storage.lkw1.Lkw1Reader;
import lakewire.storage.lkw1.Lkw1Writer;
import lakewire.testkit.LakeWireContainers;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.assertj.core.api.Assertions.assertThat;

/** Postgres plus a broker. SeaweedFS is optional. The caller closes it. */
final class ProduceCluster implements AutoCloseable {

    private final PostgreSQLContainer postgres;
    private final GenericContainer<?> seaweed;
    private final Path dataDir;
    private final ObjectStore store;
    private final BrokerRuntime runtime;

    private ProduceCluster(
        PostgreSQLContainer postgres,
        GenericContainer<?> seaweed,
        Path dataDir,
        ObjectStore store,
        BrokerRuntime runtime
    ) {
        this.postgres = postgres;
        this.seaweed = seaweed;
        this.dataDir = dataDir;
        this.store = store;
        this.runtime = runtime;
    }

    static ProduceCluster open(ObjectStore store, WriteSettings settings) throws Exception {
        requireDocker();
        PostgreSQLContainer postgres = LakeWireContainers.postgres();
        postgres.start();
        Path dataDir = Files.createTempDirectory("lakewire-produce");
        BrokerConfig config = new BrokerConfig(
            "127.0.0.1", 0, 4_000_000, 1, "a", "127.0.0.1", 0, dataDir);
        CoordinatorConfig coordinator = CoordinatorConfig.of(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        BrokerRuntime runtime = new BrokerRuntime(config, coordinator, store, settings);
        runtime.start();
        return new ProduceCluster(postgres, null, dataDir, store, runtime);
    }

    static ProduceCluster seaweed(WriteSettings settings) throws Exception {
        requireDocker();
        GenericContainer<?> seaweed = LakeWireContainers.seaweedFs();
        seaweed.start();
        String endpoint = "http://" + seaweed.getHost() + ":" + seaweed.getMappedPort(LakeWireContainers.SEAWEEDFS_S3_PORT);
        createBucket(endpoint);
        S3StorageConfig s3 = new S3StorageConfig(
            "lakewire", "us-east-1", URI.create(endpoint), true, true, "lakewire", "lakewire");
        PostgreSQLContainer postgres = LakeWireContainers.postgres();
        postgres.start();
        Path dataDir = Files.createTempDirectory("lakewire-produce");
        BrokerConfig config = new BrokerConfig(
            "127.0.0.1", 0, 4_000_000, 1, "a", "127.0.0.1", 0, dataDir);
        CoordinatorConfig coordinator = CoordinatorConfig.of(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        ObjectStore store = new S3ObjectStore(s3);
        BrokerRuntime runtime = new BrokerRuntime(config, coordinator, store, settings);
        runtime.start();
        return new ProduceCluster(postgres, seaweed, dataDir, store, runtime);
    }

    int port() {
        return runtime.port();
    }

    String jdbcUrl() {
        return postgres.getJdbcUrl();
    }

    String user() {
        return postgres.getUsername();
    }

    String password() {
        return postgres.getPassword();
    }

    Path dataDir() {
        return dataDir;
    }

    void createTopic(String name, int partitions) throws Exception {
        try (Admin admin = Admin.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + runtime.port(),
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000",
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000",
            AdminClientConfig.RETRIES_CONFIG, "0",
            AdminClientConfig.CLIENT_ID_CONFIG, "produce-it"
        ))) {
            admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1)))
                .all()
                .get(15, TimeUnit.SECONDS);
        }
    }

    int produce(String topic, String acks, int records, int partitions) {
        Map<String, Object> config = new java.util.HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + runtime.port());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, acks);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        config.put(ProducerConfig.RETRIES_CONFIG, 2);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 15_000);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "produce-it");
        AtomicInteger acked = new AtomicInteger();
        List<Exception> errors = new CopyOnWriteArrayList<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            for (int i = 0; i < records; i++) {
                producer.send(new ProducerRecord<>(topic, i % partitions, null, "v-" + i), (metadata, failure) -> {
                    if (failure != null) {
                        errors.add(failure);
                    } else {
                        acked.incrementAndGet();
                    }
                });
            }
            producer.flush();
        }
        assertThat(errors).isEmpty();
        return acked.get();
    }

    void assertCommitted(String topic, int records, int partitions) throws Exception {
        Map<Integer, Long> highWatermarks = highWatermarks(topic);
        assertThat(highWatermarks).hasSize(partitions);
        long covered = 0;
        for (int part = 0; part < partitions; part++) {
            long next = highWatermarks.get(part);
            covered += next;
            List<SegmentRow> rows = segments(topic, part);
            long cursor = 0;
            for (SegmentRow row : rows) {
                assertThat(row.baseOffset()).isEqualTo(cursor);
                assertThat(row.lastOffset()).isGreaterThanOrEqualTo(row.baseOffset());
                assertThat(row.state()).isEqualTo("COMMITTED");
                cursor = row.lastOffset() + 1;
            }
            assertThat(cursor).isEqualTo(next);
        }
        assertThat(covered).isEqualTo(records);
        assertObjectsMatchSegments(topic);
    }

    long nextOffset(String topic, int part) throws Exception {
        Long offset = highWatermarks(topic).get(part);
        return offset == null ? 0 : offset;
    }

    private void assertObjectsMatchSegments(String topic) throws Exception {
        Map<String, List<SegmentRow>> byKey = new LinkedHashMap<>();
        for (SegmentRow row : segments(topic, null)) {
            byKey.computeIfAbsent(row.key(), ignored -> new ArrayList<>()).add(row);
        }
        assertThat(byKey).isNotEmpty();
        for (Map.Entry<String, List<SegmentRow>> entry : byKey.entrySet()) {
            ObjectHead head = store.head(entry.getKey()).get(30, TimeUnit.SECONDS);
            byte[] body = store.getRange(entry.getKey(), 0, (int) head.size()).get(30, TimeUnit.SECONDS);
            Lkw1Object object = Lkw1Reader.read(body);
            long[] starts = Lkw1Writer.chunkStarts(object.brokerId(), object.chunks());
            assertThat(object.chunks()).hasSize(entry.getValue().size());
            for (int i = 0; i < object.chunks().size(); i++) {
                Lkw1Chunk chunk = object.chunks().get(i);
                long start = starts[i];
                SegmentRow match = entry.getValue().stream()
                    .filter(row -> row.part() == chunk.part() && row.byteStart() == start)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no segment for " + entry.getKey() + " @" + start));
                assertThat(match.byteLen()).isEqualTo(chunk.records().length);
                assertThat(match.lastOffset() - match.baseOffset() + 1).isEqualTo(chunk.nRecords());
            }
        }
    }

    private Map<Integer, Long> highWatermarks(String topic) throws Exception {
        String sql = """
            SELECT p.part, p.next_offset
            FROM partitions p
            JOIN topics t ON t.id = p.topic_id
            WHERE t.name = ? AND t.deleted_at IS NULL
            ORDER BY p.part
            """;
        Map<Integer, Long> offsets = new LinkedHashMap<>();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, topic);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    offsets.put(rows.getInt(1), rows.getLong(2));
                }
            }
        }
        return offsets;
    }

    private List<SegmentRow> segments(String topic, Integer part) throws Exception {
        String sql = """
            SELECT o.key, o.state, s.part, s.base_offset, s.last_offset, s.byte_start, s.byte_len
            FROM segments s
            JOIN objects o ON o.id = s.object_id
            JOIN topics t ON t.id = s.topic_id
            WHERE t.name = ? AND (?::int IS NULL OR s.part = ?)
            ORDER BY s.part, s.base_offset
            """;
        List<SegmentRow> rows = new ArrayList<>();
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, topic);
            if (part == null) {
                statement.setNull(2, java.sql.Types.INTEGER);
                statement.setNull(3, java.sql.Types.INTEGER);
            } else {
                statement.setInt(2, part);
                statement.setInt(3, part);
            }
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    rows.add(new SegmentRow(
                        result.getString(1),
                        result.getString(2),
                        result.getInt(3),
                        result.getLong(4),
                        result.getLong(5),
                        result.getLong(6),
                        result.getInt(7)
                    ));
                }
            }
        }
        return rows;
    }

    private Connection connect() throws Exception {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        failure = closeStep(failure, () -> {
            if (runtime != null) {
                runtime.close();
            }
        });
        failure = closeStep(failure, () -> {
            if (seaweed != null) {
                seaweed.stop();
            }
        });
        failure = closeStep(failure, () -> {
            if (postgres != null) {
                postgres.stop();
            }
        });
        failure = closeStep(failure, this::deleteDataDir);
        if (failure != null) {
            throw failure;
        }
    }

    private void deleteDataDir() throws java.io.IOException {
        if (dataDir == null) {
            return;
        }
        try (var paths = Files.walk(dataDir)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (java.io.IOException ignored) {
                    // best-effort cleanup of the temp directory
                }
            });
        }
    }

    private static RuntimeException closeStep(RuntimeException first, Step step) {
        try {
            step.run();
            return first;
        } catch (Exception thrown) {
            if (thrown instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (first != null) {
                return first;
            }
            return thrown instanceof RuntimeException runtime
                ? runtime
                : new IllegalStateException("failed to close the produce cluster", thrown);
        }
    }

    @FunctionalInterface
    private interface Step {
        void run() throws Exception;
    }

    private static void createBucket(String endpoint) {
        try (S3Client client = S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("lakewire", "lakewire")))
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .forcePathStyle(true)
            .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
            .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
            .build()) {
            client.createBucket(CreateBucketRequest.builder().bucket("lakewire").build());
        }
    }

    private static void requireDocker() {
        try {
            DockerClientFactory.instance().client();
        } catch (IllegalStateException failure) {
            throw new IllegalStateException("Docker is required for produce tests. Start Docker and rerun.", failure);
        }
    }

    private record SegmentRow(
        String key,
        String state,
        int part,
        long baseOffset,
        long lastOffset,
        long byteStart,
        int byteLen
    ) {
    }

    static WriteSettings fast() {
        return new WriteSettings(20, WriteSettings.DEFAULT_OBJECT_MAX_BYTES, WriteSettings.DEFAULT_BUFFER_MAX_BYTES, 4, "");
    }

    static Duration commitWait() {
        return Duration.ofSeconds(15);
    }
}
