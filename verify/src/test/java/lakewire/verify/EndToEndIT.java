package lakewire.verify;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import lakewire.broker.BrokerRuntime;
import lakewire.broker.config.BrokerConfig;
import lakewire.broker.write.WriteSettings;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.storage.S3ObjectStore;
import lakewire.storage.S3StorageConfig;
import lakewire.testkit.LakeWireContainers;
import lakewire.verify.checker.AckLog;
import lakewire.verify.checker.I1AckedReadableChecker;
import lakewire.verify.checker.I2ContiguityChecker;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.assertj.core.api.Assertions.assertThat;

class EndToEndIT {

    @Test
    void aProducerAndAnAssignedConsumerRoundTripFiftyThousandRecords() throws Exception {
        requireDocker();
        try (GenericContainer<?> seaweed = LakeWireContainers.seaweedFs()) {
            seaweed.start();
            String endpoint = "http://" + seaweed.getHost() + ":"
                + seaweed.getMappedPort(LakeWireContainers.SEAWEEDFS_S3_PORT);
            createBucket(endpoint);
            try (PostgreSQLContainer postgres = LakeWireContainers.postgres()) {
                postgres.start();
                Path dataDir = Files.createTempDirectory("lakewire-e2e");
                String advertised = sharedAddress();
                S3ObjectStore store = new S3ObjectStore(new S3StorageConfig(
                    "lakewire", "us-east-1", URI.create(endpoint), true, true, "lakewire", "lakewire"));
                BrokerConfig config = new BrokerConfig(
                    "0.0.0.0", 0, 4_000_000, 1, "a", advertised, 0, dataDir);
                try (BrokerRuntime runtime = new BrokerRuntime(
                    config,
                    CoordinatorConfig.of(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()),
                    store,
                    new WriteSettings(20, WriteSettings.DEFAULT_OBJECT_MAX_BYTES, WriteSettings.DEFAULT_BUFFER_MAX_BYTES, 4, "")
                )) {
                    runtime.start();
                    String bootstrap = advertised + ":" + runtime.port();
                    try (Admin admin = Admin.create(Map.of(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000",
                        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000",
                        AdminClientConfig.RETRIES_CONFIG, "0"
                    ))) {
                        admin.createTopics(List.of(
                            new NewTopic("events", 6, (short) 1),
                            new NewTopic("lines", 1, (short) 1)
                        )).all().get(20, TimeUnit.SECONDS);
                    }
                    List<AckLog> acked = produce(bootstrap, "events", 50_000, 6);
                    List<AckLog> read = consume(bootstrap, "events", 6, 50_000);
                    I1AckedReadableChecker.assertReadable(acked, read);
                    I2ContiguityChecker.assertContiguous(
                        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), "events");
                    consoleProduce(bootstrap);
                    assertThat(consoleConsume(bootstrap)).contains("alpha").contains("beta").contains("gamma");
                    assertThat(kcat(advertised, runtime.port())).contains("alpha").contains("beta").contains("gamma");
                    try (var files = Files.list(dataDir)) {
                        assertThat(files.toList()).isEmpty();
                    }
                } finally {
                    delete(dataDir);
                }
            }
        }
    }

    private static List<AckLog> produce(String bootstrap, String topic, int records, int partitions) {
        List<AckLog> acked = new CopyOnWriteArrayList<>();
        List<Exception> errors = new CopyOnWriteArrayList<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(bootstrap))) {
            for (int i = 0; i < records; i++) {
                String key = "k-" + i;
                String value = "v-" + i;
                int partition = i % partitions;
                producer.send(new ProducerRecord<>(topic, partition, key, value), (metadata, failure) -> {
                    if (failure != null) {
                        errors.add(failure);
                    } else {
                        acked.add(new AckLog(key, value.hashCode(), metadata.partition(), metadata.offset()));
                    }
                });
            }
            producer.flush();
        }
        assertThat(errors).isEmpty();
        assertThat(acked).hasSize(records);
        return acked;
    }

    private static void consoleProduce(String bootstrap) {
        String command = "printf '%s\\n' alpha beta gamma | /opt/kafka/bin/kafka-console-producer.sh"
            + " --bootstrap-server " + bootstrap
            + " --topic lines"
            + " --command-property enable.idempotence=false"
            + " --command-property acks=all";
        oneShot("apache/kafka:4.4.0", "sh", "-c", command);
    }

    private static String consoleConsume(String bootstrap) {
        return oneShot(
            "apache/kafka:4.4.0",
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server", bootstrap,
            "--topic", "lines",
            "--partition", "0",
            "--offset", "earliest",
            "--max-messages", "3",
            "--timeout-ms", "20000"
        );
    }

    private static List<AckLog> consume(String bootstrap, String topic, int partitions, int expected) {
        List<TopicPartition> assigned = new ArrayList<>();
        for (int part = 0; part < partitions; part++) {
            assigned.add(new TopicPartition(topic, part));
        }
        List<AckLog> read = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
            ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000",
            ConsumerConfig.CLIENT_ID_CONFIG, "e2e"
        ))) {
            consumer.assign(assigned);
            consumer.seekToBeginning(assigned);
            long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
            while (read.size() < expected && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    read.add(new AckLog(
                        record.key(),
                        record.value().hashCode(),
                        record.partition(),
                        record.offset()
                    ));
                }
            }
        }
        return read;
    }

    private static String kcat(String host, int port) {
        return oneShot(
            "edenhill/kcat:1.7.1",
            "-C", "-b", host + ":" + port,
            "-t", "lines", "-p", "0", "-o", "beginning", "-e", "-c", "3"
        );
    }

    private static String oneShot(String image, String... command) {
        try (GenericContainer<?> tool = new GenericContainer<>(DockerImageName.parse(image))
            .withCommand(command)
            .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(60)))) {
            tool.start();
            return tool.getLogs();
        }
    }

    /**
     * An address the Windows client and a container can both open. Metadata
     * advertises this host, and kcat follows it, so loopback does not work.
     */
    private static String sharedAddress() throws IOException {
        List<String> candidates = new ArrayList<>();
        Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
        while (nics.hasMoreElements()) {
            NetworkInterface nic = nics.nextElement();
            if (!nic.isUp() || nic.isLoopback()) {
                continue;
            }
            String label = (nic.getName() + " " + nic.getDisplayName()).toLowerCase(Locale.ROOT);
            if (label.contains("warp") || label.contains("bluetooth") || label.contains("loopback")
                || label.contains("vethernet") || label.contains("hyper-v") || label.contains("wsl")
                || label.contains("docker") || label.contains("virtual")) {
                continue;
            }
            for (var binding : nic.getInterfaceAddresses()) {
                if (binding.getAddress() instanceof Inet4Address address
                    && !address.isLoopbackAddress()
                    && !address.isLinkLocalAddress()) {
                    candidates.add(address.getHostAddress());
                }
            }
        }
        if (candidates.isEmpty()) {
            throw new IllegalStateException("No site-local IPv4 is available to advertise to Docker");
        }
        try (ServerSocket server = new ServerSocket(0, 16, InetAddress.getByName("0.0.0.0"))) {
            int port = server.getLocalPort();
            List<String> reachable = new ArrayList<>();
            for (String candidate : candidates) {
                if (connects(candidate, port)) {
                    reachable.add(candidate);
                }
            }
            if (reachable.isEmpty()) {
                throw new IllegalStateException("Host cannot connect to " + candidates);
            }
            String script = "for ip in " + String.join(" ", reachable) + "; do "
                + "nc -z -w 2 \"$ip\" " + port + " && echo \"$ip\" && exit 0; done; exit 1";
            String logs = oneShot("postgres:18.7-alpine", "sh", "-c", script);
            for (String line : logs.split("\\R")) {
                String trimmed = line.trim();
                if (reachable.contains(trimmed)) {
                    return trimmed;
                }
            }
            throw new IllegalStateException("Docker could not reach " + reachable + ": " + logs);
        }
    }

    private static boolean connects(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            return true;
        } catch (IOException failure) {
            return false;
        }
    }

    private static Map<String, Object> producerConfig(String bootstrap) {
        Map<String, Object> config = new java.util.HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 90_000);
        config.put(ProducerConfig.RETRIES_CONFIG, 2);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "e2e");
        return config;
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
            throw new IllegalStateException("Docker is required for EndToEndIT. Start Docker and rerun.", failure);
        }
    }

    private static void delete(Path dataDir) throws Exception {
        if (dataDir == null) {
            return;
        }
        try (var paths = Files.walk(dataDir)) {
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
