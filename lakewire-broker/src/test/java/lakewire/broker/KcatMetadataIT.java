package lakewire.broker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lakewire.broker.config.BrokerConfig;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.storage.LocalFsObjectStore;
import lakewire.testkit.LakeWireContainers;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

class KcatMetadataIT {

    private PostgreSQLContainer postgres;
    private BrokerRuntime runtime;
    private Path dataDir;

    @AfterEach
    void stop() throws Exception {
        if (runtime != null) {
            runtime.close();
        }
        if (postgres != null) {
            postgres.stop();
        }
        if (dataDir != null) {
            Files.deleteIfExists(dataDir);
        }
    }

    @Test
    void kcatListsTheTopic() throws Exception {
        try {
            DockerClientFactory.instance().client();
        } catch (IllegalStateException failure) {
            throw new IllegalStateException("Docker is required for KcatMetadataIT. Start Docker and rerun.", failure);
        }
        postgres = LakeWireContainers.postgres();
        postgres.start();
        dataDir = Files.createTempDirectory("lakewire-kcat");
        BrokerConfig config = new BrokerConfig(
            "0.0.0.0", 0, 4_000_000, 1, "a", "127.0.0.1", 0, dataDir);
        runtime = new BrokerRuntime(
            config,
            CoordinatorConfig.of(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()),
            new LocalFsObjectStore(dataDir)
        );
        runtime.start();
        try (Admin admin = Admin.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + runtime.port(),
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000",
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000",
            AdminClientConfig.RETRIES_CONFIG, "0"
        ))) {
            admin.createTopics(List.of(new NewTopic("orders", 2, (short) 1))).all().get(15, TimeUnit.SECONDS);
        }

        org.testcontainers.Testcontainers.exposeHostPorts(runtime.port());
        try (GenericContainer<?> kcat = new GenericContainer<>(DockerImageName.parse("edenhill/kcat:1.7.1"))
            .withCommand("-L", "-b", "host.testcontainers.internal:" + runtime.port())
            .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(60)))) {
            kcat.start();
            assertThat(kcat.getLogs()).contains("orders");
        }
    }
}
