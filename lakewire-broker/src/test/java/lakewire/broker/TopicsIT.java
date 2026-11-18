package lakewire.broker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lakewire.broker.config.BrokerConfig;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.storage.LocalFsObjectStore;
import lakewire.testkit.LakeWireContainers;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicsIT {

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
    void adminCreatesListsDescribesAndDeletes() throws Exception {
        start();
        try (Admin admin = admin()) {
            admin.createTopics(List.of(new NewTopic("orders", 3, (short) 2)))
                .all()
                .get(15, TimeUnit.SECONDS);
            assertThat(admin.listTopics().names().get(15, TimeUnit.SECONDS)).contains("orders");
            var described = admin.describeTopics(List.of("orders")).allTopicNames().get(15, TimeUnit.SECONDS);
            assertThat(described.get("orders").partitions()).hasSize(3);
            assertThat(described.get("orders").partitions().get(0).leader().id()).isEqualTo(1);

            assertThatThrownBy(() -> admin.createTopics(List.of(new NewTopic("orders", 1, (short) 1)))
                .all()
                .get(15, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(TopicExistsException.class);

            admin.createTopics(
                List.of(new NewTopic("ghost", 1, (short) 1)),
                new CreateTopicsOptions().validateOnly(true)
            ).all().get(15, TimeUnit.SECONDS);
            assertThat(admin.listTopics().names().get(15, TimeUnit.SECONDS)).doesNotContain("ghost");

            assertThatThrownBy(() -> admin.createTopics(List.of(
                new NewTopic("bad", 1, (short) 1).configs(Map.of("cleanup.policy", "compact"))
            )).all().get(15, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(InvalidConfigurationException.class);

            admin.deleteTopics(List.of("orders")).all().get(15, TimeUnit.SECONDS);
        }
        try (Admin again = admin()) {
            assertThat(again.listTopics().names().get(15, TimeUnit.SECONDS)).doesNotContain("orders");
            assertThatThrownBy(() -> again.describeTopics(List.of("orders")).allTopicNames().get(15, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(UnknownTopicOrPartitionException.class);
            again.createTopics(List.of(new NewTopic("orders", 1, (short) 1))).all().get(15, TimeUnit.SECONDS);
            assertThat(again.listTopics().names().get(15, TimeUnit.SECONDS)).contains("orders");
        }
    }

    private void start() throws Exception {
        try {
            DockerClientFactory.instance().client();
        } catch (IllegalStateException failure) {
            throw new IllegalStateException("Docker is required for TopicsIT. Start Docker and rerun.", failure);
        }
        postgres = LakeWireContainers.postgres();
        postgres.start();
        dataDir = Files.createTempDirectory("lakewire-topics");
        BrokerConfig config = new BrokerConfig(
            "127.0.0.1", 0, 4_000_000, 1, "a", "127.0.0.1", 0, dataDir);
        CoordinatorConfig coordinatorConfig = CoordinatorConfig.of(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        runtime = new BrokerRuntime(config, coordinatorConfig, new LocalFsObjectStore(dataDir));
        runtime.start();
    }

    private Admin admin() {
        return Admin.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + runtime.port(),
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000",
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000",
            AdminClientConfig.RETRIES_CONFIG, "0",
            AdminClientConfig.CLIENT_ID_CONFIG, "topics-it"
        ));
    }
}
