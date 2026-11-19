package lakewire.workers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import lakewire.coordinator.CommitEntry;
import lakewire.coordinator.CommitRequest;
import lakewire.coordinator.CommitResult;
import lakewire.coordinator.Coordinator;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.coordinator.TopicDirectory;
import lakewire.storage.LocalFsObjectStore;
import lakewire.storage.ObjectStore;
import lakewire.testkit.LakeWireContainers;
import org.apache.kafka.common.protocol.Errors;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.junit.jupiter.api.Assertions.fail;

final class WorkerHarness implements AutoCloseable {

    final PostgreSQLContainer postgres;
    final Coordinator coordinator;
    final ObjectStore store;
    final WorkerJobs jobs;
    final WorkerMetrics metrics;
    final TopicDirectory topics;
    final Connection admin;

    private WorkerHarness(
        PostgreSQLContainer postgres,
        Coordinator coordinator,
        ObjectStore store,
        WorkerJobs jobs,
        WorkerMetrics metrics,
        TopicDirectory topics,
        Connection admin
    ) {
        this.postgres = postgres;
        this.coordinator = coordinator;
        this.store = store;
        this.jobs = jobs;
        this.metrics = metrics;
        this.topics = topics;
        this.admin = admin;
    }

    static WorkerHarness start(WorkerConfig config) throws Exception {
        requireDocker();
        PostgreSQLContainer postgres = LakeWireContainers.postgres();
        postgres.start();
        Coordinator coordinator = new Coordinator(CoordinatorConfig.of(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()));
        coordinator.start();
        Path dir = Files.createTempDirectory("lakewire-workers");
        ObjectStore store = new LocalFsObjectStore(dir);
        WorkerMetrics metrics = new WorkerMetrics();
        WorkerJobs jobs = new WorkerJobs(coordinator, store, config, metrics);
        Connection admin = DriverManager.getConnection(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        admin.setAutoCommit(true);
        return new WorkerHarness(
            postgres, coordinator, store, jobs, metrics, new TopicDirectory(coordinator), admin);
    }

    long register(String id) {
        return new lakewire.coordinator.BrokerRegistry(coordinator).register(id, "a", id + ":9092");
    }

    long createTopic(String name, int partitions, long retentionMs) {
        return topics.createTopic(name, partitions, retentionMs, false).topicId();
    }

    void intent(String key, String broker, long epoch) {
        new lakewire.coordinator.BrokerRegistry(coordinator).createUploadIntent(key, broker, epoch);
    }

    List<CommitResult> commit(String broker, long epoch, String key, byte[] body, CommitEntry... entries) {
        intent(key, broker, epoch);
        store.put(key, body).join();
        return coordinator.commit(new CommitRequest(
            UUID.randomUUID(), key, body.length, broker, epoch, List.of(entries)));
    }

    void ageIntent(String key) {
        execute("UPDATE upload_intents SET created_at = now() - interval '1 hour' WHERE object_key = '" + key + "'");
    }

    String queryString(String sql) {
        try (Statement statement = admin.createStatement();
             ResultSet rows = statement.executeQuery(sql)) {
            if (!rows.next()) {
                return null;
            }
            return rows.getString(1);
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    long queryLong(String sql) {
        try (Statement statement = admin.createStatement();
             ResultSet rows = statement.executeQuery(sql)) {
            if (!rows.next()) {
                throw new IllegalStateException("no row for " + sql);
            }
            return rows.getLong(1);
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    void execute(String sql) {
        try (Statement statement = admin.createStatement()) {
            statement.execute(sql);
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    void terminate(int pid) {
        try (PreparedStatement statement = admin.prepareStatement("SELECT pg_terminate_backend(?)")) {
            statement.setInt(1, pid);
            statement.execute();
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    @Override
    public void close() {
        try {
            admin.close();
        } catch (SQLException ignored) {
            // container stopping
        }
        store.close();
        coordinator.close();
        postgres.stop();
    }

    static void requireDocker() {
        try {
            if (!DockerClientFactory.instance().isDockerAvailable()) {
                fail("Docker is required for worker tests but is not available. Start Docker Desktop and rerun.");
            }
        } catch (RuntimeException failure) {
            fail("Docker is required for worker tests but is not available. Start Docker Desktop and rerun.", failure);
        }
    }

    static short notLeader() {
        return Errors.NOT_LEADER_OR_FOLLOWER.code();
    }
}
