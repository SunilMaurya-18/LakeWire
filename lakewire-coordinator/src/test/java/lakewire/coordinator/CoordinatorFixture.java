package lakewire.coordinator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import lakewire.testkit.LakeWireContainers;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.junit.jupiter.api.Assertions.fail;

final class CoordinatorFixture implements AutoCloseable {

    final PostgreSQLContainer postgres;
    final Coordinator coordinator;
    final BrokerRegistry registry;
    final Connection admin;

    private CoordinatorFixture(
        PostgreSQLContainer postgres,
        Coordinator coordinator,
        BrokerRegistry registry,
        Connection admin
    ) {
        this.postgres = postgres;
        this.coordinator = coordinator;
        this.registry = registry;
        this.admin = admin;
    }

    static CoordinatorFixture start() {
        requireDocker();
        PostgreSQLContainer postgres = LakeWireContainers.postgres();
        postgres.start();
        Coordinator coordinator = null;
        try {
            CoordinatorConfig config = CoordinatorConfig.of(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
            coordinator = new Coordinator(config);
            coordinator.start();
            Connection admin = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
            admin.setAutoCommit(true);
            return new CoordinatorFixture(postgres, coordinator, new BrokerRegistry(coordinator), admin);
        } catch (RuntimeException | SQLException failure) {
            if (coordinator != null) {
                coordinator.close();
            }
            postgres.stop();
            throw new IllegalStateException("coordinator fixture failed to start", failure);
        }
    }

    void truncate() {
        try (Statement statement = admin.createStatement()) {
            statement.execute("""
                TRUNCATE segments, objects, upload_intents, producer_state, producer_ids,
                         partitions, topics, brokers RESTART IDENTITY CASCADE
                """);
        } catch (SQLException failure) {
            throw new IllegalStateException("truncate failed", failure);
        }
    }

    long register(String id) {
        return registry.register(id, "az1", id + ":9092");
    }

    long createTopic(String name, int partitions) {
        try (PreparedStatement insert = admin.prepareStatement(
            "INSERT INTO topics (name, partitions) VALUES (?, ?) RETURNING id")) {
            insert.setString(1, name);
            insert.setInt(2, partitions);
            long topicId;
            try (ResultSet rows = insert.executeQuery()) {
                rows.next();
                topicId = rows.getLong(1);
            }
            try (PreparedStatement parts = admin.prepareStatement(
                "INSERT INTO partitions (topic_id, part) VALUES (?, ?)")) {
                for (int part = 0; part < partitions; part++) {
                    parts.setLong(1, topicId);
                    parts.setInt(2, part);
                    parts.addBatch();
                }
                parts.executeBatch();
            }
            return topicId;
        } catch (SQLException failure) {
            throw new IllegalStateException("create topic failed", failure);
        }
    }

    void deleteTopic(long topicId) {
        execute("UPDATE topics SET deleted_at = now() WHERE id = " + topicId);
    }

    void expireLease(String brokerId) {
        try (PreparedStatement statement = admin.prepareStatement(
            "UPDATE brokers SET lease_expires_at = now() - interval '1 second' WHERE id = ?")) {
            statement.setString(1, brokerId);
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw new IllegalStateException("expire lease failed", failure);
        }
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

    private void execute(String sql) {
        try (Statement statement = admin.createStatement()) {
            statement.execute(sql);
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    @Override
    public void close() {
        try {
            admin.close();
        } catch (SQLException ignored) {
            // The container is stopping anyway.
        }
        coordinator.close();
        postgres.stop();
    }

    private static void requireDocker() {
        try {
            if (!DockerClientFactory.instance().isDockerAvailable()) {
                fail("Docker is required for coordinator tests but is not available. Start Docker Desktop and rerun.");
            }
        } catch (RuntimeException failure) {
            fail("Docker is required for coordinator tests but is not available. Start Docker Desktop and rerun.",
                failure);
        }
    }
}
