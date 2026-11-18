package lakewire.coordinator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Topic and cluster reads, plus the {@code sl_create_topic} / {@code sl_delete_topic} writes. */
public final class TopicDirectory implements ClusterView {

    public static final long DEFAULT_RETENTION_MS = 7L * 24 * 60 * 60 * 1000;

    private final Coordinator coordinator;

    public TopicDirectory(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public TopicMutation createTopic(String name, int partitions, long retentionMs, boolean validateOnly) {
        String sql = "SELECT error_code, topic_id FROM sl_create_topic(?, ?, ?, ?)";
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            statement.setInt(2, partitions);
            statement.setLong(3, retentionMs);
            statement.setBoolean(4, validateOnly);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new CoordinatorException("sl_create_topic returned no row");
                }
                short error = rows.getShort(1);
                long id = rows.getLong(2);
                Long topicId = rows.wasNull() ? null : id;
                return new TopicMutation(error, topicId);
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("create topic failed", failure);
        }
    }

    public short deleteTopic(String name) {
        String sql = "SELECT sl_delete_topic(?)";
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new CoordinatorException("sl_delete_topic returned no row");
                }
                return rows.getShort(1);
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("delete topic failed", failure);
        }
    }

    public List<StoredTopic> topics() {
        String sql = """
            SELECT id, name, partitions, (config->>'retention.ms')::bigint
            FROM topics
            WHERE deleted_at IS NULL
            ORDER BY name
            """;
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rows = statement.executeQuery()) {
            List<StoredTopic> topics = new ArrayList<>();
            while (rows.next()) {
                long retention = rows.getLong(4);
                if (rows.wasNull()) {
                    retention = DEFAULT_RETENTION_MS;
                }
                topics.add(new StoredTopic(rows.getLong(1), rows.getString(2), rows.getInt(3), retention));
            }
            return List.copyOf(topics);
        } catch (SQLException failure) {
            throw new CoordinatorException("list topics failed", failure);
        }
    }

    public List<LiveBroker> liveBrokers() {
        String sql = """
            SELECT id, zone, endpoint
            FROM brokers
            WHERE lease_expires_at > now()
            ORDER BY id
            """;
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rows = statement.executeQuery()) {
            List<LiveBroker> brokers = new ArrayList<>();
            while (rows.next()) {
                int nodeId = parseNodeId(rows.getString(1));
                if (nodeId < 0) {
                    continue;
                }
                String endpoint = rows.getString(3);
                int colon = endpoint.lastIndexOf(':');
                if (colon <= 0 || colon == endpoint.length() - 1) {
                    throw new CoordinatorException("broker endpoint must be host:port");
                }
                String host = endpoint.substring(0, colon);
                int port;
                try {
                    port = Integer.parseInt(endpoint.substring(colon + 1));
                } catch (NumberFormatException cause) {
                    throw new CoordinatorException("broker endpoint must be host:port", cause);
                }
                brokers.add(new LiveBroker(nodeId, host, port, rows.getString(2)));
            }
            return List.copyOf(brokers);
        } catch (SQLException failure) {
            throw new CoordinatorException("list brokers failed", failure);
        }
    }

    public String clusterId() {
        String sql = "SELECT cluster_id FROM cluster WHERE id = 1";
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rows = statement.executeQuery()) {
            if (!rows.next()) {
                throw new CoordinatorException("cluster id is missing");
            }
            return rows.getString(1);
        } catch (SQLException failure) {
            throw new CoordinatorException("cluster id lookup failed", failure);
        }
    }

    private static int parseNodeId(String id) {
        try {
            return Integer.parseInt(id);
        } catch (NumberFormatException ignored) {
            return -1;
        }
    }
}
