package lakewire.coordinator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Read models for the admin REST API. Writes stay on {@code sl_*}. */
public final class AdminCatalog {

    private final Coordinator coordinator;

    public AdminCatalog(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public String clusterId() {
        return new TopicDirectory(coordinator).clusterId();
    }

    public List<BrokerSnapshot> brokers() {
        String sql = """
            SELECT id, zone, endpoint, epoch, lease_expires_at > now()
            FROM brokers
            ORDER BY id
            """;
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rows = statement.executeQuery()) {
            List<BrokerSnapshot> brokers = new ArrayList<>();
            while (rows.next()) {
                brokers.add(new BrokerSnapshot(
                    rows.getString(1),
                    rows.getString(2),
                    rows.getString(3),
                    rows.getLong(4),
                    rows.getBoolean(5)
                ));
            }
            return List.copyOf(brokers);
        } catch (SQLException failure) {
            throw new CoordinatorException("list brokers failed", failure);
        }
    }

    public List<TopicSummary> topics() {
        String sql = """
            SELECT t.name, t.partitions, COALESCE((t.config->>'retention.ms')::bigint, ?),
                   p.part, p.next_offset, p.log_start_offset, COALESCE(SUM(s.byte_len), 0)
            FROM topics t
            JOIN partitions p ON p.topic_id = t.id
            LEFT JOIN segments s ON s.topic_id = p.topic_id AND s.part = p.part
            WHERE t.deleted_at IS NULL
            GROUP BY t.name, t.partitions, t.config, p.part, p.next_offset, p.log_start_offset
            ORDER BY t.name, p.part
            """;
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, TopicDirectory.DEFAULT_RETENTION_MS);
            try (ResultSet rows = statement.executeQuery()) {
                Map<String, TopicAccumulator> topics = new LinkedHashMap<>();
                while (rows.next()) {
                    String name = rows.getString(1);
                    int partitionCount = rows.getInt(2);
                    long retentionMs = rows.getLong(3);
                    int part = rows.getInt(4);
                    long hw = rows.getLong(5);
                    long logStart = rows.getLong(6);
                    long bytes = rows.getLong(7);
                    TopicAccumulator topic = topics.computeIfAbsent(
                        name, ignored -> new TopicAccumulator(name, partitionCount, retentionMs));
                    topic.partitions.add(new PartitionSummary(part, hw, logStart, bytes));
                    topic.bytes += bytes;
                }
                List<TopicSummary> result = new ArrayList<>(topics.size());
                for (TopicAccumulator topic : topics.values()) {
                    result.add(new TopicSummary(
                        topic.name, topic.partitionsCount, topic.retentionMs, topic.bytes,
                        List.copyOf(topic.partitions)));
                }
                return List.copyOf(result);
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("list topic stats failed", failure);
        }
    }

    public List<SegmentRow> segments(String topic, int part) {
        String sql = """
            SELECT s.base_offset, s.last_offset, s.object_id, s.byte_start, s.byte_len, s.n_batches, s.max_ts
            FROM segments s
            JOIN topics t ON t.id = s.topic_id
            WHERE t.name = ? AND t.deleted_at IS NULL AND s.part = ?
            ORDER BY s.base_offset
            """;
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, topic);
            statement.setInt(2, part);
            try (ResultSet rows = statement.executeQuery()) {
                List<SegmentRow> segments = new ArrayList<>();
                while (rows.next()) {
                    segments.add(new SegmentRow(
                        rows.getLong(1),
                        rows.getLong(2),
                        rows.getObject(3, UUID.class),
                        rows.getLong(4),
                        rows.getInt(5),
                        rows.getShort(6),
                        rows.getLong(7)
                    ));
                }
                return List.copyOf(segments);
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("list segments failed", failure);
        }
    }

    public boolean topicExists(String name) {
        String sql = "SELECT 1 FROM topics WHERE name = ? AND deleted_at IS NULL";
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("topic lookup failed", failure);
        }
    }

    public Optional<ObjectSnapshot> object(UUID id) {
        String sql = """
            SELECT id, key, size_bytes, live_refs, live_bytes, state, delete_after
            FROM objects
            WHERE id = ?
            """;
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, id);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return Optional.empty();
                }
                long size = rows.getLong(3);
                long liveBytes = rows.getLong(5);
                Timestamp deleteAfter = rows.getTimestamp(7);
                Instant until = deleteAfter == null ? null : deleteAfter.toInstant();
                double utilisation = size == 0 ? 0.0 : (double) liveBytes / (double) size;
                return Optional.of(new ObjectSnapshot(
                    rows.getObject(1, UUID.class),
                    rows.getString(2),
                    size,
                    rows.getInt(4),
                    liveBytes,
                    rows.getString(6),
                    until,
                    utilisation
                ));
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("object lookup failed", failure);
        }
    }

    private static final class TopicAccumulator {
        private final String name;
        private final int partitionsCount;
        private final long retentionMs;
        private final List<PartitionSummary> partitions = new ArrayList<>();
        private long bytes;

        private TopicAccumulator(String name, int partitionsCount, long retentionMs) {
            this.name = name;
            this.partitionsCount = partitionsCount;
            this.retentionMs = retentionMs;
        }
    }
}
