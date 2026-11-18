package lakewire.broker.read;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import lakewire.coordinator.CoordinatorException;

/** Partition watermarks and segment rows. The clock and the offsets are Postgres's. */
public final class LogCatalog implements WatermarkSource, SegmentSource {

    private final DataSource dataSource;

    public LogCatalog(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Watermark watermark(long topicId, int part) {
        String sql = """
            SELECT next_offset, log_start_offset
            FROM partitions
            WHERE topic_id = ? AND part = ?
            """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, topicId);
            statement.setInt(2, part);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new CoordinatorException("partition " + topicId + ":" + part + " is missing");
                }
                return new Watermark(rows.getLong(1), rows.getLong(2));
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("watermark read failed", failure);
        }
    }

    @Override
    public List<StoredSegment> segments(long topicId, int part) {
        String sql = """
            SELECT s.base_offset, s.last_offset, s.byte_start, s.byte_len, s.max_ts, o.key
            FROM segments s
            JOIN objects o ON o.id = s.object_id
            WHERE s.topic_id = ? AND s.part = ? AND o.state = 'COMMITTED'
            ORDER BY s.base_offset
            """;
        List<StoredSegment> segments = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, topicId);
            statement.setInt(2, part);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    segments.add(new StoredSegment(
                        rows.getLong(1),
                        rows.getLong(2),
                        rows.getLong(3),
                        rows.getInt(4),
                        rows.getLong(5),
                        rows.getString(6)
                    ));
                }
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("segment read failed", failure);
        }
        return List.copyOf(segments);
    }
}
