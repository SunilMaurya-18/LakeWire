package lakewire.verify.checker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** I2: committed segments cover {@code [0, next_offset)} with no hole and no overlap. */
public final class I2ContiguityChecker {

    private I2ContiguityChecker() {
    }

    public static void assertContiguous(String jdbcUrl, String user, String password, String topic) throws Exception {
        Map<Integer, long[]> bounds = new LinkedHashMap<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement statement = connection.prepareStatement("""
                 SELECT p.part, p.log_start_offset, p.next_offset
                 FROM partitions p
                 JOIN topics t ON t.id = p.topic_id
                 WHERE t.name = ? AND t.deleted_at IS NULL
                 ORDER BY p.part
                 """)) {
            statement.setString(1, topic);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    bounds.put(rows.getInt(1), new long[] {rows.getLong(2), rows.getLong(3)});
                }
            }
        }
        if (bounds.isEmpty()) {
            throw new AssertionError("topic " + topic + " has no partitions");
        }
        for (Map.Entry<Integer, long[]> partition : bounds.entrySet()) {
            List<long[]> spans = new ArrayList<>();
            try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
                 PreparedStatement statement = connection.prepareStatement("""
                     SELECT s.base_offset, s.last_offset
                     FROM segments s
                     JOIN topics t ON t.id = s.topic_id
                     WHERE t.name = ? AND s.part = ?
                     ORDER BY s.base_offset
                     """)) {
                statement.setString(1, topic);
                statement.setInt(2, partition.getKey());
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        spans.add(new long[] {rows.getLong(1), rows.getLong(2)});
                    }
                }
            }
            assertSpans(partition.getKey(), partition.getValue()[0], partition.getValue()[1], spans);
        }
    }

    /** Handmade spans: {@code [base, last]} inclusive, covering {@code [logStart, highWatermark)}. */
    public static void assertSpans(int partition, long logStart, long highWatermark, List<long[]> spans) {
        long cursor = logStart;
        for (long[] span : spans) {
            if (span[0] != cursor || span[1] < span[0]) {
                throw new AssertionError("partition " + partition + " breaks at " + cursor);
            }
            cursor = span[1] + 1;
        }
        if (cursor != highWatermark) {
            throw new AssertionError("partition " + partition + " covers " + cursor + " of " + highWatermark);
        }
    }
}
