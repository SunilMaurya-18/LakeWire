package lakewire.verify.checker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lakewire.storage.ObjectHead;
import lakewire.storage.ObjectStore;
import lakewire.storage.lkw1.BatchOffsets;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;

/** Committed records from objects, with {@code baseOffset} patched like Fetch. */
public final class StoredLog {

    private StoredLog() {
    }

    public static List<AckLog> load(ObjectStore store, String jdbcUrl, String user, String password, String topic)
        throws Exception {
        List<SegmentSlice> slices = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement statement = connection.prepareStatement("""
                 SELECT s.part, s.base_offset, s.byte_start, s.byte_len, o.key
                 FROM segments s
                 JOIN objects o ON o.id = s.object_id
                 JOIN topics t ON t.id = s.topic_id
                 WHERE t.name = ? AND o.state = 'COMMITTED'
                 ORDER BY s.part, s.base_offset, s.byte_start
                 """)) {
            statement.setString(1, topic);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    slices.add(new SegmentSlice(
                        rows.getInt(1),
                        rows.getLong(2),
                        rows.getLong(3),
                        rows.getInt(4),
                        rows.getString(5)
                    ));
                }
            }
        }
        Map<String, byte[]> objects = new HashMap<>();
        List<AckLog> records = new ArrayList<>();
        for (SegmentSlice slice : slices) {
            byte[] object = objects.computeIfAbsent(slice.key, key -> download(store, key));
            int end = Math.toIntExact(slice.byteStart + slice.byteLen);
            byte[] chunk = Arrays.copyOfRange(object, (int) slice.byteStart, end);
            byte[] patched = BatchOffsets.patch(chunk, slice.baseOffset);
            for (MutableRecordBatch batch : MemoryRecords.readableRecords(ByteBuffer.wrap(patched)).batches()) {
                for (Record logRecord : batch) {
                    String key = string(logRecord.key());
                    int hash = payloadHash(logRecord.value());
                    records.add(new AckLog(key, hash, slice.partition, logRecord.offset()));
                }
            }
        }
        return records;
    }

    private static int payloadHash(ByteBuffer value) {
        byte[] raw = new byte[value.remaining()];
        value.get(raw);
        String text = new String(raw, StandardCharsets.UTF_8);
        int third = text.indexOf(':', text.indexOf(':', text.indexOf(':') + 1) + 1);
        if (third < 0) {
            return Arrays.hashCode(raw);
        }
        return Arrays.hashCode(Arrays.copyOfRange(raw, third + 1, raw.length));
    }

    private static String string(ByteBuffer buffer) {
        if (buffer == null) {
            return "";
        }
        byte[] raw = new byte[buffer.remaining()];
        buffer.get(raw);
        return new String(raw, StandardCharsets.UTF_8);
    }

    private static byte[] download(ObjectStore store, String key) {
        try {
            ObjectHead head = store.head(key).get(30, TimeUnit.SECONDS);
            return store.getRange(key, 0, Math.toIntExact(head.size())).get(30, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new AssertionError("could not read " + key, failure);
        }
    }

    private record SegmentSlice(int partition, long baseOffset, long byteStart, int byteLen, String key) {
    }
}
