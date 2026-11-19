package lakewire.verify.checker;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lakewire.storage.ObjectHead;
import lakewire.storage.ObjectStore;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;

/** Idempotent record sequences in committed offset order. */
final class StoredSequences {

    record Sequence(int partition, long producerId, int sequence) {
    }

    private StoredSequences() {
    }

    static List<Sequence> load(ObjectStore store, String jdbcUrl, String user, String password, String topic)
        throws Exception {
        List<SegmentSlice> slices = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement statement = connection.prepareStatement("""
                 SELECT s.part, s.byte_start, s.byte_len, o.key
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
                        rows.getInt(3),
                        rows.getString(4)
                    ));
                }
            }
        }
        Map<String, byte[]> objects = new HashMap<>();
        List<Sequence> sequences = new ArrayList<>();
        for (SegmentSlice slice : slices) {
            byte[] object = objects.computeIfAbsent(slice.key, key -> download(store, key));
            int end = Math.toIntExact(slice.byteStart + slice.byteLen);
            if (slice.byteStart < 0 || end > object.length) {
                throw new AssertionError("segment slice is outside " + slice.key);
            }
            ByteBuffer view = ByteBuffer.wrap(object, (int) slice.byteStart, slice.byteLen);
            for (MutableRecordBatch batch : MemoryRecords.readableRecords(view).batches()) {
                if (batch.producerId() < 0 || batch.baseSequence() < 0) {
                    continue;
                }
                int count = (int) (batch.nextOffset() - batch.baseOffset());
                for (int index = 0; index < count; index++) {
                    sequences.add(new Sequence(slice.partition, batch.producerId(), batch.baseSequence() + index));
                }
            }
        }
        return sequences;
    }

    private static byte[] download(ObjectStore store, String key) {
        try {
            ObjectHead head = store.head(key).get(30, TimeUnit.SECONDS);
            return store.getRange(key, 0, Math.toIntExact(head.size())).get(30, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new AssertionError("could not read " + key, failure);
        }
    }

    private record SegmentSlice(int partition, long byteStart, int byteLen, String key) {
    }
}
