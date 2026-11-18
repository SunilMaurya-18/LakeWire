package lakewire.storage;

import java.nio.ByteBuffer;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;

/** Test helper that builds one magic-2 record batch. */
public final class RecordChunks {

    private RecordChunks() {
    }

    public static byte[] oneBatch(Compression compression, int records, long timestamp) {
        ByteBuffer buffer = ByteBuffer.allocate(256 + records * 64);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, compression, TimestampType.CREATE_TIME, 0L);
        for (int i = 0; i < records; i++) {
            builder.append(timestamp + i, new byte[] {(byte) i}, new byte[] {(byte) (i + 1), 2, 3, 4});
        }
        MemoryRecords written = builder.build();
        ByteBuffer bytes = written.buffer();
        byte[] copy = new byte[bytes.remaining()];
        bytes.get(copy);
        return copy;
    }
}
