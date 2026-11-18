package lakewire.storage.lkw1;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;

/**
 * Patches each RecordBatch v2 {@code baseOffset} in a chunk. The batch CRC does not cover
 * those 8 bytes, so {@code ensureValid()} still passes.
 */
public final class BatchOffsets {

    private BatchOffsets() {
    }

    public static byte[] patch(byte[] chunk, long baseOffset) {
        if (chunk == null || chunk.length == 0) {
            throw new IllegalArgumentException("chunk must not be empty");
        }
        byte[] copy = Arrays.copyOf(chunk, chunk.length);
        ByteBuffer buffer = ByteBuffer.wrap(copy);
        int position = 0;
        long next = baseOffset;
        while (position < copy.length) {
            if (position + 12 > copy.length) {
                throw new IllegalArgumentException("truncated batch");
            }
            int batchLength = buffer.getInt(position + 8);
            int size = batchLength + 12;
            if (batchLength < 0 || position + size > copy.length) {
                throw new IllegalArgumentException("truncated batch");
            }
            ByteBuffer view = ByteBuffer.wrap(copy, position, size);
            MutableRecordBatch batch = MemoryRecords.readableRecords(view.duplicate()).batches().iterator().next();
            int records = (int) (batch.nextOffset() - batch.baseOffset());
            buffer.putLong(position, next);
            MutableRecordBatch patched = MemoryRecords.readableRecords(
                ByteBuffer.wrap(copy, position, size).duplicate()
            ).batches().iterator().next();
            patched.ensureValid();
            next += records;
            position += size;
        }
        return copy;
    }
}
