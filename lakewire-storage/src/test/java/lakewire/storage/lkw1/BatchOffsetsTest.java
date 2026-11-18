package lakewire.storage.lkw1;

import java.util.Arrays;
import lakewire.storage.RecordChunks;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchOffsetsTest {

    @Test
    void patchesBaseOffsetsWithoutInvalidatingTheBatchCrc() {
        byte[] first = RecordChunks.oneBatch(Compression.NONE, 2, 1_000);
        byte[] second = RecordChunks.oneBatch(Compression.gzip().build(), 3, 2_000);
        byte[] chunk = new byte[first.length + second.length];
        System.arraycopy(first, 0, chunk, 0, first.length);
        System.arraycopy(second, 0, chunk, first.length, second.length);

        byte[] patched = BatchOffsets.patch(chunk, 100);
        assertThat(patched).hasSize(chunk.length);
        assertThat(Arrays.copyOfRange(patched, 8, first.length)).isEqualTo(Arrays.copyOfRange(first, 8, first.length));
        assertThat(Arrays.copyOfRange(patched, first.length + 8, patched.length))
            .isEqualTo(Arrays.copyOfRange(second, 8, second.length));
        assertThat(readLong(patched, 0)).isEqualTo(100);
        assertThat(readLong(patched, first.length)).isEqualTo(102);
    }

    @Test
    void aCorruptedPayloadFailsValidation() {
        byte[] chunk = RecordChunks.oneBatch(Compression.NONE, 1, 1);
        chunk[chunk.length - 1] ^= 0x01;
        assertThatThrownBy(() -> BatchOffsets.patch(chunk, 0)).isInstanceOf(CorruptRecordException.class);
    }

    private static long readLong(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (bytes[offset + i] & 0xFFL);
        }
        return value;
    }
}
