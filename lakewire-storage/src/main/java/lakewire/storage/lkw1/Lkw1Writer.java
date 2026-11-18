package lakewire.storage.lkw1;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lakewire.storage.Crc32c;

/** Writes one LKW1 object. Index {@code byte_start} values are absolute file offsets (A29). */
public final class Lkw1Writer {

    private static final byte[] MAGIC = "LKW1".getBytes(StandardCharsets.US_ASCII);
    static final int ENTRY_BYTES = 8 + 4 + 8 + 4 + 4 + 2 + 8 + 4;

    private Lkw1Writer() {
    }

    public static byte[] write(Lkw1Object object) {
        return write(object.brokerId(), object.brokerEpoch(), object.createdMs(), object.chunks());
    }

    public static byte[] write(String brokerId, long brokerEpoch, long createdMs, List<Lkw1Chunk> chunks) {
        if (brokerId == null) {
            throw new IllegalArgumentException("brokerId must not be null");
        }
        if (chunks == null) {
            throw new IllegalArgumentException("chunks must not be null");
        }
        byte[] brokerBytes = brokerId.getBytes(StandardCharsets.UTF_8);
        if (brokerBytes.length > 65535) {
            throw new IllegalArgumentException("brokerId is longer than 65535 bytes");
        }
        int headerLength = 4 + 1 + 2 + brokerBytes.length + 8 + 8;
        int bodyLength = 0;
        for (Lkw1Chunk chunk : chunks) {
            bodyLength += chunk.records().length;
        }
        int indexLength = chunks.size() * ENTRY_BYTES;
        ByteBuffer out = ByteBuffer.allocate(headerLength + bodyLength + indexLength + 16);
        out.put(MAGIC);
        out.put((byte) 1);
        out.putShort((short) brokerBytes.length);
        out.put(brokerBytes);
        out.putLong(brokerEpoch);
        out.putLong(createdMs);

        int absolute = headerLength;
        for (Lkw1Chunk chunk : chunks) {
            out.put(chunk.records());
        }
        int indexOffset = out.position();
        ByteBuffer index = ByteBuffer.allocate(indexLength);
        for (Lkw1Chunk chunk : chunks) {
            index.putLong(chunk.topicId());
            index.putInt(chunk.part());
            index.putLong(absolute);
            index.putInt(chunk.records().length);
            index.putInt(chunk.nRecords());
            index.putShort(chunk.nBatches());
            index.putLong(chunk.maxTimestamp());
            index.putInt((int) Crc32c.of(chunk.records()));
            absolute += chunk.records().length;
        }
        byte[] indexBytes = index.array();
        out.put(indexBytes);
        out.putLong(indexOffset);
        out.putInt((int) Crc32c.of(indexBytes));
        out.put(MAGIC);
        return out.array();
    }

    public static List<Lkw1Chunk> chunks(Lkw1Chunk... chunks) {
        return new ArrayList<>(Arrays.asList(chunks));
    }
}
