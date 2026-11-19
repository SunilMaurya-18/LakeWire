package lakewire.storage;

import java.time.Instant;

/** Size in bytes and the unsigned CRC32C of the object body. CRC is -1 when the store has none. */
public record ObjectHead(long size, long crc32c, Instant lastModified) {

    public ObjectHead {
        if (lastModified == null) {
            lastModified = Instant.EPOCH;
        }
    }

    public ObjectHead(long size, long crc32c) {
        this(size, crc32c, Instant.EPOCH);
    }
}
