package lakewire.storage;

import java.util.zip.CRC32C;

public final class Crc32c {

    private Crc32c() {
    }

    public static long of(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes);
        return crc.getValue();
    }

    public static long of(byte[] bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return crc.getValue();
    }
}
