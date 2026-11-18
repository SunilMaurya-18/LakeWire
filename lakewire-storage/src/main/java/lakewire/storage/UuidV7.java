package lakewire.storage;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * RFC 9562 UUID version 7. Monotonic within this process: a later call never
 * sorts before an earlier one while the clock stays in the positive range.
 */
public final class UuidV7 {

    private static final Object LOCK = new Object();
    private static final SecureRandom RANDOM = new SecureRandom();
    private static long lastMillis = -1;
    private static int sequence;
    private static long randomBits;

    private UuidV7() {
    }

    public static UUID next() {
        synchronized (LOCK) {
            long now = System.currentTimeMillis();
            if (now > lastMillis) {
                lastMillis = now;
                sequence = RANDOM.nextInt(1 << 12);
                randomBits = RANDOM.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL;
            } else {
                sequence++;
                if (sequence > 0xFFF) {
                    lastMillis++;
                    sequence = 0;
                    randomBits = RANDOM.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL;
                }
                now = lastMillis;
            }
            long most = ((now & 0xFFFF_FFFF_FFFFL) << 16) | 0x7000L | (sequence & 0xFFFL);
            long least = 0x8000_0000_0000_0000L | randomBits;
            return new UUID(most, least);
        }
    }

    public static long unixMillis(UUID id) {
        return id.getMostSignificantBits() >>> 16;
    }

    public static int sequence(UUID id) {
        return (int) (id.getMostSignificantBits() & 0xFFFL);
    }
}
