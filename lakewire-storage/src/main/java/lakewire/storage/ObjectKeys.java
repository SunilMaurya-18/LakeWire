package lakewire.storage;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;

public final class ObjectKeys {

    private ObjectKeys() {
    }

    public static void require(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        if (key.startsWith("/") || key.endsWith("/") || key.indexOf('\\') >= 0 || key.contains("..")) {
            throw new IllegalArgumentException("invalid object key");
        }
    }

    /**
     * {@code {prefix}/v2/{yyyy}/{mm}/{dd}/{hh}/{broker}-{epoch}-{uuidv7}.lkw} in UTC.
     * A blank prefix omits the leading segment.
     */
    public static String objectKey(String prefix, String brokerId, long epoch, UUID id, Instant created) {
        if (brokerId == null || brokerId.isBlank() || brokerId.indexOf('/') >= 0 || brokerId.indexOf('\\') >= 0) {
            throw new IllegalArgumentException("brokerId must not contain a path separator");
        }
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must be >= 0");
        }
        if (id == null) {
            throw new IllegalArgumentException("id must not be null");
        }
        if (created == null) {
            throw new IllegalArgumentException("created must not be null");
        }
        ZonedDateTime utc = created.atZone(ZoneOffset.UTC);
        String dated = "v2/%04d/%02d/%02d/%02d/%s-%d-%s.lkw".formatted(
            utc.getYear(),
            utc.getMonthValue(),
            utc.getDayOfMonth(),
            utc.getHour(),
            brokerId,
            epoch,
            id
        );
        if (prefix == null || prefix.isBlank()) {
            return dated;
        }
        String trimmed = prefix;
        while (trimmed.startsWith("/")) {
            trimmed = trimmed.substring(1);
        }
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        if (trimmed.isBlank() || trimmed.contains("..")) {
            throw new IllegalArgumentException("invalid prefix");
        }
        return trimmed + "/" + dated;
    }
}
