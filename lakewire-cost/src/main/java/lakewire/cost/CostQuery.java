package lakewire.cost;

import java.math.BigDecimal;

public record CostQuery(
    BigDecimal gibWritten,
    int zones,
    int objectBytes,
    int retentionDays,
    int hotDays
) {

    public static final int DEFAULT_OBJECT_BYTES = 8 * 1024 * 1024;

    public CostQuery {
        if (gibWritten == null || gibWritten.signum() <= 0) {
            throw new IllegalArgumentException("gibWritten must be positive");
        }
        if (zones < 1) {
            throw new IllegalArgumentException("zones must be at least 1");
        }
        if (objectBytes < 1) {
            throw new IllegalArgumentException("objectBytes must be positive");
        }
        if (retentionDays < 0) {
            throw new IllegalArgumentException("retentionDays must not be negative");
        }
        if (hotDays < 0) {
            throw new IllegalArgumentException("hotDays must not be negative");
        }
    }

    public static CostQuery tableDefault() {
        return new CostQuery(BigDecimal.ONE, 3, DEFAULT_OBJECT_BYTES, 7, 1);
    }
}
