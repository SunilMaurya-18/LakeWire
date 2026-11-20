package lakewire.coordinator;

import java.time.Instant;
import java.util.UUID;

public record ObjectSnapshot(
    UUID id,
    String key,
    long sizeBytes,
    int liveRefs,
    long liveBytes,
    String state,
    Instant deleteAfter,
    double utilisation
) {
}
