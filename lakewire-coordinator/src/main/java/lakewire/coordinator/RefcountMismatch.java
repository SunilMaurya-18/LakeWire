package lakewire.coordinator;

import java.util.UUID;

public record RefcountMismatch(
    UUID objectId,
    String key,
    int liveRefs,
    int actualRefs,
    long liveBytes,
    long actualBytes
) {
}
