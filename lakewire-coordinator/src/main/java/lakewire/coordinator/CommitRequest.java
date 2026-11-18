package lakewire.coordinator;

import java.util.List;
import java.util.UUID;

public record CommitRequest(
    UUID objectId,
    String key,
    long sizeBytes,
    String brokerId,
    long brokerEpoch,
    List<CommitEntry> entries
) {

    public CommitRequest {
        if (objectId == null) {
            throw new IllegalArgumentException("objectId must not be null");
        }
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        if (brokerId == null || brokerId.isBlank()) {
            throw new IllegalArgumentException("brokerId must not be blank");
        }
        if (entries == null) {
            throw new IllegalArgumentException("entries must not be null");
        }
        entries = List.copyOf(entries);
    }
}
