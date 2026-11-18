package lakewire.coordinator;

public record StoredTopic(long id, String name, int partitions, long retentionMs) {
}
