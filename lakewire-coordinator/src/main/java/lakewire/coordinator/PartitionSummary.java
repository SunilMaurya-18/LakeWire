package lakewire.coordinator;

public record PartitionSummary(int id, long highWatermark, long logStartOffset, long bytes) {
}
