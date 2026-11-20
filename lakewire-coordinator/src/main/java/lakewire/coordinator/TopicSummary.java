package lakewire.coordinator;

import java.util.List;

public record TopicSummary(
    String name,
    int partitions,
    long retentionMs,
    long bytes,
    List<PartitionSummary> partitionStats
) {
}
