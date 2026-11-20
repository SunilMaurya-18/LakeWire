package lakewire.coordinator;

import java.util.UUID;

public record SegmentRow(
    long baseOffset,
    long lastOffset,
    UUID objectId,
    long byteStart,
    int byteLen,
    short nBatches,
    long maxTs
) {
}
