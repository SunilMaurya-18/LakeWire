package lakewire.broker.read;

public record StoredSegment(
    long baseOffset,
    long lastOffset,
    long byteStart,
    int byteLen,
    long maxTimestamp,
    String objectKey
) {
}
