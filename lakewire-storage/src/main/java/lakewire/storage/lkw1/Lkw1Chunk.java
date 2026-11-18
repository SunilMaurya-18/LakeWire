package lakewire.storage.lkw1;

public record Lkw1Chunk(
    long topicId,
    int part,
    byte[] records,
    int nRecords,
    short nBatches,
    long maxTimestamp
) {

    public Lkw1Chunk {
        if (records == null) {
            throw new IllegalArgumentException("records must not be null");
        }
        if (nRecords < 1) {
            throw new IllegalArgumentException("nRecords must be >= 1");
        }
        if (nBatches < 1) {
            throw new IllegalArgumentException("nBatches must be >= 1");
        }
        records = records.clone();
    }
}
