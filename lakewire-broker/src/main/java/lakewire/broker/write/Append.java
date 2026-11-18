package lakewire.broker.write;

/** One Produce partition payload. The record bytes are a single magic-2 batch (A20). */
public record Append(
    long topicId,
    int partition,
    byte[] records,
    int nRecords,
    short nBatches,
    long maxTimestamp,
    long producerId,
    short producerEpoch,
    int firstSeq,
    int lastSeq
) {
    public Append {
        if (records == null || records.length == 0) {
            throw new IllegalArgumentException("records must not be empty");
        }
        records = records.clone();
    }
}
