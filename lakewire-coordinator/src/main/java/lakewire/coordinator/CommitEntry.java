package lakewire.coordinator;

public record CommitEntry(
    long topicId,
    int part,
    long byteStart,
    int byteLen,
    int nRecords,
    short nBatches,
    long maxTimestamp,
    long producerId,
    short producerEpoch,
    int firstSeq,
    int lastSeq
) {

    public CommitEntry {
        if (nRecords < 1) {
            throw new IllegalArgumentException("nRecords must be >= 1");
        }
    }

    /** A non-idempotent entry. producerId -1 skips sl_check_sequence. */
    public static CommitEntry of(long topicId, int part, int nRecords, int byteLen) {
        return new CommitEntry(topicId, part, 0, byteLen, nRecords, (short) 1, 0, -1, (short) 0, 0, 0);
    }
}
