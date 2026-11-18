package lakewire.broker.read;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;

public record PartitionRead(short errorCode, long highWatermark, long logStartOffset, MemoryRecords records) {

    public static PartitionRead unknown() {
        return new PartitionRead(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), -1L, -1L, MemoryRecords.EMPTY);
    }

    public static PartitionRead outOfRange(Watermark watermark) {
        return new PartitionRead(
            Errors.OFFSET_OUT_OF_RANGE.code(),
            watermark.highWatermark(),
            watermark.logStartOffset(),
            MemoryRecords.EMPTY
        );
    }

    public static PartitionRead empty(Watermark watermark) {
        return new PartitionRead(Errors.NONE.code(), watermark.highWatermark(), watermark.logStartOffset(), MemoryRecords.EMPTY);
    }

    public static PartitionRead ok(Watermark watermark, MemoryRecords records) {
        return new PartitionRead(Errors.NONE.code(), watermark.highWatermark(), watermark.logStartOffset(), records);
    }

    public static PartitionRead storage(Watermark watermark) {
        return new PartitionRead(
            Errors.KAFKA_STORAGE_ERROR.code(),
            watermark.highWatermark(),
            watermark.logStartOffset(),
            MemoryRecords.EMPTY
        );
    }
}
