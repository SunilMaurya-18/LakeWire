package lakewire.broker.read;

import org.apache.kafka.common.protocol.Errors;

public record OffsetBound(short errorCode, long offset, long timestamp) {

    public static OffsetBound error(Errors error) {
        return new OffsetBound(error.code(), -1L, -1L);
    }

    public static OffsetBound found(long offset, long timestamp) {
        return new OffsetBound(Errors.NONE.code(), offset, timestamp);
    }
}
