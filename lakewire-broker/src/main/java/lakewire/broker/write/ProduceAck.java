package lakewire.broker.write;

import org.apache.kafka.common.protocol.Errors;

public record ProduceAck(short errorCode, long baseOffset, long logStartOffset) {

    public static ProduceAck error(Errors error) {
        return new ProduceAck(error.code(), -1L, -1L);
    }

    public static ProduceAck ok(long baseOffset, long logStartOffset) {
        return new ProduceAck(Errors.NONE.code(), baseOffset, logStartOffset);
    }
}
