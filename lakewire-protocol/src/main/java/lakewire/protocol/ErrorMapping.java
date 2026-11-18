package lakewire.protocol;

import org.apache.kafka.common.protocol.Errors;

/**
 * Kafka error codes for coordinator and storage failures (PRD §10.1).
 * Clients see the code only; stack traces stay on the broker.
 */
public enum ErrorMapping {
    STALE_BROKER_EPOCH(Errors.NOT_LEADER_OR_FOLLOWER),
    EXPIRED_LEASE(Errors.NOT_LEADER_OR_FOLLOWER),
    COORDINATOR_UNAVAILABLE(Errors.REQUEST_TIMED_OUT),
    SEQUENCE_GAP(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER),
    UNKNOWN_OR_EXPIRED_PRODUCER(Errors.UNKNOWN_PRODUCER_ID),
    FENCED_EPOCH(Errors.INVALID_PRODUCER_EPOCH);

    private final Errors error;

    ErrorMapping(Errors error) {
        this.error = error;
    }

    public Errors error() {
        return error;
    }

    public short code() {
        return error.code();
    }
}
