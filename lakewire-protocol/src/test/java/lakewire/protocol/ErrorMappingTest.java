package lakewire.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

class ErrorMappingTest {

    @Test
    void staleBrokerEpochIsNotLeaderOrFollower() {
        assertThat(ErrorMapping.STALE_BROKER_EPOCH.code()).isEqualTo(Errors.NOT_LEADER_OR_FOLLOWER.code());
    }

    @Test
    void expiredLeaseIsNotLeaderOrFollower() {
        assertThat(ErrorMapping.EXPIRED_LEASE.code()).isEqualTo(Errors.NOT_LEADER_OR_FOLLOWER.code());
    }

    @Test
    void coordinatorUnavailableIsRequestTimedOut() {
        assertThat(ErrorMapping.COORDINATOR_UNAVAILABLE.code()).isEqualTo(Errors.REQUEST_TIMED_OUT.code());
    }

    @Test
    void sequenceGapIsOutOfOrder() {
        assertThat(ErrorMapping.SEQUENCE_GAP.code()).isEqualTo(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER.code());
    }

    @Test
    void unknownOrExpiredProducerIsUnknownProducerId() {
        assertThat(ErrorMapping.UNKNOWN_OR_EXPIRED_PRODUCER.code()).isEqualTo(Errors.UNKNOWN_PRODUCER_ID.code());
    }

    @Test
    void fencedEpochIsInvalidProducerEpoch() {
        assertThat(ErrorMapping.FENCED_EPOCH.code()).isEqualTo(Errors.INVALID_PRODUCER_EPOCH.code());
    }

    @Test
    void storageRetriesExhaustedIsKafkaStorageError() {
        assertThat(ErrorMapping.STORAGE_RETRIES_EXHAUSTED.code()).isEqualTo(Errors.KAFKA_STORAGE_ERROR.code());
    }

    @Test
    void producerCoordinatorUnavailableIsCoordinatorNotAvailable() {
        assertThat(ErrorMapping.PRODUCER_COORDINATOR_UNAVAILABLE.code())
            .isEqualTo(Errors.COORDINATOR_NOT_AVAILABLE.code());
    }
}
