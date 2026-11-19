package lakewire.coordinator;

import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SequenceCheckIT {

    private static final short UNKNOWN = Errors.UNKNOWN_PRODUCER_ID.code();
    private static final short EPOCH = Errors.INVALID_PRODUCER_EPOCH.code();
    private static final short OUT_OF_ORDER = Errors.OUT_OF_ORDER_SEQUENCE_NUMBER.code();

    private static CoordinatorFixture fixture;

    @BeforeAll
    static void start() {
        fixture = CoordinatorFixture.start();
    }

    @AfterAll
    static void stop() {
        if (fixture != null) {
            fixture.close();
        }
    }

    @BeforeEach
    void reset() {
        fixture.truncate();
    }

    @Test
    void initAllocatesBumpsFencesAndOverflows() throws Exception {
        ProducerIdAllocation created = fixture.coordinator.initProducerId(-1, (short) -1);
        assertThat(created.errorCode()).isEqualTo((short) 0);
        assertThat(created.epoch()).isZero();
        assertThat(created.producerId()).isPositive();

        ProducerIdAllocation bumped = fixture.coordinator.initProducerId(created.producerId(), created.epoch());
        assertThat(bumped.errorCode()).isEqualTo((short) 0);
        assertThat(bumped.producerId()).isEqualTo(created.producerId());
        assertThat(bumped.epoch()).isEqualTo((short) 1);

        ProducerIdAllocation fenced = fixture.coordinator.initProducerId(created.producerId(), (short) 0);
        assertThat(fenced.errorCode()).isEqualTo(EPOCH);
        assertThat(fenced.producerId()).isEqualTo(-1);

        ProducerIdAllocation unknown = fixture.coordinator.initProducerId(9_000_000L, (short) 0);
        assertThat(unknown.errorCode()).isEqualTo((short) 0);
        assertThat(unknown.producerId()).isNotEqualTo(9_000_000L);
        assertThat(unknown.epoch()).isZero();

        try (Statement statement = fixture.admin.createStatement()) {
            statement.execute("UPDATE producer_ids SET producer_epoch = 32767 WHERE producer_id = " + created.producerId());
        }
        ProducerIdAllocation overflow = fixture.coordinator.initProducerId(created.producerId(), (short) 32767);
        assertThat(overflow.errorCode()).isEqualTo((short) 0);
        assertThat(overflow.producerId()).isNotEqualTo(created.producerId());
        assertThat(overflow.epoch()).isZero();
    }

    @Test
    void everySequenceRuleIncludingWrapAndTheFiveEntryWindow() {
        long topic = fixture.createTopic("seq", 1);
        long brokerEpoch = fixture.register("b1");

        assertThat(commit(topic, brokerEpoch, 40, (short) 0, 0, 0).errorCode()).isEqualTo(UNKNOWN);
        assertThat(fixture.queryLong("SELECT count(*) FROM segments")).isZero();

        ProducerIdAllocation producer = fixture.coordinator.initProducerId(-1, (short) -1);
        long id = producer.producerId();
        assertThat(commit(topic, brokerEpoch, id, (short) 0, 4, 4).errorCode()).isEqualTo(UNKNOWN);

        CommitResult first = commit(topic, brokerEpoch, id, (short) 0, 0, 0);
        assertThat(first.errorCode()).isZero();
        assertThat(first.baseOffset()).isZero();

        CommitResult second = commit(topic, brokerEpoch, id, (short) 0, 1, 1);
        assertThat(second.errorCode()).isZero();
        assertThat(second.baseOffset()).isEqualTo(1);

        assertThat(commit(topic, brokerEpoch, id, (short) 0, 3, 3).errorCode()).isEqualTo(OUT_OF_ORDER);
        CommitResult third = commit(topic, brokerEpoch, id, (short) 0, 2, 2);
        assertThat(third.errorCode()).isZero();
        assertThat(third.baseOffset()).isEqualTo(2);

        CommitResult replay = commit(topic, brokerEpoch, id, (short) 0, 1, 1);
        assertThat(replay.errorCode()).isZero();
        assertThat(replay.baseOffset()).isEqualTo(1);
        assertThat(fixture.queryLong("SELECT count(*) FROM segments")).isEqualTo(3);

        assertThat(commit(topic, brokerEpoch, id, (short) 4, 0, 0).errorCode()).isEqualTo(EPOCH);

        ProducerIdAllocation bumped = fixture.coordinator.initProducerId(id, (short) 0);
        assertThat(bumped.epoch()).isEqualTo((short) 1);
        assertThat(commit(topic, brokerEpoch, id, (short) 0, 0, 0).errorCode()).isEqualTo(EPOCH);
        assertThat(commit(topic, brokerEpoch, id, (short) 1, 5, 5).errorCode()).isEqualTo(OUT_OF_ORDER);
        CommitResult reset = commit(topic, brokerEpoch, id, (short) 1, 0, 0);
        assertThat(reset.errorCode()).isZero();
        assertThat(reset.baseOffset()).isEqualTo(3);

        CommitResult toMax = commit(topic, brokerEpoch, id, (short) 1, 1, Integer.MAX_VALUE);
        assertThat(toMax.errorCode()).isZero();
        CommitResult afterWrap = commit(topic, brokerEpoch, id, (short) 1, 0, 1);
        assertThat(afterWrap.errorCode()).isZero();
        assertThat(afterWrap.baseOffset()).isEqualTo(toMax.baseOffset() + 1);
        CommitResult wrapDup = commit(topic, brokerEpoch, id, (short) 1, 0, 1);
        assertThat(wrapDup.errorCode()).isZero();
        assertThat(wrapDup.baseOffset()).isEqualTo(afterWrap.baseOffset());
    }

    @Test
    void theSixthOldestDuplicateIsOutOfOrder() {
        long topic = fixture.createTopic("window", 1);
        long brokerEpoch = fixture.register("b1");
        long id = fixture.coordinator.initProducerId(-1, (short) -1).producerId();
        long[] bases = new long[6];
        for (int seq = 0; seq < 6; seq++) {
            CommitResult result = commit(topic, brokerEpoch, id, (short) 0, seq, seq);
            assertThat(result.errorCode()).isZero();
            bases[seq] = result.baseOffset();
        }
        CommitResult dropped = commit(topic, brokerEpoch, id, (short) 0, 0, 0);
        assertThat(dropped.errorCode()).isEqualTo(OUT_OF_ORDER);
        CommitResult kept = commit(topic, brokerEpoch, id, (short) 0, 1, 1);
        assertThat(kept.errorCode()).isZero();
        assertThat(kept.baseOffset()).isEqualTo(bases[1]);
        assertThat(fixture.queryLong("SELECT count(*) FROM segments")).isEqualTo(6);
    }

    private static CommitResult commit(
        long topic,
        long brokerEpoch,
        long producerId,
        short producerEpoch,
        int first,
        int last
    ) {
        CommitEntry entry = new CommitEntry(
            topic, 0, 0, 8, 1, (short) 1, 0, producerId, producerEpoch, first, last);
        String key = "obj-" + UUID.randomUUID();
        fixture.registry.createUploadIntent(key, "b1", brokerEpoch);
        List<CommitResult> results = fixture.coordinator.commit(new CommitRequest(
            UUID.randomUUID(),
            key,
            8,
            "b1",
            brokerEpoch,
            List.of(entry)
        ));
        assertThat(results).hasSize(1);
        return results.get(0);
    }
}
