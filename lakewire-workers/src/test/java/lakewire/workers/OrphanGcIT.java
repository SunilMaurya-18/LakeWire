package lakewire.workers;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lakewire.coordinator.CommitEntry;
import lakewire.coordinator.CommitRequest;
import lakewire.coordinator.CommitResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrphanGcIT {

    @Test
    @Timeout(120)
    void expiredIntentBeatsLateCommitAndListingSweepDeletesLeaks() throws Exception {
        WorkerConfig config = WorkerConfig.defaults();
        try (WorkerHarness harness = WorkerHarness.start(config)) {
            long epoch = harness.register("1");
            long topic = harness.createTopic("orders", 1, 86_400_000);
            byte[] body = {9, 9, 9};
            harness.intent("late", "1", epoch);
            harness.store.put("late", body).join();
            harness.ageIntent("late");

            harness.jobs.orphanGc();
            assertThat(harness.queryLong("SELECT count(*) FROM upload_intents WHERE object_key = 'late'")).isZero();
            assertThatThrownBy(() -> harness.store.head("late").get(10, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(lakewire.storage.ObjectNotFoundException.class);

            List<CommitResult> late = harness.coordinator.commit(new CommitRequest(
                UUID.randomUUID(),
                "late",
                body.length,
                "1",
                epoch,
                List.of(CommitEntry.of(topic, 0, 1, 3))
            ));
            assertThat(late).isNotEmpty();
            assertThat(late.get(0).errorCode()).isEqualTo(WorkerHarness.notLeader());
            assertThat(harness.queryLong("SELECT count(*) FROM objects")).isZero();
            assertThat(harness.queryLong("SELECT next_offset FROM partitions WHERE topic_id = " + topic)).isZero();

            harness.store.put("leak", new byte[] {1}).join();
            harness.store.put("kept-intent", new byte[] {2}).join();
            harness.intent("kept-intent", "1", epoch);
            harness.jobs.orphanGc();
            assertThatThrownBy(() -> harness.store.head("leak").get(10, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(lakewire.storage.ObjectNotFoundException.class);
            assertThat(harness.store.head("kept-intent").get(10, TimeUnit.SECONDS).size()).isEqualTo(1);

            harness.execute("""
                INSERT INTO producer_state (producer_id, topic_id, part, producer_epoch, last_seq, recent, updated_at)
                VALUES (1, %d, 0, 0, 0, '[]'::jsonb, now() - interval '8 days')
                """.formatted(topic));
            harness.jobs.expireProducerState();
            assertThat(harness.queryLong("SELECT count(*) FROM producer_state")).isZero();
        }
    }
}
