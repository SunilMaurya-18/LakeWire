package lakewire.workers;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lakewire.coordinator.CommitEntry;
import lakewire.coordinator.TopicDirectory;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetentionIT {

    @Test
    @Timeout(120)
    void sharedObjectWaitsForEverySegmentAndGrace() throws Exception {
        WorkerConfig config = new WorkerConfig(
            true, 60_000, 60_000, 60_000, 50, 60_000, 100,
            7L * 24 * 60 * 60 * 1000, 60_000, 80, 200, "");
        try (WorkerHarness harness = WorkerHarness.start(config)) {
            long epoch = harness.register("1");
            long shortTopic = harness.createTopic("short", 1, 1);
            long longTopic = harness.createTopic("long", 1, TopicDirectory.DEFAULT_RETENTION_MS);
            long now = System.currentTimeMillis();
            byte[] body = {1, 2, 3, 4};
            harness.commit(
                "1",
                epoch,
                "shared",
                body,
                new CommitEntry(shortTopic, 0, 0, 2, 1, (short) 1, 0, -1, (short) 0, 0, 0),
                new CommitEntry(longTopic, 0, 2, 2, 1, (short) 1, now, -1, (short) 0, 0, 0)
            );

            harness.jobs.retention();
            assertThat(harness.queryString("SELECT state FROM objects WHERE key = 'shared'")).isEqualTo("COMMITTED");
            assertThat(harness.queryLong("SELECT live_refs FROM objects WHERE key = 'shared'")).isEqualTo(1);
            assertThat(harness.queryLong(
                "SELECT count(*) FROM segments s JOIN topics t ON t.id = s.topic_id WHERE t.name = 'short'"
            )).isZero();
            assertThat(harness.store.head("shared").get(10, TimeUnit.SECONDS).size()).isEqualTo(4);

            harness.topics.deleteTopic("long");
            harness.jobs.retention();
            assertThat(harness.queryString("SELECT state FROM objects WHERE key = 'shared'"))
                .isEqualTo("DELETE_PENDING");
            assertThat(harness.queryLong("SELECT live_refs FROM objects WHERE key = 'shared'")).isZero();
            assertThat(harness.store.head("shared").get(10, TimeUnit.SECONDS).size()).isEqualTo(4);
            assertThat(harness.queryLong("SELECT count(*) FROM topics WHERE name = 'long'")).isZero();

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                harness.jobs.sweepDeletePending();
                assertThatThrownBy(() -> harness.store.head("shared").get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(lakewire.storage.ObjectNotFoundException.class);
            });
            assertThat(harness.queryLong("SELECT count(*) FROM objects WHERE key = 'shared'")).isZero();
        }
    }
}
