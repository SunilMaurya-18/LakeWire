package lakewire.coordinator;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class HwListenerIT {

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
    void commitNotifiesAndRollbackDoesNot() throws Exception {
        long topic = fixture.createTopic("hw", 1);
        long epoch = fixture.register("b1");
        List<HwUpdate> updates = new CopyOnWriteArrayList<>();
        try (HwListener listener = new HwListener(fixture.coordinator.config())) {
            listener.subscribe(updates::add);
            listener.start();
            listener.awaitReady(Duration.ofSeconds(10));

            fixture.registry.createUploadIntent("obj-hw", "b1", epoch);
            fixture.coordinator.commit(new CommitRequest(
                UUID.randomUUID(), "obj-hw", 4, "b1", epoch, List.of(CommitEntry.of(topic, 0, 3, 4))));

            await().atMost(Duration.ofSeconds(5)).until(() -> !updates.isEmpty());
            assertThat(updates).containsExactly(new HwUpdate(topic, 0, 3));

            int delivered = updates.size();
            fixture.admin.setAutoCommit(false);
            try {
                fixture.registry.createUploadIntent("obj-rollback", "b1", epoch);
                fixture.coordinator.commitOn(
                    fixture.admin,
                    new CommitRequest(
                        UUID.randomUUID(),
                        "obj-rollback",
                        4,
                        "b1",
                        epoch,
                        List.of(CommitEntry.of(topic, 0, 1, 4))
                    )
                );
                fixture.admin.rollback();
            } catch (RuntimeException failure) {
                fixture.admin.rollback();
                throw failure;
            } finally {
                fixture.admin.setAutoCommit(true);
            }

            await()
                .during(Duration.ofMillis(1500))
                .atMost(Duration.ofSeconds(4))
                .until(() -> updates.size() == delivered);
        }
        assertThat(fixture.queryLong("SELECT count(*) FROM objects WHERE key = 'obj-rollback'")).isZero();
        assertThat(fixture.queryLong("SELECT next_offset FROM partitions WHERE topic_id = " + topic)).isEqualTo(3);
    }
}
