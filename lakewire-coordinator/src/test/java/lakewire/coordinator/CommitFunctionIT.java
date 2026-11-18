package lakewire.coordinator;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CommitFunctionIT {

    private static final short NOT_LEADER = Errors.NOT_LEADER_OR_FOLLOWER.code();
    private static final short UNKNOWN_TOPIC = Errors.UNKNOWN_TOPIC_OR_PARTITION.code();

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
    void singleObjectThenTheNextCommitContinuesTheLog() {
        long topic = fixture.createTopic("orders", 1);
        long epoch = fixture.register("b1");
        List<CommitResult> first = commit("b1", epoch, "obj-1", 10, CommitEntry.of(topic, 0, 4, 10));
        List<CommitResult> second = commit("b1", epoch, "obj-2", 5, CommitEntry.of(topic, 0, 2, 5));

        assertThat(first).containsExactly(new CommitResult(0, 0, (short) 0, 0));
        assertThat(second).containsExactly(new CommitResult(0, 4, (short) 0, 0));
        assertThat(fixture.queryLong("SELECT next_offset FROM partitions WHERE topic_id = " + topic))
            .isEqualTo(6);
        assertThat(fixture.queryLong("SELECT last_offset FROM segments WHERE topic_id = " + topic + " AND base_offset = 0"))
            .isEqualTo(3);
        assertThat(fixture.queryString("SELECT state FROM objects WHERE key = 'obj-1'")).isEqualTo("COMMITTED");
        assertThat(fixture.queryLong("SELECT live_refs FROM objects WHERE key = 'obj-1'")).isEqualTo(1);
        assertThat(fixture.queryLong("SELECT live_bytes FROM objects WHERE key = 'obj-1'")).isEqualTo(10);
    }

    @Test
    void multiPartitionOffsetsFollowArrayOrderInsideEachPartition() {
        long topic = fixture.createTopic("events", 3);
        long epoch = fixture.register("b1");
        List<CommitResult> results = commit(
            "b1",
            epoch,
            "obj-multi",
            30,
            CommitEntry.of(topic, 2, 2, 8),
            CommitEntry.of(topic, 0, 3, 9),
            CommitEntry.of(topic, 0, 4, 7),
            CommitEntry.of(topic, 1, 1, 6)
        );

        assertThat(results).containsExactly(
            new CommitResult(0, 0, (short) 0, 0),
            new CommitResult(1, 0, (short) 0, 0),
            new CommitResult(2, 3, (short) 0, 0),
            new CommitResult(3, 0, (short) 0, 0)
        );
        assertThat(nextOffset(topic, 0)).isEqualTo(7);
        assertThat(nextOffset(topic, 1)).isEqualTo(1);
        assertThat(nextOffset(topic, 2)).isEqualTo(2);
        assertThat(fixture.queryLong(
            "SELECT count(*) FROM segments WHERE object_id = (SELECT id FROM objects WHERE key = 'obj-multi')"
        )).isEqualTo(4);
    }

    @Test
    void deletedTopicIsUnknownAndDoesNotAdvanceTheLog() {
        long topic = fixture.createTopic("gone", 1);
        long epoch = fixture.register("b1");
        fixture.deleteTopic(topic);

        List<CommitResult> results = commit("b1", epoch, "obj-gone", 4, CommitEntry.of(topic, 0, 1, 4));

        assertThat(results).containsExactly(new CommitResult(0, -1, UNKNOWN_TOPIC, 0));
        assertThat(fixture.queryString("SELECT state FROM objects WHERE key = 'obj-gone'")).isEqualTo("DELETE_PENDING");
        assertThat(fixture.queryLong("SELECT count(*) FROM segments")).isZero();
        assertThat(fixture.queryLong("SELECT next_offset FROM partitions WHERE topic_id = " + topic)).isZero();
    }

    @Test
    void staleEpochWritesNothing() {
        long topic = fixture.createTopic("t", 1);
        long epoch = fixture.register("b1");
        fixture.registry.createUploadIntent("obj-stale", "b1", epoch);

        List<CommitResult> results = fixture.coordinator.commit(
            request("b1", epoch + 5, "obj-stale", 4, CommitEntry.of(topic, 0, 1, 4)));

        assertThat(results).containsExactly(new CommitResult(0, -1, NOT_LEADER, -1));
        assertThat(fixture.queryLong("SELECT count(*) FROM objects")).isZero();
        assertThat(fixture.queryLong("SELECT count(*) FROM upload_intents")).isEqualTo(1);
    }

    @Test
    void expiredLeaseWritesNothing() {
        long topic = fixture.createTopic("t", 1);
        long epoch = fixture.register("b1");
        fixture.expireLease("b1");
        fixture.registry.createUploadIntent("obj-expired", "b1", epoch);

        List<CommitResult> results = fixture.coordinator.commit(
            request("b1", epoch, "obj-expired", 4, CommitEntry.of(topic, 0, 1, 4)));

        assertThat(results).containsExactly(new CommitResult(0, -1, NOT_LEADER, -1));
        assertThat(fixture.queryLong("SELECT count(*) FROM objects")).isZero();
    }

    @Test
    void missingIntentWritesNothing() {
        long topic = fixture.createTopic("t", 1);
        long epoch = fixture.register("b1");

        List<CommitResult> results = fixture.coordinator.commit(
            request("b1", epoch, "obj-missing", 4, CommitEntry.of(topic, 0, 1, 4)));

        assertThat(results).containsExactly(new CommitResult(0, -1, NOT_LEADER, -1));
        assertThat(fixture.queryLong("SELECT count(*) FROM objects")).isZero();
    }

    @Test
    void replayReturnsTheSameOffsetsAndDoesNotWriteAgain() {
        long topic = fixture.createTopic("t", 2);
        long epoch = fixture.register("b1");
        CommitEntry[] entries = {CommitEntry.of(topic, 1, 3, 9), CommitEntry.of(topic, 0, 2, 4)};
        List<CommitResult> first = commit("b1", epoch, "obj-replay", 13, entries);
        List<CommitResult> second = commit("b1", epoch, "obj-replay", 13, entries);

        assertThat(second).isEqualTo(first);
        assertThat(first).containsExactly(
            new CommitResult(0, 0, (short) 0, 0),
            new CommitResult(1, 0, (short) 0, 0)
        );
        assertThat(fixture.queryLong("SELECT count(*) FROM objects")).isEqualTo(1);
        assertThat(fixture.queryLong("SELECT count(*) FROM segments")).isEqualTo(2);
        assertThat(nextOffset(topic, 0)).isEqualTo(2);
        assertThat(nextOffset(topic, 1)).isEqualTo(3);
    }

    @Test
    void nothingAcceptedBecomesDeletePending() {
        long live = fixture.createTopic("live", 1);
        long dead = fixture.createTopic("dead", 1);
        fixture.deleteTopic(dead);
        long epoch = fixture.register("b1");

        List<CommitResult> results = commit(
            "b1", epoch, "obj-empty", 8,
            CommitEntry.of(dead, 0, 2, 5),
            CommitEntry.of(live, 9, 1, 3)
        );

        assertThat(results).containsExactly(
            new CommitResult(0, -1, UNKNOWN_TOPIC, 0),
            new CommitResult(1, -1, UNKNOWN_TOPIC, -1)
        );
        assertThat(fixture.queryString("SELECT state FROM objects WHERE key = 'obj-empty'"))
            .isEqualTo("DELETE_PENDING");
        assertThat(fixture.queryString("SELECT delete_after::text FROM objects WHERE key = 'obj-empty'"))
            .isNotBlank();
        assertThat(fixture.queryLong("SELECT live_refs FROM objects WHERE key = 'obj-empty'")).isZero();
        assertThat(fixture.queryLong("SELECT count(*) FROM segments")).isZero();
        assertThat(nextOffset(live, 0)).isZero();
    }

    @Test
    void replayOfAFullyRejectedObjectRepeatsTheErrors() {
        long dead = fixture.createTopic("dead", 1);
        fixture.deleteTopic(dead);
        long epoch = fixture.register("b1");
        CommitEntry entry = CommitEntry.of(dead, 0, 1, 4);
        List<CommitResult> first = commit("b1", epoch, "obj-reject", 4, entry);
        List<CommitResult> second = commit("b1", epoch, "obj-reject", 4, entry);

        assertThat(second).isEqualTo(first);
        assertThat(fixture.queryLong("SELECT count(*) FROM objects")).isEqualTo(1);
        assertThat(fixture.queryLong("SELECT count(*) FROM segments")).isZero();
    }

    @Test
    void groupCommitAssignsContiguousOffsets() throws Exception {
        long topic = fixture.createTopic("grouped", 1);
        long epoch = fixture.register("b1");
        GroupCommitter committer = new GroupCommitter(fixture.coordinator, true);
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            Future<List<CommitResult>> left = pool.submit(() -> grouped(committer, start, "b1", epoch, "g-1", topic));
            Future<List<CommitResult>> right = pool.submit(() -> grouped(committer, start, "b1", epoch, "g-2", topic));
            start.countDown();
            List<CommitResult> a = left.get();
            List<CommitResult> b = right.get();
            assertThat(List.of(a.get(0).baseOffset(), b.get(0).baseOffset())).containsExactlyInAnyOrder(0L, 2L);
            assertThat(a.get(0).errorCode()).isZero();
            assertThat(b.get(0).errorCode()).isZero();
        } finally {
            committer.close();
        }
        assertThat(nextOffset(topic, 0)).isEqualTo(4);
        assertThat(fixture.queryLong("SELECT count(*) FROM objects")).isEqualTo(2);
    }

    private static List<CommitResult> grouped(
        GroupCommitter committer,
        CountDownLatch start,
        String broker,
        long epoch,
        String key,
        long topic
    ) throws InterruptedException {
        start.await();
        fixture.registry.createUploadIntent(key, broker, epoch);
        return committer.commitGrouped(request(broker, epoch, key, 4, CommitEntry.of(topic, 0, 2, 4)));
    }

    private static List<CommitResult> commit(String broker, long epoch, String key, long size, CommitEntry... entries) {
        fixture.registry.createUploadIntent(key, broker, epoch);
        return fixture.coordinator.commit(request(broker, epoch, key, size, entries));
    }

    private static CommitRequest request(String broker, long epoch, String key, long size, CommitEntry... entries) {
        return new CommitRequest(UUID.randomUUID(), key, size, broker, epoch, List.of(entries));
    }

    private static long nextOffset(long topic, int part) {
        try (Statement statement = fixture.admin.createStatement();
             ResultSet rows = statement.executeQuery(
                 "SELECT next_offset FROM partitions WHERE topic_id = " + topic + " AND part = " + part)) {
            rows.next();
            return rows.getLong(1);
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
