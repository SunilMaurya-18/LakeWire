package lakewire.coordinator;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

class ConcurrentCommitIT {

    private static final int THREADS = 8;
    private static final int COMMITS = 10_000;
    private static final int PARTITIONS = 64;

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

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void tenThousandCommitsHaveNoHolesOrOverlaps() throws Exception {
        long seed = Long.getLong("lakewire.test.seed", 42L);
        System.out.println("ConcurrentCommitIT seed=" + seed);
        fixture.truncate();
        long topic = fixture.createTopic("concurrent", PARTITIONS);
        long epoch = fixture.register("b1");

        AtomicInteger failures = new AtomicInteger();
        ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor();
        heartbeats.scheduleAtFixedRate(() -> {
            if (!fixture.registry.heartbeat("b1", epoch)) {
                failures.incrementAndGet();
            }
        }, 0, 1, TimeUnit.SECONDS);
        try (ExecutorService pool = Executors.newFixedThreadPool(THREADS)) {
            List<Future<?>> tasks = new ArrayList<>();
            int perThread = COMMITS / THREADS;
            for (int thread = 0; thread < THREADS; thread++) {
                int threadId = thread;
                tasks.add(pool.submit(() -> commitBatch(seed, threadId, perThread, topic, epoch, failures)));
            }
            for (Future<?> task : tasks) {
                task.get(9, TimeUnit.MINUTES);
            }
        } finally {
            heartbeats.shutdownNow();
        }
        assertThat(failures.get()).as("seed %s commit errors", seed).isZero();

        Map<Integer, Long> nextOffsets = new HashMap<>();
        try (PreparedStatement statement = fixture.admin.prepareStatement(
            "SELECT part, next_offset FROM partitions WHERE topic_id = ? ORDER BY part")) {
            statement.setLong(1, topic);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    nextOffsets.put(rows.getInt(1), rows.getLong(2));
                }
            }
        }
        assertThat(nextOffsets).as("seed %s", seed).hasSize(PARTITIONS);

        Map<Integer, List<long[]>> segments = new HashMap<>();
        try (PreparedStatement statement = fixture.admin.prepareStatement("""
            SELECT part, base_offset, last_offset
            FROM segments
            WHERE topic_id = ?
            ORDER BY part, base_offset
            """)) {
            statement.setLong(1, topic);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    segments.computeIfAbsent(rows.getInt(1), ignored -> new ArrayList<>())
                        .add(new long[] {rows.getLong(2), rows.getLong(3)});
                }
            }
        }

        for (int part = 0; part < PARTITIONS; part++) {
            long next = nextOffsets.get(part);
            List<long[]> ranges = segments.getOrDefault(part, List.of());
            long cursor = 0;
            long records = 0;
            for (long[] range : ranges) {
                assertThat(range[0])
                    .as("seed %s part %s hole or overlap at %s", seed, part, cursor)
                    .isEqualTo(cursor);
                assertThat(range[1]).as("seed %s part %s", seed, part).isGreaterThanOrEqualTo(range[0]);
                long count = range[1] - range[0] + 1;
                records += count;
                cursor += count;
            }
            assertThat(cursor).as("seed %s part %s coverage", seed, part).isEqualTo(next);
            assertThat(records).as("seed %s part %s record sum", seed, part).isEqualTo(next);
        }
    }

    private static void commitBatch(
        long seed,
        int threadId,
        int commits,
        long topic,
        long epoch,
        AtomicInteger failures
    ) {
        java.util.SplittableRandom random = new java.util.SplittableRandom(seed + threadId);
        for (int i = 0; i < commits; i++) {
            int part = random.nextInt(PARTITIONS);
            int records = 1 + random.nextInt(5);
            String key = "c-" + threadId + "-" + i;
            try {
                fixture.registry.createUploadIntent(key, "b1", epoch);
                List<CommitResult> results = fixture.coordinator.commit(new CommitRequest(
                    UUID.randomUUID(),
                    key,
                    records,
                    "b1",
                    epoch,
                    List.of(CommitEntry.of(topic, part, records, records))
                ));
                if (results.size() != 1 || results.get(0).errorCode() != 0) {
                    failures.incrementAndGet();
                    throw new AssertionError("seed " + seed + " key " + key + " result " + results);
                }
            } catch (RuntimeException failure) {
                failures.incrementAndGet();
                throw new AssertionError("seed " + seed + " key " + key, failure);
            }
        }
    }
}
