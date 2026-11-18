package lakewire.bench.coordinator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lakewire.coordinator.BrokerRegistry;
import lakewire.coordinator.CommitEntry;
import lakewire.coordinator.CommitRequest;
import lakewire.coordinator.CommitResult;
import lakewire.coordinator.Coordinator;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.coordinator.GroupCommitter;
import lakewire.testkit.LakeWireContainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Day-3 commit latency lab. Not bound to the Maven lifecycle.
 * Run: mvnw -pl bench -am exec:java
 */
public final class CoordinatorLatencyLab {

    private static final int[] PARTITIONS = {1, 16, 128, 512, 1024};
    private static final int[] COMMITTERS = {1, 3, 6, 12};
    private static final int TOPIC_PARTITIONS = 1024;

    public static void main(String[] args) throws Exception {
        PostgreSQLContainer postgres = LakeWireContainers.postgres().withUrlParam("socketTimeout", "180");
        postgres.start();
        Coordinator coordinator = null;
        ScheduledExecutorService heartbeats = null;
        try {
            String jdbcUrl = postgres.getJdbcUrl();
            CoordinatorConfig config = CoordinatorConfig.of(jdbcUrl, postgres.getUsername(), postgres.getPassword());
            coordinator = new Coordinator(config);
            coordinator.start();
            BrokerRegistry registry = new BrokerRegistry(coordinator);
            long epoch = registry.register("lab", "az1", "lab:9092");
            heartbeats = Executors.newSingleThreadScheduledExecutor();
            heartbeats.scheduleAtFixedRate(() -> {
                try {
                    registry.heartbeat("lab", epoch);
                } catch (RuntimeException failure) {
                    System.err.println("lab heartbeat failed: " + failure.getMessage());
                }
            }, 0, 1, TimeUnit.SECONDS);
            long topicId = createTopic(coordinator, TOPIC_PARTITIONS);
            String fsync = query(coordinator, "SHOW fsync");
            String serverVersion = query(coordinator, "SHOW server_version");

            List<Sample> samples = new ArrayList<>();
            for (Strategy strategy : Strategy.values()) {
                for (int partitions : PARTITIONS) {
                    for (int committers : COMMITTERS) {
                        samples.add(measure(coordinator, registry, epoch, topicId, strategy, partitions, committers));
                    }
                }
            }

            LocalDate date = LocalDate.now();
            Path directory = outputDirectory();
            Files.createDirectories(directory);
            Path csv = directory.resolve("results-" + date + ".csv");
            Path svg = directory.resolve("commit-latency-" + date + ".svg");
            writeCsv(csv, samples, fsync, serverVersion);
            LatencyChart.write(svg, samples);
            System.out.println("wrote " + csv.toAbsolutePath());
            System.out.println("wrote " + svg.toAbsolutePath());
        } finally {
            if (heartbeats != null) {
                heartbeats.shutdownNow();
            }
            if (coordinator != null) {
                coordinator.close();
            }
            try {
                postgres.stop();
            } catch (RuntimeException failure) {
                System.err.println("postgres stop: " + failure.getMessage());
            }
        }
    }

    /** exec:java runs in-process, so the working directory is the repo root, not bench/. */
    private static Path outputDirectory() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        if (cwd.getFileName() != null && "bench".equals(cwd.getFileName().toString())) {
            return cwd.resolve("coordinator");
        }
        return cwd.resolve("bench").resolve("coordinator");
    }

    private static Sample measure(
        Coordinator coordinator,
        BrokerRegistry registry,
        long epoch,
        long topicId,
        Strategy strategy,
        int partitions,
        int committers
    ) throws Exception {
        int warmup = partitions >= 512 ? 1 : 3;
        int measured = partitions >= 1024 ? 3 : partitions >= 512 ? 4 : 12;
        List<CommitEntry> entries = new ArrayList<>(partitions);
        for (int part = 0; part < partitions; part++) {
            entries.add(CommitEntry.of(topicId, part, 1, 32));
        }
        GroupCommitter group = strategy == Strategy.GROUP ? new GroupCommitter(coordinator, true) : null;
        try {
            runWave(coordinator, registry, group, epoch, strategy, entries, committers, warmup, false);
            List<Long> nanos = runWave(
                coordinator, registry, group, epoch, strategy, entries, committers, measured, true);
            Sample sample = Sample.from(strategy, partitions, committers, nanos);
            System.out.printf(
                Locale.ROOT,
                "%s parts=%d committers=%d samples=%d p50=%.2f p99=%.2f mean=%.2f%n",
                strategy.wireName,
                partitions,
                committers,
                sample.samples,
                sample.p50Ms,
                sample.p99Ms,
                sample.meanMs
            );
            return sample;
        } finally {
            if (group != null) {
                group.close();
            }
        }
    }

    private static List<Long> runWave(
        Coordinator coordinator,
        BrokerRegistry registry,
        GroupCommitter group,
        long epoch,
        Strategy strategy,
        List<CommitEntry> entries,
        int committers,
        int perThread,
        boolean record
    ) throws Exception {
        if (perThread == 0) {
            return List.of();
        }
        try (ExecutorService pool = Executors.newFixedThreadPool(committers)) {
            List<Future<long[]>> tasks = new ArrayList<>();
            for (int thread = 0; thread < committers; thread++) {
                int threadId = thread;
                tasks.add(pool.submit(() -> oneThread(
                    coordinator, registry, group, epoch, strategy, entries, threadId, perThread)));
            }
            List<Long> all = new ArrayList<>();
            for (Future<long[]> task : tasks) {
                for (long nanos : task.get(30, TimeUnit.MINUTES)) {
                    if (record) {
                        all.add(nanos);
                    }
                }
            }
            return all;
        }
    }

    private static long[] oneThread(
        Coordinator coordinator,
        BrokerRegistry registry,
        GroupCommitter group,
        long epoch,
        Strategy strategy,
        List<CommitEntry> entries,
        int threadId,
        int iterations
    ) {
        long[] nanos = new long[iterations];
        for (int i = 0; i < iterations; i++) {
            String key = strategy.wireName + "-" + entries.size() + "-" + threadId + "-" + i + "-" + UUID.randomUUID();
            registry.createUploadIntent(key, "lab", epoch);
            CommitRequest request = new CommitRequest(
                UUID.randomUUID(), key, entries.size() * 32L, "lab", epoch, entries);
            long started = System.nanoTime();
            List<CommitResult> results = switch (strategy) {
                case ROW_LOOP -> coordinator.commitRowLoop(request);
                case SET_BASED -> coordinator.commit(request);
                case GROUP -> group.commitGrouped(request);
            };
            nanos[i] = System.nanoTime() - started;
            if (results.size() != entries.size()) {
                throw new IllegalStateException(key + " returned " + results.size() + " rows");
            }
            for (CommitResult result : results) {
                if (result.errorCode() != 0) {
                    throw new IllegalStateException(key + " error " + result.errorCode() + " at " + result.idx());
                }
            }
        }
        return nanos;
    }

    private static long createTopic(Coordinator coordinator, int partitions) throws Exception {
        try (var connection = coordinator.dataSource().getConnection();
             var insert = connection.prepareStatement(
                 "INSERT INTO topics (name, partitions) VALUES ('lab', ?) RETURNING id")) {
            insert.setInt(1, partitions);
            long topicId;
            try (var rows = insert.executeQuery()) {
                rows.next();
                topicId = rows.getLong(1);
            }
            try (var parts = connection.prepareStatement(
                "INSERT INTO partitions (topic_id, part) VALUES (?, ?)")) {
                for (int part = 0; part < partitions; part++) {
                    parts.setLong(1, topicId);
                    parts.setInt(2, part);
                    parts.addBatch();
                }
                parts.executeBatch();
            }
            return topicId;
        }
    }

    private static String query(Coordinator coordinator, String sql) throws Exception {
        try (var connection = coordinator.dataSource().getConnection();
             var statement = connection.createStatement();
             var rows = statement.executeQuery(sql)) {
            rows.next();
            return rows.getString(1);
        }
    }

    private static void writeCsv(Path csv, List<Sample> samples, String fsync, String serverVersion) throws Exception {
        String header = """
            # machine=%s %s processors=%d
            # java=%s
            # postgres=postgres:18.7-alpine server_version=%s fsync=%s
            # date=%s
            # note=client-visible commit latency only (intent insert is excluded). Every commit updates partitions 0..N-1, so committers contend on the same rows. Group commit waits up to 5 ms. Testcontainers PostgreSQLContainer sets fsync=off, so this lab does not measure fsync amortization.
            strategy,partitions_per_commit,committers,samples,p50_ms,p99_ms,mean_ms
            """.formatted(
            System.getProperty("os.name"),
            System.getProperty("os.arch"),
            Runtime.getRuntime().availableProcessors(),
            System.getProperty("java.version"),
            serverVersion,
            fsync,
            LocalDate.now()
        );
        StringBuilder body = new StringBuilder(header);
        for (Sample sample : samples) {
            body.append(sample.strategy.wireName).append(',')
                .append(sample.partitions).append(',')
                .append(sample.committers).append(',')
                .append(sample.samples).append(',')
                .append(String.format(Locale.ROOT, "%.3f", sample.p50Ms)).append(',')
                .append(String.format(Locale.ROOT, "%.3f", sample.p99Ms)).append(',')
                .append(String.format(Locale.ROOT, "%.3f", sample.meanMs)).append('\n');
        }
        Files.writeString(csv, body.toString());
    }

    enum Strategy {
        ROW_LOOP("row-loop"),
        SET_BASED("set-based"),
        GROUP("group-commit");

        final String wireName;

        Strategy(String wireName) {
            this.wireName = wireName;
        }
    }

    record Sample(
        Strategy strategy,
        int partitions,
        int committers,
        int samples,
        double p50Ms,
        double p99Ms,
        double meanMs
    ) {
        static Sample from(Strategy strategy, int partitions, int committers, List<Long> nanos) {
            long[] sorted = nanos.stream().mapToLong(Long::longValue).sorted().toArray();
            double mean = Arrays.stream(sorted).average().orElse(0) / 1_000_000.0;
            return new Sample(
                strategy,
                partitions,
                committers,
                sorted.length,
                percentile(sorted, 50) / 1_000_000.0,
                percentile(sorted, 99) / 1_000_000.0,
                mean
            );
        }

        private static double percentile(long[] sorted, int percent) {
            if (sorted.length == 0) {
                return 0;
            }
            int index = (int) Math.ceil(percent / 100.0 * sorted.length) - 1;
            index = Math.max(0, Math.min(sorted.length - 1, index));
            return sorted[index];
        }
    }
}
