package lakewire.workers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lakewire.coordinator.Coordinator;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.storage.ObjectStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One active worker via {@code pg_try_advisory_lock}. Losing the lock connection
 * drops leadership; jobs stop until the lock is acquired again.
 */
public final class WorkerRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerRuntime.class);

    private final WorkerConfig config;
    private final CoordinatorConfig coordinatorConfig;
    private final WorkerJobs jobs;
    private final AtomicBoolean open = new AtomicBoolean();
    private final AtomicBoolean leader = new AtomicBoolean();
    private final AtomicInteger lockPid = new AtomicInteger();
    private Thread loop;

    public WorkerRuntime(
        WorkerConfig config,
        Coordinator coordinator,
        ObjectStore store,
        WorkerMetrics metrics
    ) {
        this.config = config;
        this.coordinatorConfig = coordinator.config();
        this.jobs = new WorkerJobs(coordinator, store, config, metrics);
    }

    public WorkerJobs jobs() {
        return jobs;
    }

    public boolean isLeader() {
        return leader.get();
    }

    public int lockBackendPid() {
        return lockPid.get();
    }

    public void start() {
        if (!config.enabled() || !open.compareAndSet(false, true)) {
            return;
        }
        loop = Thread.ofVirtual().name("lakewire-workers").start(this::run);
    }

    private void run() {
        while (open.get() && !Thread.currentThread().isInterrupted()) {
            try (Connection connection = DriverManager.getConnection(
                coordinatorConfig.jdbcUrl(),
                coordinatorConfig.username(),
                coordinatorConfig.password()
            )) {
                connection.setAutoCommit(true);
                try (PreparedStatement name = connection.prepareStatement("SELECT set_config('application_name', 'lakewire-worker-lock', false)")) {
                    name.execute();
                }
                if (!tryLock(connection)) {
                    leader.set(false);
                    lockPid.set(0);
                    sleep(config.lockRetryMs());
                    continue;
                }
                lockPid.set(backendPid(connection));
                leader.set(true);
                log.info("worker lock acquired pid={}", lockPid.get());
                long lastGc = 0;
                long lastRetention = 0;
                long lastSweep = 0;
                long lastAudit = 0;
                long lastProducer = 0;
                while (open.get() && connection.isValid(2)) {
                    long now = System.nanoTime() / 1_000_000L;
                    if (now - lastGc >= config.gcPeriodMs()) {
                        jobs.orphanGc();
                        lastGc = now;
                    }
                    if (now - lastRetention >= config.retentionPeriodMs()) {
                        jobs.retention();
                        lastRetention = now;
                    }
                    if (now - lastSweep >= config.deleteSweepMs()) {
                        jobs.sweepDeletePending();
                        lastSweep = now;
                    }
                    if (now - lastAudit >= config.refAuditMs()) {
                        jobs.audit();
                        lastAudit = now;
                    }
                    if (now - lastProducer >= config.producerExpiryPeriodMs()) {
                        jobs.expireProducerState();
                        lastProducer = now;
                    }
                    sleep(Math.min(config.lockRetryMs(), 200));
                }
            } catch (SQLException failure) {
                log.warn("worker lock connection lost: {}", failure.toString());
            } finally {
                leader.set(false);
                lockPid.set(0);
            }
            if (open.get()) {
                sleep(config.lockRetryMs());
            }
        }
    }

    private static boolean tryLock(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
            statement.setLong(1, WorkerConfig.LOCK_KEY);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() && rows.getBoolean(1);
            }
        }
    }

    private static int backendPid(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_backend_pid()");
             ResultSet rows = statement.executeQuery()) {
            rows.next();
            return rows.getInt(1);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        open.set(false);
        leader.set(false);
        if (loop != null) {
            loop.interrupt();
            loop = null;
        }
    }
}
