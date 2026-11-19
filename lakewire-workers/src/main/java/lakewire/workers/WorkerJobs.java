package lakewire.workers;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lakewire.coordinator.Coordinator;
import lakewire.coordinator.RefcountMismatch;
import lakewire.storage.ObjectNotFoundException;
import lakewire.storage.ObjectStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Orphan GC, retention, DELETE_PENDING sweep, ref audit, and producer-state expiry. */
public final class WorkerJobs {

    private static final Logger log = LoggerFactory.getLogger(WorkerJobs.class);
    private static final long STORE_TIMEOUT_SECONDS = 30;

    private final Coordinator coordinator;
    private final ObjectStore store;
    private final WorkerConfig config;
    private final WorkerMetrics metrics;

    public WorkerJobs(Coordinator coordinator, ObjectStore store, WorkerConfig config, WorkerMetrics metrics) {
        this.coordinator = coordinator;
        this.store = store;
        this.config = config;
        this.metrics = metrics;
    }

    public void orphanGc() {
        for (String key : coordinator.gcExpiredIntents(config.orphanTtlMs())) {
            deleteIdempotent(key);
        }
        List<String> listed;
        try {
            listed = store.list(config.objectPrefix()).get(STORE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("list interrupted", interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IllegalStateException("list failed", failure);
        }
        for (String key : coordinator.uncommittableKeys(listed)) {
            deleteIdempotent(key);
        }
    }

    public void retention() {
        coordinator.applyRetention(config.graceMs());
    }

    public void sweepDeletePending() {
        for (String key : coordinator.dueDeletePending()) {
            deleteIdempotent(key);
            coordinator.forgetObject(key);
        }
    }

    public void audit() {
        for (RefcountMismatch mismatch : coordinator.auditRefcounts()) {
            log.error("I7 refcount mismatch key={} live_refs={} actual_refs={} live_bytes={} actual_bytes={}",
                mismatch.key(), mismatch.liveRefs(), mismatch.actualRefs(),
                mismatch.liveBytes(), mismatch.actualBytes());
            metrics.auditViolation("I7");
        }
        List<String> keys = coordinator.committedObjectKeys();
        int cap = config.auditHeadCap();
        int n = cap == 0 ? 0 : Math.min(cap, keys.size());
        for (int i = 0; i < n; i++) {
            String key = keys.get(i);
            try {
                store.head(key).get(STORE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("head interrupted", interrupted);
            } catch (TimeoutException failure) {
                log.warn("I7 HEAD timed out for {}", key);
            } catch (ExecutionException failure) {
                if (root(failure) instanceof ObjectNotFoundException) {
                    log.error("I7 COMMITTED object missing key={}", key);
                    metrics.auditViolation("I7");
                } else {
                    log.warn("I7 HEAD failed for {}: {}", key, failure.toString());
                }
            }
        }
    }

    public void expireProducerState() {
        coordinator.expireProducerState(config.producerTtlMs());
    }

    private void deleteIdempotent(String key) {
        try {
            store.delete(key).get(STORE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("delete interrupted", interrupted);
        } catch (TimeoutException failure) {
            throw new IllegalStateException("delete timed out for " + key, failure);
        } catch (ExecutionException failure) {
            if (root(failure) instanceof ObjectNotFoundException) {
                return;
            }
            throw new IllegalStateException("delete failed for " + key, failure);
        }
    }

    private static Throwable root(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
