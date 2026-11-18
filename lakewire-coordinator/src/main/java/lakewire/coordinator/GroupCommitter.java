package lakewire.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * When the flag is on, the first caller waits up to 5 ms so later callers share one transaction.
 * The wait is {@link Object#wait(long)}, bounded by the window.
 */
public final class GroupCommitter implements AutoCloseable {

    static final long WINDOW_MS = 5;
    private static final long AWAIT_TIMEOUT_MS = 30_000;

    private final Coordinator coordinator;
    private final boolean enabled;
    private final Object monitor = new Object();
    private Batch open;
    private boolean closed;

    public GroupCommitter(Coordinator coordinator) {
        this(coordinator, coordinator.config().groupCommit());
    }

    public GroupCommitter(Coordinator coordinator, boolean enabled) {
        this.coordinator = coordinator;
        this.enabled = enabled;
    }

    public List<CommitResult> commit(CommitRequest request) {
        if (!enabled) {
            return coordinator.commit(request);
        }
        return commitGrouped(request);
    }

    public List<CommitResult> commitGrouped(CommitRequest request) {
        Slot slot;
        Batch leading = null;
        synchronized (monitor) {
            if (closed) {
                throw new CoordinatorException("group committer is closed");
            }
            boolean leader = open == null;
            if (leader) {
                open = new Batch();
                leading = open;
            }
            slot = open.enroll(request);
            if (leader) {
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WINDOW_MS);
                while (open == leading) {
                    long remainingNanos = deadline - System.nanoTime();
                    if (remainingNanos <= 0) {
                        break;
                    }
                    try {
                        monitor.wait(TimeUnit.NANOSECONDS.toMillis(remainingNanos) + 1);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        if (open == leading) {
                            open = null;
                        }
                        leading.fail(new CoordinatorException("group commit interrupted", interrupted));
                        throw new CoordinatorException("group commit interrupted", interrupted);
                    }
                }
                if (open == leading) {
                    open = null;
                } else {
                    leading = null;
                }
            }
        }
        if (leading != null) {
            try {
                leading.succeed(coordinator.commitGroup(leading.requests()));
            } catch (RuntimeException failure) {
                leading.fail(failure);
            }
        }
        return slot.await();
    }

    @Override
    public void close() {
        Batch abandoned;
        synchronized (monitor) {
            closed = true;
            abandoned = open;
            open = null;
            monitor.notifyAll();
        }
        if (abandoned != null) {
            abandoned.fail(new CoordinatorException("group committer is closed"));
        }
    }

    private static final class Batch {
        private final List<CommitRequest> requests = new ArrayList<>();
        private final List<Slot> slots = new ArrayList<>();
        private final AtomicBoolean finished = new AtomicBoolean();

        Slot enroll(CommitRequest request) {
            Slot slot = new Slot();
            requests.add(request);
            slots.add(slot);
            return slot;
        }

        List<CommitRequest> requests() {
            return List.copyOf(requests);
        }

        void succeed(List<List<CommitResult>> results) {
            if (!finished.compareAndSet(false, true)) {
                return;
            }
            for (int i = 0; i < slots.size(); i++) {
                slots.get(i).complete(results.get(i), null);
            }
        }

        void fail(RuntimeException failure) {
            if (!finished.compareAndSet(false, true)) {
                return;
            }
            for (Slot slot : slots) {
                slot.complete(null, failure);
            }
        }
    }

    private static final class Slot {
        private final CountDownLatch done = new CountDownLatch(1);
        private List<CommitResult> result;
        private RuntimeException failure;

        void complete(List<CommitResult> result, RuntimeException failure) {
            this.result = result;
            this.failure = failure;
            done.countDown();
        }

        List<CommitResult> await() {
            try {
                if (!done.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    throw new CoordinatorException("group commit timed out");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new CoordinatorException("group commit interrupted", interrupted);
            }
            if (failure != null) {
                if (failure instanceof CoordinatorException coordinatorException) {
                    throw coordinatorException;
                }
                throw new CoordinatorException("group commit failed", failure);
            }
            return result;
        }
    }
}
