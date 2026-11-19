package lakewire.workers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/** In-process counters; the broker registers a Prometheus listener (A44). */
public final class WorkerMetrics {

    private final ConcurrentHashMap<String, LongAdder> violations = new ConcurrentHashMap<>();
    private volatile Consumer<String> listener = ignored -> { };

    public void setListener(Consumer<String> listener) {
        this.listener = listener == null ? ignored -> { } : listener;
    }

    public void auditViolation(String invariant) {
        violations.computeIfAbsent(invariant, ignored -> new LongAdder()).increment();
        listener.accept(invariant);
    }

    public long violations(String invariant) {
        LongAdder adder = violations.get(invariant);
        return adder == null ? 0 : adder.sum();
    }
}
