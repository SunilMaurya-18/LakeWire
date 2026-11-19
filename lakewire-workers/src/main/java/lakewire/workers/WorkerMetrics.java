package lakewire.workers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/** In-process counters until M10 serves Prometheus (A44). */
public final class WorkerMetrics {

    private final ConcurrentHashMap<String, LongAdder> violations = new ConcurrentHashMap<>();

    public void auditViolation(String invariant) {
        violations.computeIfAbsent(invariant, ignored -> new LongAdder()).increment();
    }

    public long violations(String invariant) {
        LongAdder adder = violations.get(invariant);
        return adder == null ? 0 : adder.sum();
    }
}
