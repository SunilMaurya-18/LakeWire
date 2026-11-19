package lakewire.broker.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lakewire.broker.write.WriteBuffer;

/**
 * Prometheus names from M10. Micrometer adds {@code _total} / {@code _seconds} on scrape.
 */
public final class LakeWireMetrics implements AutoCloseable {

    static final LakeWireMetrics NOOP = new LakeWireMetrics(false);

    private final boolean enabled;
    private final PrometheusMeterRegistry registry;
    private final Timer commitLatency;
    private final Timer produceLatency;
    private final Timer fetchLatency;
    private final AtomicLong objectBytes = new AtomicLong();

    public LakeWireMetrics() {
        this(true);
    }

    private LakeWireMetrics(boolean enabled) {
        this.enabled = enabled;
        PrometheusMeterRegistry created = enabled ? new PrometheusMeterRegistry(PrometheusConfig.DEFAULT) : null;
        this.registry = created;
        if (!enabled) {
            this.commitLatency = null;
            this.produceLatency = null;
            this.fetchLatency = null;
            return;
        }
        this.commitLatency = Timer.builder("lakewire.commit.latency")
            .publishPercentileHistogram()
            .publishPercentiles(0.5, 0.99)
            .register(registry);
        this.produceLatency = Timer.builder("lakewire.produce.latency")
            .publishPercentileHistogram()
            .publishPercentiles(0.5, 0.99)
            .register(registry);
        this.fetchLatency = Timer.builder("lakewire.fetch.latency")
            .publishPercentileHistogram()
            .publishPercentiles(0.5, 0.99)
            .register(registry);
        Counter.builder("lakewire.s3.put").tag("outcome", "ok").register(registry);
        Counter.builder("lakewire.s3.put").tag("outcome", "error").register(registry);
        Counter.builder("lakewire.s3.get").tag("outcome", "ok").register(registry);
        Counter.builder("lakewire.s3.get").tag("outcome", "error").register(registry);
        Counter.builder("lakewire.s3.get").tag("outcome", "not_found").register(registry);
        Counter.builder("lakewire.cache.hits").register(registry);
        Counter.builder("lakewire.cache.misses").register(registry);
        Counter.builder("lakewire.audit.violations").tag("invariant", "I7").register(registry);
        Counter.builder("lakewire.audit.violations").tag("invariant", "I8").register(registry);
        Gauge.builder("lakewire.object.bytes", objectBytes, AtomicLong::doubleValue).register(registry);
    }

    public static LakeWireMetrics noop() {
        return NOOP;
    }

    public void bindBuffer(WriteBuffer buffer) {
        if (!enabled) {
            return;
        }
        Gauge.builder("lakewire.buffer.bytes", buffer, b -> (double) b.bufferedBytes()).register(registry);
    }

    public void produceNanos(long nanos) {
        if (enabled) {
            produceLatency.record(nanos, TimeUnit.NANOSECONDS);
        }
    }

    public void fetchNanos(long nanos) {
        if (enabled) {
            fetchLatency.record(nanos, TimeUnit.NANOSECONDS);
        }
    }

    public void commitNanos(long nanos) {
        if (enabled) {
            commitLatency.record(nanos, TimeUnit.NANOSECONDS);
        }
    }

    public void objectBytes(long bytes) {
        if (enabled) {
            objectBytes.set(bytes);
        }
    }

    public void s3Put(String outcome) {
        if (enabled) {
            registry.counter("lakewire.s3.put", "outcome", outcome).increment();
        }
    }

    public void s3Get(String outcome) {
        if (enabled) {
            registry.counter("lakewire.s3.get", "outcome", outcome).increment();
        }
    }

    public void cacheHit() {
        if (enabled) {
            registry.counter("lakewire.cache.hits").increment();
        }
    }

    public void cacheMiss() {
        if (enabled) {
            registry.counter("lakewire.cache.misses").increment();
        }
    }

    public void auditViolation(String invariant) {
        if (enabled) {
            registry.counter("lakewire.audit.violations", "invariant", invariant).increment();
        }
    }

    public String scrape() {
        return enabled ? registry.scrape() : "";
    }

    @Override
    public void close() {
        if (enabled) {
            registry.close();
        }
    }
}
