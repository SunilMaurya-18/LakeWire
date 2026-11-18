package lakewire.storage;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Test and nemesis wrapper. Fault decisions are drawn on the calling thread,
 * so the same seed and the same sequence of calls produce the same faults.
 */
public final class FaultInjectingObjectStore implements ObjectStore {

    private final ObjectStore delegate;
    private final java.util.SplittableRandom random;
    private final double putFailureRate;
    private final Duration extraPutLatency;
    private final double getNotFoundRate;

    public FaultInjectingObjectStore(
        ObjectStore delegate,
        long seed,
        double putFailureRate,
        Duration extraPutLatency,
        double getNotFoundRate
    ) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (putFailureRate < 0 || putFailureRate > 1 || getNotFoundRate < 0 || getNotFoundRate > 1) {
            throw new IllegalArgumentException("fault rates must be between 0 and 1");
        }
        if (extraPutLatency == null || extraPutLatency.isNegative()) {
            throw new IllegalArgumentException("extraPutLatency must not be negative");
        }
        this.random = new java.util.SplittableRandom(seed);
        this.putFailureRate = putFailureRate;
        this.extraPutLatency = extraPutLatency;
        this.getNotFoundRate = getNotFoundRate;
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] body) {
        boolean fail = random.nextDouble() < putFailureRate;
        if (fail) {
            return CompletableFuture.failedFuture(new TransientStorageException("injected 5xx for " + key));
        }
        CompletableFuture<Void> stored = delegate.put(key, body);
        if (extraPutLatency.isZero()) {
            return stored;
        }
        return stored.thenCompose(ignored -> CompletableFuture.runAsync(
            () -> { },
            CompletableFuture.delayedExecutor(extraPutLatency.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
        ));
    }

    @Override
    public CompletableFuture<byte[]> getRange(String key, long start, int length) {
        boolean missing = random.nextDouble() < getNotFoundRate;
        if (missing) {
            return CompletableFuture.failedFuture(new ObjectNotFoundException(key));
        }
        return delegate.getRange(key, start, length);
    }

    @Override
    public CompletableFuture<ObjectHead> head(String key) {
        return delegate.head(key);
    }

    @Override
    public CompletableFuture<Void> delete(String key) {
        return delegate.delete(key);
    }

    @Override
    public CompletableFuture<List<String>> list(String prefix) {
        return delegate.list(prefix);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
