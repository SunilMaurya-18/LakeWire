package lakewire.broker.read;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import lakewire.broker.metrics.LakeWireMetrics;
import lakewire.storage.ObjectNotFoundException;
import lakewire.storage.ObjectStore;
import lakewire.storage.TransientStorageException;

/** Byte-weighted cache of chunk bytes. Hit and miss counts feed the M10 cache-hit ratio. */
public final class BlockCache {

    private final ObjectStore store;
    private final long maxWeight;
    private final Cache<BlockKey, byte[]> cache;
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final LakeWireMetrics metrics;

    public BlockCache(ObjectStore store, long maxWeight) {
        this(store, maxWeight, LakeWireMetrics.noop());
    }

    public BlockCache(ObjectStore store, long maxWeight, LakeWireMetrics metrics) {
        this.store = store;
        this.maxWeight = maxWeight;
        this.metrics = metrics == null ? LakeWireMetrics.noop() : metrics;
        this.cache = Caffeine.newBuilder()
            .maximumWeight(maxWeight)
            .weigher((BlockKey key, byte[] value) -> value.length)
            .build();
    }

    public byte[] get(String key, long start, int length) {
        BlockKey id = new BlockKey(key, start, length);
        byte[] cached = cache.getIfPresent(id);
        if (cached != null) {
            hits.incrementAndGet();
            metrics.cacheHit();
            return cached.clone();
        }
        misses.incrementAndGet();
        metrics.cacheMiss();
        byte[] loaded = await(store.getRange(key, start, length));
        if (loaded.length > 0 && loaded.length <= maxWeight) {
            cache.put(id, loaded);
        }
        return loaded.clone();
    }

    public void invalidate(String key, long start, int length) {
        cache.invalidate(new BlockKey(key, start, length));
    }

    public long hits() {
        return hits.get();
    }

    public long misses() {
        return misses.get();
    }

    private static byte[] await(CompletableFuture<byte[]> future) {
        try {
            byte[] body = future.get(30, TimeUnit.SECONDS);
            return body == null ? new byte[0] : body;
        } catch (TimeoutException timeout) {
            throw new TransientStorageException("read timed out", timeout);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new TransientStorageException("read interrupted", interrupted);
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause() == null ? failure : failure.getCause();
            if (cause instanceof ObjectNotFoundException missing) {
                throw missing;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new TransientStorageException("read failed", cause);
        }
    }

    private record BlockKey(String key, long start, int length) {
    }
}
