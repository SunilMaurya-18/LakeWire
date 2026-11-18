package lakewire.broker.read;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lakewire.coordinator.HwListener;
import lakewire.coordinator.HwUpdate;

/**
 * Cached high watermarks. A fetch at or above the cached watermark re-reads Postgres
 * before it chooses between waiting and {@code OFFSET_OUT_OF_RANGE}.
 */
public final class HwCache {

    private final WatermarkSource source;
    private final ConcurrentHashMap<Key, Watermark> cache = new ConcurrentHashMap<>();
    private final AtomicLong generation = new AtomicLong();
    private final Object monitor = new Object();

    public HwCache(WatermarkSource source, HwListener listener) {
        this.source = source;
        if (listener != null) {
            listener.subscribe(this::observe);
        }
    }

    public Watermark get(long topicId, int part) {
        Watermark cached = cache.get(new Key(topicId, part));
        if (cached != null) {
            return cached;
        }
        return refresh(topicId, part);
    }

    public Watermark refresh(long topicId, int part) {
        Watermark loaded = source.watermark(topicId, part);
        cache.put(new Key(topicId, part), loaded);
        return loaded;
    }

    public void put(long topicId, int part, Watermark watermark) {
        cache.put(new Key(topicId, part), watermark);
    }

    public long generation() {
        return generation.get();
    }

    public void await(long observedGeneration, long deadlineNanos) {
        synchronized (monitor) {
            while (generation.get() == observedGeneration) {
                long remainingMs = (deadlineNanos - System.nanoTime()) / 1_000_000L;
                if (remainingMs <= 0) {
                    return;
                }
                try {
                    monitor.wait(remainingMs);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void observe(HwUpdate update) {
        cache.compute(new Key(update.topicId(), update.part()), (key, current) -> {
            if (current == null || update.nextOffset() < current.highWatermark()) {
                return current;
            }
            return new Watermark(update.nextOffset(), current.logStartOffset());
        });
        generation.incrementAndGet();
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }

    private record Key(long topicId, int part) {
    }
}
