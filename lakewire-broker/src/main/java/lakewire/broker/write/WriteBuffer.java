package lakewire.broker.write;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.kafka.common.protocol.Errors;

/**
 * One buffer for every partition. Seals when {@code lingerMs} has passed since the first
 * chunk or the open object reaches {@code objectMaxBytes}.
 */
public final class WriteBuffer {

    private final int lingerMs;
    private final int objectMaxBytes;
    private final long bufferMaxBytes;
    private final LongSupplier clock;
    private final Consumer<List<BufferedChunk>> onSeal;
    private final Object monitor = new Object();
    private final List<BufferedChunk> open = new ArrayList<>();
    private int openBytes;
    private long bufferedBytes;
    private long firstNano;

    public WriteBuffer(
        int lingerMs,
        int objectMaxBytes,
        long bufferMaxBytes,
        LongSupplier clock,
        Consumer<List<BufferedChunk>> onSeal
    ) {
        this.lingerMs = lingerMs;
        this.objectMaxBytes = objectMaxBytes;
        this.bufferMaxBytes = bufferMaxBytes;
        this.clock = clock;
        this.onSeal = onSeal;
    }

    public CompletableFuture<ProduceAck> append(Append append, long waitMs) {
        int size = append.records().length;
        synchronized (monitor) {
            if (size > bufferMaxBytes) {
                return CompletableFuture.completedFuture(ProduceAck.error(Errors.REQUEST_TIMED_OUT));
            }
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(waitMs, 0));
            while (bufferedBytes + size > bufferMaxBytes) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return CompletableFuture.completedFuture(ProduceAck.error(Errors.REQUEST_TIMED_OUT));
                }
                try {
                    monitor.wait(TimeUnit.NANOSECONDS.toMillis(remaining) + 1);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return CompletableFuture.completedFuture(ProduceAck.error(Errors.REQUEST_TIMED_OUT));
                }
            }
            if (!open.isEmpty() && openBytes + size > objectMaxBytes) {
                deliver(takeOpen());
            }
            BufferedChunk chunk = new BufferedChunk(append);
            open.add(chunk);
            openBytes += size;
            bufferedBytes += size;
            if (open.size() == 1) {
                firstNano = clock.getAsLong();
            }
            if (lingerMs <= 0 || openBytes >= objectMaxBytes) {
                deliver(takeOpen());
            }
            return chunk.ack();
        }
    }

    public void sealIfDue() {
        List<BufferedChunk> sealed = null;
        synchronized (monitor) {
            if (!open.isEmpty()
                && clock.getAsLong() - firstNano >= TimeUnit.MILLISECONDS.toNanos(lingerMs)) {
                sealed = takeOpen();
            }
        }
        if (sealed != null) {
            onSeal.accept(sealed);
        }
    }

    public void failOpen(short errorCode) {
        List<BufferedChunk> failed;
        synchronized (monitor) {
            failed = takeOpen();
            for (BufferedChunk chunk : failed) {
                bufferedBytes -= chunk.bytes();
            }
            monitor.notifyAll();
        }
        ProduceAck ack = new ProduceAck(errorCode, -1L, -1L);
        for (BufferedChunk chunk : failed) {
            chunk.ack().complete(ack);
        }
    }

    public void release(int bytes) {
        synchronized (monitor) {
            bufferedBytes -= bytes;
            if (bufferedBytes < 0) {
                bufferedBytes = 0;
            }
            monitor.notifyAll();
        }
    }

    public long bufferedBytes() {
        synchronized (monitor) {
            return bufferedBytes;
        }
    }

    public int openChunks() {
        synchronized (monitor) {
            return open.size();
        }
    }

    private List<BufferedChunk> takeOpen() {
        List<BufferedChunk> sealed = List.copyOf(open);
        open.clear();
        openBytes = 0;
        return sealed;
    }

    private void deliver(List<BufferedChunk> sealed) {
        if (!sealed.isEmpty()) {
            onSeal.accept(sealed);
        }
    }
}
