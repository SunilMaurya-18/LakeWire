package lakewire.broker.write;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import lakewire.coordinator.BrokerRegistry;
import lakewire.coordinator.CommitEntry;
import lakewire.coordinator.CommitRequest;
import lakewire.coordinator.CommitResult;
import lakewire.coordinator.Coordinator;
import lakewire.coordinator.CoordinatorException;
import lakewire.coordinator.GroupCommitter;
import lakewire.protocol.ErrorMapping;
import lakewire.storage.ObjectAlreadyExistsException;
import lakewire.storage.ObjectKeys;
import lakewire.storage.ObjectStore;
import lakewire.storage.TransientStorageException;
import lakewire.storage.UuidV7;
import lakewire.storage.lkw1.Lkw1Chunk;
import lakewire.storage.lkw1.Lkw1Writer;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Seals objects, uploads them create-only, then commits in seal order.
 * Uploads may finish out of order. A commit is acked only after the PUT and the SQL call.
 */
public final class WritePipeline implements ProduceSink, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WritePipeline.class);
    private static final int PUT_ATTEMPTS = 8;
    private static final int COMMIT_ATTEMPTS = 3;

    private final WriteSettings settings;
    private final ObjectStore store;
    private final BrokerRegistry registry;
    private final GroupCommitter commits;
    private final LongSupplier epoch;
    private final AtomicBoolean accepting;
    private final Runnable onFence;
    private final String brokerId;
    private final Semaphore uploads;
    private final LinkedBlockingQueue<Sealed> commitQueue = new LinkedBlockingQueue<>();
    private final List<Sealed> inflight = new CopyOnWriteArrayList<>();
    private final WriteBuffer buffer;
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread sealer;
    private Thread committer;

    public WritePipeline(
        WriteSettings settings,
        ObjectStore store,
        Coordinator coordinator,
        LongSupplier epoch,
        AtomicBoolean accepting,
        int brokerId,
        Runnable onFence
    ) {
        this.settings = settings;
        this.store = store;
        this.registry = new BrokerRegistry(coordinator);
        this.commits = new GroupCommitter(coordinator);
        this.epoch = epoch;
        this.accepting = accepting;
        this.onFence = onFence == null ? () -> { } : onFence;
        this.brokerId = Integer.toString(brokerId);
        this.uploads = new Semaphore(settings.uploadConcurrency());
        this.buffer = new WriteBuffer(
            settings.lingerMs(),
            settings.objectMaxBytes(),
            settings.bufferMaxBytes(),
            System::nanoTime,
            this::enqueue
        );
    }

    @Override
    public boolean accepting() {
        return accepting.get();
    }

    @Override
    public int objectMaxBytes() {
        return settings.objectMaxBytes();
    }

    public WriteSettings settings() {
        return settings;
    }

    public WriteBuffer buffer() {
        return buffer;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        sealer = Thread.ofVirtual().name("lakewire-seal").start(this::sealLoop);
        committer = Thread.ofVirtual().name("lakewire-commit").start(this::commitLoop);
    }

    @Override
    public CompletableFuture<ProduceAck> append(Append append, long waitMs) {
        if (!accepting.get()) {
            return CompletableFuture.completedFuture(ProduceAck.error(Errors.NOT_LEADER_OR_FOLLOWER));
        }
        return buffer.append(append, waitMs);
    }

    public void failAll(short errorCode) {
        buffer.failOpen(errorCode);
        for (Sealed sealed : inflight) {
            sealed.fail(errorCode);
        }
    }

    private void sealLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            buffer.sealIfDue();
            try {
                Thread.sleep(5);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void enqueue(List<BufferedChunk> chunks) {
        int bytes = 0;
        for (BufferedChunk chunk : chunks) {
            bytes += chunk.bytes();
        }
        Sealed sealed = new Sealed(chunks, bytes);
        inflight.add(sealed);
        commitQueue.add(sealed);
        Thread.ofVirtual().name("lakewire-upload").start(() -> upload(sealed));
    }

    private void upload(Sealed sealed) {
        try {
            long currentEpoch = epoch.getAsLong();
            Instant created = Instant.now();
            UUID objectId = UuidV7.next();
            String key = ObjectKeys.objectKey(settings.objectPrefix(), brokerId, currentEpoch, objectId, created);
            registry.createUploadIntent(key, brokerId, currentEpoch);
            List<Lkw1Chunk> lkw = new ArrayList<>(sealed.chunks.size());
            for (BufferedChunk chunk : sealed.chunks) {
                Append append = chunk.append();
                lkw.add(new Lkw1Chunk(
                    append.topicId(),
                    append.partition(),
                    append.records(),
                    append.nRecords(),
                    append.nBatches(),
                    append.maxTimestamp()
                ));
            }
            byte[] body = Lkw1Writer.write(brokerId, currentEpoch, created.toEpochMilli(), lkw);
            long[] starts = Lkw1Writer.chunkStarts(brokerId, lkw);
            putWithRetry(key, body);
            List<CommitEntry> entries = new ArrayList<>(lkw.size());
            for (int i = 0; i < lkw.size(); i++) {
                Append append = sealed.chunks.get(i).append();
                entries.add(new CommitEntry(
                    append.topicId(),
                    append.partition(),
                    starts[i],
                    append.records().length,
                    append.nRecords(),
                    append.nBatches(),
                    append.maxTimestamp(),
                    append.producerId(),
                    append.producerEpoch(),
                    append.firstSeq(),
                    append.lastSeq()
                ));
            }
            sealed.prepared.complete(new CommitRequest(objectId, key, body.length, brokerId, currentEpoch, entries));
        } catch (Exception failure) {
            log.warn("upload failed: {}", failure.toString());
            sealed.prepared.completeExceptionally(failure);
        }
    }

    private void putWithRetry(String key, byte[] body) throws InterruptedException {
        TransientStorageException last = null;
        for (int attempt = 1; attempt <= PUT_ATTEMPTS; attempt++) {
            uploads.acquire();
            try {
                store.put(key, body).join();
                return;
            } catch (CompletionException failure) {
                Throwable cause = unwrap(failure);
                if (cause instanceof ObjectAlreadyExistsException) {
                    return;
                }
                if (cause instanceof TransientStorageException transientFailure && attempt < PUT_ATTEMPTS) {
                    last = transientFailure;
                } else if (cause instanceof RuntimeException runtime) {
                    throw runtime;
                } else {
                    throw new TransientStorageException("put failed for " + key, cause);
                }
            } finally {
                uploads.release();
            }
            Thread.sleep(20L * attempt);
        }
        throw last == null ? new TransientStorageException("put failed for " + key) : last;
    }

    private void commitLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            Sealed sealed;
            try {
                sealed = commitQueue.take();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                if (sealed.failed.get()) {
                    continue;
                }
                CommitRequest request = sealed.prepared.join();
                if (sealed.failed.get()) {
                    continue;
                }
                List<CommitResult> results = commitWithReplay(request);
                sealed.succeed(results);
                if (fenced(results)) {
                    accepting.set(false);
                    onFence.run();
                }
            } catch (CompletionException | CoordinatorException failure) {
                log.warn("commit failed: {}", failure.toString());
                Errors error = storage(failure)
                    ? ErrorMapping.STORAGE_RETRIES_EXHAUSTED.error()
                    : Errors.REQUEST_TIMED_OUT;
                sealed.fail(error.code());
            } finally {
                buffer.release(sealed.bytes);
                inflight.remove(sealed);
            }
        }
    }

    private List<CommitResult> commitWithReplay(CommitRequest request) {
        CoordinatorException last = null;
        for (int attempt = 1; attempt <= COMMIT_ATTEMPTS; attempt++) {
            try {
                return commits.commit(request);
            } catch (CoordinatorException failure) {
                last = failure;
            }
        }
        throw last;
    }

    /** Epoch or lease rejection returns NOT_LEADER on every entry and writes nothing. */
    static boolean fenced(List<CommitResult> results) {
        if (results == null || results.isEmpty()) {
            return false;
        }
        for (CommitResult result : results) {
            if (result.errorCode() != Errors.NOT_LEADER_OR_FOLLOWER.code()) {
                return false;
            }
        }
        return true;
    }

    private static boolean storage(Throwable failure) {
        Throwable cause = unwrap(failure);
        return cause instanceof TransientStorageException || cause instanceof ObjectAlreadyExistsException;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    @Override
    public void close() {
        running.set(false);
        if (sealer != null) {
            sealer.interrupt();
        }
        if (committer != null) {
            committer.interrupt();
        }
        failAll(Errors.REQUEST_TIMED_OUT.code());
        commits.close();
    }

    private static final class Sealed {
        private final List<BufferedChunk> chunks;
        private final int bytes;
        private final CompletableFuture<CommitRequest> prepared = new CompletableFuture<>();
        private final AtomicBoolean failed = new AtomicBoolean();

        private Sealed(List<BufferedChunk> chunks, int bytes) {
            this.chunks = chunks;
            this.bytes = bytes;
        }

        private void succeed(List<CommitResult> results) {
            if (!failed.compareAndSet(false, true)) {
                return;
            }
            ProduceAck[] acks = new ProduceAck[chunks.size()];
            for (int i = 0; i < acks.length; i++) {
                acks[i] = ProduceAck.error(ErrorMapping.STORAGE_RETRIES_EXHAUSTED.error());
            }
            for (CommitResult result : results) {
                if (result.idx() < 0 || result.idx() >= acks.length) {
                    continue;
                }
                if (result.errorCode() == Errors.NONE.code()) {
                    acks[result.idx()] = ProduceAck.ok(result.baseOffset(), result.logStartOffset());
                } else {
                    acks[result.idx()] = new ProduceAck(result.errorCode(), -1L, result.logStartOffset());
                }
            }
            for (int i = 0; i < chunks.size(); i++) {
                chunks.get(i).ack().complete(acks[i]);
            }
        }

        private void fail(short errorCode) {
            if (!failed.compareAndSet(false, true)) {
                return;
            }
            prepared.completeExceptionally(new CoordinatorException("upload failed"));
            ProduceAck ack = new ProduceAck(errorCode, -1L, -1L);
            for (BufferedChunk chunk : chunks) {
                chunk.ack().complete(ack);
            }
        }
    }
}
