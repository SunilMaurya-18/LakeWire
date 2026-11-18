package lakewire.broker.write;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WriteBufferTest {

    @Test
    void lingerSealsInAppendOrder() {
        AtomicLong clock = new AtomicLong(1_000);
        List<List<BufferedChunk>> sealed = new ArrayList<>();
        WriteBuffer buffer = buffer(100, 1_000, clock, sealed);

        buffer.append(chunk(3), 1_000);
        buffer.append(chunk(4), 1_000);
        buffer.append(chunk(5), 1_000);
        assertThat(sealed).isEmpty();
        assertThat(buffer.openChunks()).isEqualTo(3);

        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(99));
        buffer.sealIfDue();
        assertThat(sealed).isEmpty();

        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1));
        buffer.sealIfDue();
        assertThat(sealed).hasSize(1);
        assertThat(sealed.get(0)).extracting(chunk -> chunk.bytes()).containsExactly(3, 4, 5);
        assertThat(buffer.openChunks()).isZero();
    }

    @Test
    void sizeSealsTheOpenObjectBeforeTheChunkThatWouldOverflow() {
        AtomicLong clock = new AtomicLong(0);
        List<List<BufferedChunk>> sealed = new ArrayList<>();
        WriteBuffer buffer = buffer(60_000, 10, clock, sealed);

        buffer.append(chunk(6), 1_000);
        assertThat(sealed).isEmpty();
        buffer.append(chunk(6), 1_000);

        assertThat(sealed).hasSize(1);
        assertThat(sealed.get(0)).extracting(chunk -> chunk.bytes()).containsExactly(6);
        assertThat(buffer.openChunks()).isEqualTo(1);
        assertThat(buffer.bufferedBytes()).isEqualTo(12);
    }

    @Test
    void aFullBufferWaitsAndThenTimesOut() throws Exception {
        AtomicLong clock = new AtomicLong(0);
        List<List<BufferedChunk>> sealed = new ArrayList<>();
        WriteBuffer buffer = new WriteBuffer(0, 8, 8, clock::get, sealed::add);
        buffer.append(chunk(6), 1_000);
        assertThat(buffer.bufferedBytes()).isEqualTo(6);

        CompletableFuture<ProduceAck> timedOut = buffer.append(chunk(6), 40);
        assertThat(timedOut.get(1, TimeUnit.SECONDS).errorCode()).isEqualTo(Errors.REQUEST_TIMED_OUT.code());
        assertThat(buffer.bufferedBytes()).isEqualTo(6);

        buffer.release(6);
        buffer.append(chunk(4), 1_000);
        assertThat(buffer.bufferedBytes()).isEqualTo(4);
    }

    @Test
    void aChunkLargerThanTheBufferIsNotQueued() throws Exception {
        WriteBuffer buffer = new WriteBuffer(0, 4, 4, () -> 0L, ignored -> { });
        ProduceAck ack = buffer.append(chunk(5), 1_000).get(1, TimeUnit.SECONDS);
        assertThat(ack.errorCode()).isEqualTo(Errors.REQUEST_TIMED_OUT.code());
        assertThat(buffer.bufferedBytes()).isZero();
    }

    private static WriteBuffer buffer(int lingerMs, int objectMax, AtomicLong clock, List<List<BufferedChunk>> sealed) {
        return new WriteBuffer(lingerMs, objectMax, 1_000_000, clock::get, sealed::add);
    }

    private static Append chunk(int bytes) {
        return new Append(1, 0, new byte[bytes], 1, (short) 1, 0, -1, (short) 0, -1, -1);
    }
}
