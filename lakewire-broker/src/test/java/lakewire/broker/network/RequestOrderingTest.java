package lakewire.broker.network;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lakewire.broker.config.BrokerConfig;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

class RequestOrderingTest {

    private static final BrokerConfig CONFIG = new BrokerConfig("127.0.0.1", 0, 1_048_576);

    @Test
    void requestsOnOneConnectionAreProcessedOneAtATimeInOrder() throws Exception {
        int requests = 10;
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxRunning = new AtomicInteger();
        List<Integer> processOrder = new CopyOnWriteArrayList<>();
        List<Boolean> ranOnVirtualThread = new CopyOnWriteArrayList<>();

        // Echoes the 4-byte id it receives. Early requests sleep longer, so if the broker ran them
        // at the same time, later ones would overlap with them and finish first.
        KafkaServer server = new KafkaServer(CONFIG, frame -> {
            int id = frame.getInt();
            maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            ranOnVirtualThread.add(Thread.currentThread().isVirtual());
            sleep((requests - id) * 20L);
            processOrder.add(id);
            running.decrementAndGet();
            return Optional.of(ByteBuffer.allocate(4).putInt(0, id));
        });
        server.start();
        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            for (int id = 0; id < requests; id++) {
                client.send(ByteBuffer.allocate(4).putInt(0, id));
            }
            List<Integer> received = new ArrayList<>();
            for (int i = 0; i < requests; i++) {
                received.add(client.receive().getInt());
            }

            assertThat(received).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            assertThat(processOrder).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            assertThat(maxRunning.get()).isEqualTo(1);
            assertThat(ranOnVirtualThread).containsOnly(true);
        } finally {
            server.stop();
        }
    }

    @Test
    void differentConnectionsAreProcessedInParallel() throws Exception {
        // Each handler waits until the other connection's handler has also started.
        // If connections were processed one after another, the latch would time out (5 s).
        CountDownLatch bothStarted = new CountDownLatch(2);
        List<Boolean> sawOtherConnection = new CopyOnWriteArrayList<>();
        List<Boolean> ranOnVirtualThread = new CopyOnWriteArrayList<>();

        KafkaServer server = new KafkaServer(CONFIG, frame -> {
            int id = frame.getInt();
            ranOnVirtualThread.add(Thread.currentThread().isVirtual());
            bothStarted.countDown();
            try {
                sawOtherConnection.add(bothStarted.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            sleep(500);
            return Optional.of(ByteBuffer.allocate(4).putInt(0, id));
        });
        server.start();
        try (KafkaTestClient first = new KafkaTestClient(server.boundPort());
             KafkaTestClient second = new KafkaTestClient(server.boundPort())) {
            long startNanos = System.nanoTime();
            first.send(ByteBuffer.allocate(4).putInt(0, 1));
            second.send(ByteBuffer.allocate(4).putInt(0, 2));

            assertThat(first.receive().getInt()).isEqualTo(1);
            assertThat(second.receive().getInt()).isEqualTo(2);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertThat(sawOtherConnection).containsExactly(true, true);
            assertThat(ranOnVirtualThread).containsOnly(true);
            // Two 500 ms requests in parallel take ~500 ms; one after another would take 5 s or more.
            assertThat(elapsedMillis).isLessThan(3_000);
        } finally {
            server.stop();
        }
    }

    @Test
    void earlierResponsesAreSentBeforeAFailedRequestClosesTheConnection() throws Exception {
        List<Integer> processed = new CopyOnWriteArrayList<>();
        KafkaServer server = new KafkaServer(CONFIG, frame -> {
            int id = frame.getInt();
            processed.add(id);
            if (id == 2) {
                throw new IllegalArgumentException("bad request " + id);
            }
            sleep(id == 0 ? 200 : 0);
            return Optional.of(ByteBuffer.allocate(4).putInt(0, id));
        });
        server.start();
        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            for (int id = 0; id < 4; id++) {
                client.send(ByteBuffer.allocate(4).putInt(0, id));
            }
            assertThat(client.receive().getInt()).isEqualTo(0);
            assertThat(client.receive().getInt()).isEqualTo(1);
            assertThat(client.readByteOrEof()).isEqualTo(-1);
            // Request 3 arrived after the bad one, so it must never be processed.
            assertThat(processed).containsExactly(0, 1, 2);
        } finally {
            server.stop();
        }
    }

    @Test
    void anEmptyResponseWritesNoFrameAndKeepsOrder() throws Exception {
        KafkaServer server = new KafkaServer(CONFIG, frame -> {
            int id = frame.getInt();
            if (id == 1) {
                return Optional.empty();
            }
            return Optional.of(ByteBuffer.allocate(4).putInt(0, id));
        });
        server.start();
        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            for (int id = 0; id < 3; id++) {
                client.send(ByteBuffer.allocate(4).putInt(0, id));
            }
            assertThat(client.receive().getInt()).isEqualTo(0);
            assertThat(client.receive().getInt()).isEqualTo(2);
        } finally {
            server.stop();
        }
    }

    @Test
    void manyPipelinedApiVersionsRequestsAreAnsweredInOrder() throws Exception {
        int requests = 200; // well above MAX_IN_FLIGHT_PER_CONNECTION, so reading pauses and resumes
        KafkaServer server = new KafkaServer(CONFIG);
        server.start();
        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            List<RequestHeader> headers = new ArrayList<>();
            for (int correlationId = 1000; correlationId < 1000 + requests; correlationId++) {
                RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "pipeline", correlationId);
                headers.add(header);
                client.send(new ApiVersionsRequest.Builder((short) 3).build((short) 3).serializeWithHeader(header));
            }
            for (RequestHeader header : headers) {
                // parseResponse throws CorrelationIdMismatchException if a response is out of order.
                ApiVersionsResponse response =
                    (ApiVersionsResponse) AbstractResponse.parseResponse(client.receive(), header);
                assertThat(response.data().errorCode()).isEqualTo(Errors.NONE.code());
            }
        } finally {
            server.stop();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
