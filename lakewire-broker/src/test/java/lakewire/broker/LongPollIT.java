package lakewire.broker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import lakewire.storage.LocalFsObjectStore;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestData.FetchPartition;
import org.apache.kafka.common.message.FetchRequestData.FetchTopic;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LongPollIT {

    @Test
    void anEmptyFetchWaitsOutMaxWaitAndAProduceWakesTheNextOne() throws Exception {
        java.nio.file.Path data = Files.createTempDirectory("lakewire-poll");
        try (ProduceCluster cluster = ProduceCluster.open(new LocalFsObjectStore(data), ProduceCluster.fast())) {
            cluster.createTopic("wait", 1);
            long idleStarted = System.nanoTime();
            PartitionData idle = fetch(cluster.port(), 0, 700, 1);
            long idleMs = elapsedMs(idleStarted);
            assertThat(idle.errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(idle.highWatermark()).isZero();
            assertThat(idle.records().sizeInBytes()).isZero();
            assertThat(idleMs).isBetween(500L, 2_000L);

            Thread producer = Thread.ofPlatform().start(() -> {
                try {
                    Thread.sleep(150);
                    cluster.produce("wait", "all", 1, 1);
                } catch (Exception failure) {
                    throw new IllegalStateException(failure);
                }
            });
            long liveStarted = System.nanoTime();
            PartitionData live = fetch(cluster.port(), 0, 5_000, 1);
            long liveMs = elapsedMs(liveStarted);
            producer.join(10_000);
            assertThat(live.errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(live.highWatermark()).isEqualTo(1);
            assertThat(live.records().sizeInBytes()).isGreaterThan(0);
            assertThat(liveMs).isLessThan(2_500L);
        } finally {
            delete(data);
        }
    }

    private static PartitionData fetch(int port, long offset, int maxWaitMs, int minBytes) throws Exception {
        FetchRequestData data = new FetchRequestData()
            .setMaxWaitMs(maxWaitMs)
            .setMinBytes(minBytes)
            .setMaxBytes(1_000_000)
            .setTopics(java.util.List.of(new FetchTopic()
                .setTopic("wait")
                .setPartitions(java.util.List.of(new FetchPartition()
                    .setPartition(0)
                    .setFetchOffset(offset)
                    .setPartitionMaxBytes(1_000_000)))));
        RequestHeader header = new RequestHeader(ApiKeys.FETCH, (short) 11, "poll", 1);
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(maxWaitMs + 5_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            ByteBuffer frame = new FetchRequest(data, header.apiVersion()).serializeWithHeader(header);
            byte[] bytes = new byte[frame.remaining()];
            frame.get(bytes);
            out.writeInt(bytes.length);
            out.write(bytes);
            out.flush();
            DataInputStream in = new DataInputStream(socket.getInputStream());
            byte[] response = new byte[in.readInt()];
            in.readFully(response);
            FetchResponse parsed = (FetchResponse) AbstractResponse.parseResponse(ByteBuffer.wrap(response), header);
            return parsed.data().responses().iterator().next().partitions().get(0);
        }
    }

    private static long elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000L;
    }

    private static void delete(java.nio.file.Path data) throws Exception {
        try (var paths = Files.walk(data)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                    // the broker already closed the store
                }
            });
        }
    }
}
