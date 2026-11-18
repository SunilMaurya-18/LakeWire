package lakewire.broker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import lakewire.broker.write.WriteSettings;
import lakewire.storage.LocalFsObjectStore;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Acks0IT {

    @Test
    void acksZeroWritesNoResponseFrame() throws Exception {
        java.nio.file.Path data = Files.createTempDirectory("lakewire-acks0");
        try (ProduceCluster cluster = ProduceCluster.open(
            new LocalFsObjectStore(data),
            new WriteSettings(20, WriteSettings.DEFAULT_OBJECT_MAX_BYTES, WriteSettings.DEFAULT_BUFFER_MAX_BYTES, 4, "")
        )) {
            cluster.createTopic("best", 1);
            try (Socket socket = new Socket("127.0.0.1", cluster.port())) {
                socket.setSoTimeout(5_000);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream());
                send(out, produce());
                send(out, apiVersions());
                byte[] response = readFrame(in);
                assertThat(ByteBuffer.wrap(response).getInt()).isEqualTo(2);
                socket.setSoTimeout(400);
                assertThatThrownBy(in::read).isInstanceOf(SocketTimeoutException.class);
            }
            Awaitility.await().atMost(ProduceCluster.commitWait()).until(() -> cluster.nextOffset("best", 0) == 1);
        } finally {
            try (var paths = Files.walk(data)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception ignored) {
                        // closed with the broker
                    }
                });
            }
        }
    }

    private static ByteBuffer produce() {
        var builder = MemoryRecords.builder(ByteBuffer.allocate(256), Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(0L, new byte[] {1}, new byte[] {2});
        MemoryRecords records = builder.build();
        TopicProduceData topic = new TopicProduceData()
            .setName("best")
            .setPartitionData(java.util.List.of(new PartitionProduceData().setIndex(0).setRecords(records)));
        ProduceRequestData data = new ProduceRequestData()
            .setAcks((short) 0)
            .setTimeoutMs(5_000)
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(java.util.List.of(topic).iterator()));
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 9, "acks0", 1);
        return new ProduceRequest(data, (short) 9).serializeWithHeader(header);
    }

    private static ByteBuffer apiVersions() {
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "acks0", 2);
        return new ApiVersionsRequest.Builder((short) 0).build((short) 0).serializeWithHeader(header);
    }

    private static void send(DataOutputStream out, ByteBuffer frame) throws Exception {
        byte[] bytes = new byte[frame.remaining()];
        frame.get(bytes);
        out.writeInt(bytes.length);
        out.write(bytes);
        out.flush();
    }

    private static byte[] readFrame(DataInputStream in) throws Exception {
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        return bytes;
    }
}
