package lakewire.broker;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import lakewire.broker.network.KafkaTestClient;
import lakewire.storage.LocalFsObjectStore;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DuplicateRetryIT {

    @Test
    void resendingTheSameProduceFrameReturnsTheOriginalOffset() throws Exception {
        Path data = Files.createTempDirectory("lakewire-dup");
        try (ProduceCluster cluster = ProduceCluster.open(new LocalFsObjectStore(data), ProduceCluster.fast())) {
            cluster.createTopic("retry", 1);
            try (KafkaTestClient client = new KafkaTestClient(cluster.port())) {
                InitProducerIdResponseData allocated = init(client);
                assertThat(allocated.errorCode()).isEqualTo(Errors.NONE.code());
                RequestHeader header = produceHeader(7);
                byte[] frame = produceFrame(allocated.producerId(), allocated.producerEpoch(), header);
                long first = baseOffset(send(client, frame, header));
                long second = baseOffset(send(client, frame, header));
                assertThat(first).isZero();
                assertThat(second).isEqualTo(first);
            }
            try (Connection connection = DriverManager.getConnection(cluster.jdbcUrl(), cluster.user(), cluster.password());
                 Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery("""
                     SELECT count(*)
                     FROM segments s
                     JOIN topics t ON t.id = s.topic_id
                     WHERE t.name = 'retry'
                     """)) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getLong(1)).isEqualTo(1);
            }
        } finally {
            delete(data);
        }
    }

    private static InitProducerIdResponseData init(KafkaTestClient client) throws Exception {
        RequestHeader header = new RequestHeader(ApiKeys.INIT_PRODUCER_ID, (short) 4, "dup", 1);
        InitProducerIdRequest request = new InitProducerIdRequest.Builder(
            new InitProducerIdRequestData()
                .setTransactionalId(null)
                .setTransactionTimeoutMs(60_000)
                .setProducerId(-1)
                .setProducerEpoch((short) -1)
        ).build(header.apiVersion());
        InitProducerIdResponse response = (InitProducerIdResponse) send(client, frame(request, header), header);
        return response.data();
    }

    private static byte[] produceFrame(long producerId, short epoch, RequestHeader header) {
        MemoryRecords records = MemoryRecords.withIdempotentRecords(
            Compression.NONE,
            producerId,
            epoch,
            0,
            new SimpleRecord(1L, "k".getBytes(java.nio.charset.StandardCharsets.UTF_8), "v".getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );
        TopicProduceData topic = new TopicProduceData()
            .setName("retry")
            .setPartitionData(java.util.List.of(new PartitionProduceData().setIndex(0).setRecords(records)));
        ProduceRequestData data = new ProduceRequestData()
            .setAcks((short) -1)
            .setTimeoutMs(5_000)
            .setTopicData(new ProduceRequestData.TopicProduceDataCollection(java.util.List.of(topic).iterator()));
        ProduceRequest request = new ProduceRequest.Builder((short) 3, (short) 12, data).build(header.apiVersion());
        return frame(request, header);
    }

    private static RequestHeader produceHeader(int correlation) {
        return new RequestHeader(ApiKeys.PRODUCE, (short) 9, "dup", correlation);
    }

    private static long baseOffset(AbstractResponse response) {
        ProduceResponse produced = (ProduceResponse) response;
        for (TopicProduceResponse topic : produced.data().responses()) {
            if ("retry".equals(topic.name())) {
                assertThat(topic.partitionResponses().get(0).errorCode()).isEqualTo(Errors.NONE.code());
                return topic.partitionResponses().get(0).baseOffset();
            }
        }
        throw new AssertionError("missing retry topic");
    }

    private static AbstractResponse send(KafkaTestClient client, byte[] bytes, RequestHeader header) throws Exception {
        client.send(ByteBuffer.wrap(bytes));
        return AbstractResponse.parseResponse(client.receive(), header);
    }

    private static byte[] frame(AbstractRequest request, RequestHeader header) {
        ByteBuffer buffer = request.serializeWithHeader(header);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static void delete(Path root) throws Exception {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            });
        }
    }
}
