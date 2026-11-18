package lakewire.broker.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lakewire.broker.write.Append;
import lakewire.broker.write.ProduceAck;
import lakewire.broker.write.ProduceSink;
import lakewire.coordinator.ClusterView;
import lakewire.coordinator.CoordinatorException;
import lakewire.coordinator.LiveBroker;
import lakewire.coordinator.StoredTopic;
import lakewire.coordinator.TopicMutation;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProduceHandlerTest {

    @Test
    void unknownTopicAndPartitionAreRejected() {
        FakeSink sink = new FakeSink();
        ProduceResponseData unknownTopic = send(cluster(), sink, request("missing", 0, oneRecord(), (short) -1));
        assertThat(error(unknownTopic, "missing")).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        assertThat(sink.appended).isEmpty();

        ProduceResponseData unknownPart = send(cluster(), sink, request("orders", 4, oneRecord(), (short) -1));
        assertThat(error(unknownPart)).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    @Test
    void aBatchThatIsNotOneValidMagicTwoBatchIsInvalid() {
        FakeSink sink = new FakeSink();
        assertThat(error(send(cluster(), sink, request("orders", 0, MemoryRecords.EMPTY, (short) -1))))
            .isEqualTo(Errors.INVALID_RECORD.code());
        assertThat(error(send(cluster(), sink, request("orders", 0, magicOne(), (short) -1))))
            .isEqualTo(Errors.INVALID_RECORD.code());
        assertThat(error(send(cluster(), sink, request("orders", 0, twoBatches(), (short) -1))))
            .isEqualTo(Errors.INVALID_RECORD.code());
        assertThat(error(send(cluster(), sink, request("orders", 0, corrupt(oneRecord()), (short) -1))))
            .isEqualTo(Errors.INVALID_RECORD.code());
        assertThat(error(send(cluster(), sink, request("orders", 0, transactional(), (short) -1))))
            .isEqualTo(Errors.INVALID_RECORD.code());
        assertThat(sink.appended).isEmpty();
    }

    @Test
    void anOversizedBatchIsMessageTooLarge() {
        FakeSink sink = new FakeSink();
        sink.maxBytes = 8;
        assertThat(error(send(cluster(), sink, request("orders", 0, oneRecord(), (short) -1))))
            .isEqualTo(Errors.MESSAGE_TOO_LARGE.code());
    }

    @Test
    void acksOtherThanZeroOneOrAllAreRejected() {
        ProduceResponseData response = send(cluster(), new FakeSink(), request("orders", 0, oneRecord(), (short) 2));
        assertThat(error(response)).isEqualTo(Errors.INVALID_REQUIRED_ACKS.code());
        assertThat(response.responses().iterator().next().partitionResponses().get(0).baseOffset()).isEqualTo(-1);
    }

    @Test
    void aTransactionalIdIsAnInvalidRequest() {
        ProduceRequestData data = request("orders", 0, oneRecord(), (short) -1);
        data.setTransactionalId("tx-1");
        assertThat(error(send(cluster(), new FakeSink(), data))).isEqualTo(Errors.INVALID_REQUEST.code());
    }

    @Test
    void aFencedBrokerRejectsTheProduce() {
        FakeSink sink = new FakeSink();
        sink.accepting = false;
        assertThat(error(send(cluster(), sink, request("orders", 0, oneRecord(), (short) 1))))
            .isEqualTo(Errors.NOT_LEADER_OR_FOLLOWER.code());
        assertThat(sink.appended).isEmpty();
    }

    @Test
    void aSlowAppendTimesOut() {
        FakeSink sink = new FakeSink();
        sink.next = new CompletableFuture<>();
        ProduceRequestData data = request("orders", 0, oneRecord(), (short) -1);
        data.setTimeoutMs(30);
        ProduceResponseData response = send(cluster(), sink, data);
        assertThat(error(response)).isEqualTo(Errors.REQUEST_TIMED_OUT.code());
        assertThat(partition(response, "orders").logAppendTimeMs()).isEqualTo(-1);
    }

    @Test
    void acksAllAndAcksOneWaitForTheCommit() {
        for (short acks : new short[] {-1, 1}) {
            FakeSink sink = new FakeSink();
            sink.next = CompletableFuture.completedFuture(ProduceAck.ok(12, 4));
            ProduceResponseData response = send(cluster(), sink, request("orders", 0, oneRecord(), acks));
            var partition = partition(response, "orders");
            assertThat(partition.errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(partition.baseOffset()).isEqualTo(12);
            assertThat(partition.logStartOffset()).isEqualTo(4);
            assertThat(partition.logAppendTimeMs()).isEqualTo(-1);
            assertThat(sink.appended).hasSize(1);
            assertThat(sink.appended.get(0).partition()).isZero();
            assertThat(sink.appended.get(0).nRecords()).isEqualTo(1);
        }
    }

    @Test
    void acksZeroEnqueuesAndWritesNoFrame() {
        FakeSink sink = new FakeSink();
        ProduceRequestData data = request("orders", 0, oneRecord(), (short) 0);
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 9, "unit", 1);
        ProduceRequest request = new ProduceRequest(data, (short) 9);
        Optional<ByteBuffer> raw = new RequestRouter(cluster(), 1, sink).process(request.serializeWithHeader(header));
        assertThat(raw).isEmpty();
        assertThat(sink.appended).hasSize(1);
    }

    private static ProduceResponseData send(ClusterView topics, ProduceSink sink, ProduceRequestData data) {
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 9, "unit", 1);
        ProduceRequest request = new ProduceRequest(data, (short) 9);
        ByteBuffer raw = new RequestRouter(topics, 1, sink).process(request.serializeWithHeader(header)).orElseThrow();
        ProduceResponse parsed = (ProduceResponse) AbstractResponse.parseResponse(raw, header);
        return parsed.data();
    }

    private static short error(ProduceResponseData data) {
        return error(data, "orders");
    }

    private static short error(ProduceResponseData data, String topic) {
        return partition(data, topic).errorCode();
    }

    private static PartitionProduceResponse partition(ProduceResponseData data, String topic) {
        for (TopicProduceResponse response : data.responses()) {
            if (topic.equals(response.name())) {
                return response.partitionResponses().get(0);
            }
        }
        throw new AssertionError("missing topic " + topic);
    }

    private static ProduceRequestData request(String topic, int partition, MemoryRecords records, short acks) {
        TopicProduceData topicData = new TopicProduceData()
            .setName(topic)
            .setPartitionData(List.of(new PartitionProduceData().setIndex(partition).setRecords(records)));
        return new ProduceRequestData()
            .setAcks(acks)
            .setTimeoutMs(1_000)
            .setTopicData(new org.apache.kafka.common.message.ProduceRequestData.TopicProduceDataCollection(List.of(topicData).iterator()));
    }

    private static MemoryRecords oneRecord() {
        var builder = MemoryRecords.builder(ByteBuffer.allocate(256), Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(0L, new byte[] {1}, new byte[] {2});
        return builder.build();
    }

    private static MemoryRecords magicOne() {
        var builder = MemoryRecords.builder(
            ByteBuffer.allocate(256), (byte) 1, Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(0L, new byte[] {1}, new byte[] {2});
        return builder.build();
    }

    private static MemoryRecords twoBatches() {
        byte[] first = bytes(oneRecord());
        byte[] second = bytes(oneRecord());
        ByteBuffer joined = ByteBuffer.allocate(first.length + second.length);
        joined.put(first).put(second).flip();
        return MemoryRecords.readableRecords(joined);
    }

    private static MemoryRecords corrupt(MemoryRecords records) {
        byte[] raw = bytes(records);
        raw[raw.length / 2] ^= 0x5A;
        return MemoryRecords.readableRecords(ByteBuffer.wrap(raw));
    }

    private static MemoryRecords transactional() {
        return MemoryRecords.withTransactionalRecords(
            Compression.NONE,
            1L,
            (short) 0,
            0,
            new SimpleRecord(0L, new byte[] {1}, new byte[] {2})
        );
    }

    private static byte[] bytes(MemoryRecords records) {
        ByteBuffer view = records.buffer().duplicate();
        byte[] raw = new byte[records.sizeInBytes()];
        view.get(raw);
        return raw;
    }

    private static ClusterView cluster() {
        return new ClusterView() {
            @Override
            public List<LiveBroker> liveBrokers() {
                return List.of();
            }

            @Override
            public String clusterId() {
                return "abc";
            }

            @Override
            public List<StoredTopic> topics() {
                return List.of(new StoredTopic(7, "orders", 2, 1));
            }

            @Override
            public TopicMutation createTopic(String name, int partitions, long retentionMs, boolean validateOnly) {
                throw new CoordinatorException("unused");
            }

            @Override
            public short deleteTopic(String name) {
                throw new CoordinatorException("unused");
            }
        };
    }

    private static final class FakeSink implements ProduceSink {
        private boolean accepting = true;
        private int maxBytes = 1024 * 1024;
        private final List<Append> appended = new ArrayList<>();
        private CompletableFuture<ProduceAck> next = CompletableFuture.completedFuture(ProduceAck.ok(0, 0));

        @Override
        public boolean accepting() {
            return accepting;
        }

        @Override
        public int objectMaxBytes() {
            return maxBytes;
        }

        @Override
        public CompletableFuture<ProduceAck> append(Append append, long waitMs) {
            appended.add(append);
            return next;
        }
    }
}
