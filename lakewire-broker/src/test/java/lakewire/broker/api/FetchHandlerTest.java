package lakewire.broker.api;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lakewire.broker.read.BlockCache;
import lakewire.broker.read.HwCache;
import lakewire.broker.read.LogReader;
import lakewire.broker.read.SegmentIndex;
import lakewire.broker.read.StoredSegment;
import lakewire.broker.read.Watermark;
import lakewire.coordinator.ClusterView;
import lakewire.coordinator.CoordinatorException;
import lakewire.coordinator.LiveBroker;
import lakewire.coordinator.StoredTopic;
import lakewire.coordinator.TopicMutation;
import lakewire.storage.ObjectHead;
import lakewire.storage.ObjectStore;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestData.FetchPartition;
import org.apache.kafka.common.message.FetchRequestData.FetchTopic;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FetchHandlerTest {

    @Test
    void offsetAtTheCachedWatermarkRereadsPostgresBeforeReturning() {
        Fixture fixture = fixture(new Watermark(0, 0), new Watermark(2, 0), chunk(one("a"), one("b")));
        PartitionData atCached = fetch(fixture, 0, 1_000_000);
        assertThat(atCached.errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(atCached.highWatermark()).isEqualTo(2);
        assertThat(batches(atCached)).isEqualTo(2);
    }

    @Test
    void anOffsetAboveTheWatermarkIsOutOfRangeAndTheWatermarkItselfIsEmpty() {
        Fixture fixture = fixture(new Watermark(2, 0), new Watermark(2, 0), chunk(one("a"), one("b")));
        PartitionData empty = fetch(fixture, 2, 1_000_000);
        assertThat(empty.errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(empty.records().sizeInBytes()).isZero();
        assertThat(empty.lastStableOffset()).isEqualTo(2);
        assertThat(empty.preferredReadReplica()).isEqualTo(-1);

        PartitionData above = fetch(fixture, 3, 1_000_000);
        assertThat(above.errorCode()).isEqualTo(Errors.OFFSET_OUT_OF_RANGE.code());
    }

    @Test
    void anOffsetBelowLogStartIsOutOfRange() {
        Fixture fixture = fixture(new Watermark(4, 2), new Watermark(4, 2), chunk(one("a")));
        assertThat(fetch(fixture, 1, 1_000_000).errorCode()).isEqualTo(Errors.OFFSET_OUT_OF_RANGE.code());
    }

    @Test
    void maxBytesKeepsTheFirstBatchEvenWhenItDoesNotFit() {
        byte[] first = one("aaaa");
        byte[] second = one("b");
        Fixture fixture = fixture(new Watermark(2, 0), new Watermark(2, 0), chunk(first, second));
        PartitionData onlyFirst = fetch(fixture, 0, first.length);
        assertThat(batches(onlyFirst)).isEqualTo(1);
        assertThat(onlyFirst.records().sizeInBytes()).isEqualTo(first.length);

        PartitionData oversized = fetch(fixture, 0, 1);
        assertThat(batches(oversized)).isEqualTo(1);
        assertThat(oversized.records().sizeInBytes()).isEqualTo(first.length);
    }

    private static PartitionData fetch(Fixture fixture, long offset, int maxBytes) {
        FetchRequestData data = new FetchRequestData()
            .setMaxWaitMs(0)
            .setMinBytes(1)
            .setMaxBytes(maxBytes)
            .setTopics(List.of(new FetchTopic()
                .setTopic("orders")
                .setPartitions(List.of(new FetchPartition()
                    .setPartition(0)
                    .setFetchOffset(offset)
                    .setPartitionMaxBytes(maxBytes)))));
        RequestHeader header = new RequestHeader(ApiKeys.FETCH, (short) 4, "unit", 1);
        FetchRequest request = new FetchRequest(data, header.apiVersion());
        ByteBuffer raw = new RequestRouter(fixture.cluster(), null, fixture.reader())
            .process(request.serializeWithHeader(header))
            .orElseThrow();
        FetchResponse parsed = (FetchResponse) AbstractResponse.parseResponse(raw, header);
        return parsed.data().responses().iterator().next().partitions().get(0);
    }

    private static int batches(PartitionData data) {
        int count = 0;
        for (var ignored : ((MemoryRecords) data.records()).batches()) {
            count++;
        }
        return count;
    }

    private static Fixture fixture(Watermark cached, Watermark fresh, byte[] chunk) {
        MapStore store = new MapStore();
        store.objects.put("obj", chunk);
        HwCache watermarks = new HwCache((topic, part) -> fresh, null);
        watermarks.put(7, 0, cached);
        SegmentIndex index = new SegmentIndex((topic, part) -> List.of());
        index.remember(7, 0, List.of(new StoredSegment(0, 1, 0, chunk.length, 10, "obj")));
        LogReader reader = new LogReader(cluster(), watermarks, index, new BlockCache(store, 1024 * 1024));
        return new Fixture(cluster(), reader);
    }

    private static byte[] one(String value) {
        var builder = MemoryRecords.builder(ByteBuffer.allocate(256), Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, new byte[] {1}, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        MemoryRecords records = builder.build();
        ByteBuffer view = records.buffer().duplicate();
        byte[] raw = new byte[records.sizeInBytes()];
        view.get(raw);
        return raw;
    }

    private static byte[] chunk(byte[]... batches) {
        int size = 0;
        for (byte[] batch : batches) {
            size += batch.length;
        }
        byte[] joined = new byte[size];
        int cursor = 0;
        for (byte[] batch : batches) {
            System.arraycopy(batch, 0, joined, cursor, batch.length);
            cursor += batch.length;
        }
        return joined;
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
                return List.of(new StoredTopic(7, "orders", 1, 1));
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

    private record Fixture(ClusterView cluster, LogReader reader) {
    }

    private static final class MapStore implements ObjectStore {
        private final Map<String, byte[]> objects = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<Void> put(String key, byte[] body) {
            objects.put(key, body.clone());
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<byte[]> getRange(String key, long start, int length) {
            byte[] body = objects.get(key);
            if (body == null) {
                return CompletableFuture.failedFuture(new lakewire.storage.ObjectNotFoundException(key));
            }
            return CompletableFuture.completedFuture(java.util.Arrays.copyOfRange(body, (int) start, (int) start + length));
        }

        @Override
        public CompletableFuture<ObjectHead> head(String key) {
            byte[] body = objects.get(key);
            return body == null
                ? CompletableFuture.failedFuture(new lakewire.storage.ObjectNotFoundException(key))
                : CompletableFuture.completedFuture(new ObjectHead(body.length, -1));
        }

        @Override
        public CompletableFuture<Void> delete(String key) {
            objects.remove(key);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<List<String>> list(String prefix) {
            return CompletableFuture.completedFuture(List.copyOf(objects.keySet()));
        }

        @Override
        public void close() {
        }
    }
}
