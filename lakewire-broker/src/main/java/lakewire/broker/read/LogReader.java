package lakewire.broker.read;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lakewire.coordinator.ClusterView;
import lakewire.coordinator.StoredTopic;
import lakewire.storage.ObjectNotFoundException;
import lakewire.storage.lkw1.BatchOffsets;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads a partition up to the high watermark. Record batches keep their bytes; only base offsets are patched. */
public final class LogReader {

    private static final Logger log = LoggerFactory.getLogger(LogReader.class);

    private final ClusterView topics;
    private final HwCache watermarks;
    private final SegmentIndex segments;
    private final BlockCache blocks;

    public LogReader(ClusterView topics, HwCache watermarks, SegmentIndex segments, BlockCache blocks) {
        this.topics = topics;
        this.watermarks = watermarks;
        this.segments = segments;
        this.blocks = blocks;
    }

    public long generation() {
        return watermarks.generation();
    }

    public void await(long observedGeneration, int maxWaitMs) {
        long deadline = System.nanoTime() + Math.max(maxWaitMs, 0) * 1_000_000L;
        watermarks.await(observedGeneration, deadline);
    }

    public PartitionRead fetch(String topic, int partition, long offset, int maxBytes) {
        StoredTopic stored = find(topic);
        if (stored == null || partition < 0 || partition >= stored.partitions()) {
            return PartitionRead.unknown();
        }
        Watermark mark = watermarks.refresh(stored.id(), partition);
        if (offset < mark.logStartOffset() || offset > mark.highWatermark()) {
            return PartitionRead.outOfRange(mark);
        }
        if (offset == mark.highWatermark()) {
            return PartitionRead.empty(mark);
        }
        try {
            MemoryRecords records = readRecords(stored.id(), partition, offset, mark.highWatermark(), maxBytes, true);
            return PartitionRead.ok(mark, records);
        } catch (ObjectNotFoundException missing) {
            Watermark fresh = watermarks.refresh(stored.id(), partition);
            if (offset < fresh.logStartOffset()) {
                return PartitionRead.outOfRange(fresh);
            }
            log.error("I7 object missing after reload topic={} partition={} offset={}", topic, partition, offset);
            return PartitionRead.storage(fresh);
        }
    }

    public OffsetBound list(String topic, int partition, long timestamp) {
        StoredTopic stored = find(topic);
        if (stored == null || partition < 0 || partition >= stored.partitions()) {
            return OffsetBound.error(Errors.UNKNOWN_TOPIC_OR_PARTITION);
        }
        Watermark mark = watermarks.refresh(stored.id(), partition);
        if (timestamp == -2L) {
            return OffsetBound.found(mark.logStartOffset(), -1L);
        }
        if (timestamp == -1L) {
            return OffsetBound.found(mark.highWatermark(), -1L);
        }
        if (timestamp < 0) {
            return OffsetBound.error(Errors.INVALID_REQUEST);
        }
        for (StoredSegment segment : segments.all(stored.id(), partition)) {
            if (segment.maxTimestamp() < timestamp || segment.baseOffset() >= mark.highWatermark()) {
                continue;
            }
            byte[] patched = BatchOffsets.patch(load(stored.id(), partition, segment), segment.baseOffset());
            for (BatchSlice batch : batches(patched)) {
                if (batch.maxTimestamp() >= timestamp && batch.nextOffset() - 1 < mark.highWatermark()) {
                    return OffsetBound.found(batch.baseOffset(), batch.maxTimestamp());
                }
            }
        }
        return OffsetBound.found(-1L, -1L);
    }

    private MemoryRecords readRecords(
        long topicId,
        int partition,
        long offset,
        long highWatermark,
        int maxBytes,
        boolean allowReload
    ) {
        try {
            return assemble(topicId, partition, offset, highWatermark, maxBytes);
        } catch (ObjectNotFoundException missing) {
            if (!allowReload) {
                throw missing;
            }
            segments.invalidate(topicId, partition);
            return readRecords(topicId, partition, offset, highWatermark, maxBytes, false);
        }
    }

    private MemoryRecords assemble(long topicId, int partition, long offset, long highWatermark, int maxBytes) {
        List<BatchSlice> chosen = new ArrayList<>();
        int bytes = 0;
        boolean first = true;
        for (StoredSegment segment : segments.covering(topicId, partition, offset, Math.max(maxBytes, 1))) {
            if (segment.baseOffset() >= highWatermark) {
                break;
            }
            byte[] patched = BatchOffsets.patch(load(topicId, partition, segment), segment.baseOffset());
            for (BatchSlice batch : batches(patched)) {
                if (batch.nextOffset() <= offset || batch.nextOffset() - 1 >= highWatermark) {
                    continue;
                }
                if (!first && bytes + batch.size() > maxBytes) {
                    return join(chosen);
                }
                chosen.add(batch);
                bytes += batch.size();
                first = false;
                if (bytes >= maxBytes) {
                    return join(chosen);
                }
            }
        }
        return join(chosen);
    }

    private byte[] load(long topicId, int partition, StoredSegment segment) {
        try {
            return blocks.get(segment.objectKey(), segment.byteStart(), segment.byteLen());
        } catch (ObjectNotFoundException missing) {
            blocks.invalidate(segment.objectKey(), segment.byteStart(), segment.byteLen());
            segments.invalidate(topicId, partition);
            throw missing;
        }
    }

    private static MemoryRecords join(List<BatchSlice> batches) {
        if (batches.isEmpty()) {
            return MemoryRecords.EMPTY;
        }
        int size = 0;
        for (BatchSlice batch : batches) {
            size += batch.size();
        }
        byte[] out = new byte[size];
        int cursor = 0;
        for (BatchSlice batch : batches) {
            System.arraycopy(batch.bytes(), batch.position(), out, cursor, batch.size());
            cursor += batch.size();
        }
        return MemoryRecords.readableRecords(ByteBuffer.wrap(out));
    }

    static List<BatchSlice> batches(byte[] patched) {
        ByteBuffer buffer = ByteBuffer.wrap(patched);
        List<BatchSlice> spans = new ArrayList<>();
        int position = 0;
        while (position < patched.length) {
            if (position + 12 > patched.length) {
                throw new IllegalArgumentException("truncated batch");
            }
            int batchLength = buffer.getInt(position + 8);
            int size = batchLength + 12;
            if (batchLength < 0 || position + size > patched.length) {
                throw new IllegalArgumentException("truncated batch");
            }
            MutableRecordBatch batch = MemoryRecords.readableRecords(
                ByteBuffer.wrap(patched, position, size).duplicate()
            ).batches().iterator().next();
            spans.add(new BatchSlice(
                patched,
                position,
                size,
                batch.baseOffset(),
                batch.nextOffset(),
                batch.maxTimestamp()
            ));
            position += size;
        }
        return spans;
    }

    private StoredTopic find(String name) {
        for (StoredTopic topic : topics.topics()) {
            if (topic.name().equals(name)) {
                return topic;
            }
        }
        return null;
    }

    private record BatchSlice(byte[] bytes, int position, int size, long baseOffset, long nextOffset, long maxTimestamp) {
    }
}
