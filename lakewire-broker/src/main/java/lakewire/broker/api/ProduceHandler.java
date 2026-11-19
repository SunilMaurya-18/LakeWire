package lakewire.broker.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lakewire.broker.metrics.LakeWireMetrics;
import lakewire.broker.write.Append;
import lakewire.broker.write.ProduceAck;
import lakewire.broker.write.ProduceSink;
import lakewire.coordinator.ClusterView;
import lakewire.coordinator.CoordinatorException;
import lakewire.coordinator.StoredTopic;
import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponseCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Produce. acks=1 and acks=all both wait for the object PUT and the commit. */
public final class ProduceHandler {

    private static final Logger log = LoggerFactory.getLogger(ProduceHandler.class);

    private ProduceHandler() {
    }

    public static Optional<ByteBuffer> handle(KafkaRequest request, ClusterView topics, ProduceSink pipeline) {
        return handle(request, topics, pipeline, LakeWireMetrics.noop());
    }

    public static Optional<ByteBuffer> handle(
        KafkaRequest request,
        ClusterView topics,
        ProduceSink pipeline,
        LakeWireMetrics metrics
    ) {
        long started = System.nanoTime();
        try {
            return handleInner(request, topics, pipeline);
        } finally {
            metrics.produceNanos(System.nanoTime() - started);
        }
    }

    private static Optional<ByteBuffer> handleInner(
        KafkaRequest request,
        ClusterView topics,
        ProduceSink pipeline
    ) {
        ProduceRequestData data = ((ProduceRequest) request.body()).data();
        short acks = data.acks();
        if (acks != 0 && acks != 1 && acks != -1) {
            return Optional.of(respond(request, errorResponse(data, Errors.INVALID_REQUIRED_ACKS)));
        }
        if (data.transactionalId() != null) {
            return acks == 0
                ? Optional.empty()
                : Optional.of(respond(request, errorResponse(data, Errors.INVALID_REQUEST)));
        }
        if (topics == null || pipeline == null) {
            return acks == 0 ? Optional.empty() : Optional.of(RequestRouter.coordinatorUnavailable(request));
        }
        if (!pipeline.accepting()) {
            return acks == 0
                ? Optional.empty()
                : Optional.of(respond(request, errorResponse(data, Errors.NOT_LEADER_OR_FOLLOWER)));
        }
        try {
            List<Slot> slots = new ArrayList<>();
            TopicProduceResponseCollection responses = new TopicProduceResponseCollection();
            List<StoredTopic> stored = topics.topics();
            int objectMax = pipeline.objectMaxBytes();
            for (TopicProduceData topicData : data.topicData()) {
                StoredTopic topic = find(stored, topicData.name());
                TopicProduceResponse topicResponse = new TopicProduceResponse().setName(topicData.name());
                List<PartitionProduceResponse> partitions = new ArrayList<>();
                for (PartitionProduceData partition : topicData.partitionData()) {
                    PartitionProduceResponse partitionResponse = new PartitionProduceResponse()
                        .setIndex(partition.index())
                        .setLogAppendTimeMs(-1);
                    Checked checked = check(topic, partition, objectMax);
                    if (checked.error != null) {
                        partitions.add(apply(partitionResponse, checked.error));
                    } else {
                        CompletableFuture<ProduceAck> future = pipeline.append(checked.append, data.timeoutMs());
                        if (acks == 0) {
                            partitions.add(partitionResponse.setErrorCode(Errors.NONE.code()).setBaseOffset(-1).setLogStartOffset(-1));
                        } else {
                            slots.add(new Slot(partitionResponse, future));
                            partitions.add(partitionResponse);
                        }
                    }
                }
                topicResponse.setPartitionResponses(partitions);
                responses.add(topicResponse);
            }
            if (acks == 0) {
                return Optional.empty();
            }
            await(slots, data.timeoutMs());
            return Optional.of(respond(request, new ProduceResponseData().setResponses(responses)));
        } catch (CoordinatorException failure) {
            log.warn("produce failed: {}", failure.toString());
            return acks == 0 ? Optional.empty() : Optional.of(RequestRouter.coordinatorUnavailable(request));
        }
    }

    private static void await(List<Slot> slots, int timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(timeoutMs, 0));
        for (Slot slot : slots) {
            long remaining = deadline - System.nanoTime();
            ProduceAck ack;
            try {
                if (remaining <= 0) {
                    ack = ProduceAck.error(Errors.REQUEST_TIMED_OUT);
                } else if (slot.future.isDone()) {
                    ack = slot.future.getNow(ProduceAck.error(Errors.REQUEST_TIMED_OUT));
                } else {
                    ack = slot.future.get(remaining, TimeUnit.NANOSECONDS);
                }
            } catch (Exception failure) {
                ack = ProduceAck.error(Errors.REQUEST_TIMED_OUT);
            }
            apply(slot.response, ack);
        }
    }

    private static PartitionProduceResponse apply(PartitionProduceResponse response, ProduceAck ack) {
        return response
            .setErrorCode(ack.errorCode())
            .setBaseOffset(ack.baseOffset())
            .setLogStartOffset(ack.logStartOffset())
            .setLogAppendTimeMs(-1);
    }

    private static Checked check(StoredTopic topic, PartitionProduceData partition, int objectMaxBytes) {
        if (topic == null || partition.index() < 0 || partition.index() >= topic.partitions()) {
            return Checked.bad(ProduceAck.error(Errors.UNKNOWN_TOPIC_OR_PARTITION));
        }
        BaseRecords raw = partition.records();
        if (!(raw instanceof MemoryRecords memory) || memory.sizeInBytes() <= 0) {
            return Checked.bad(ProduceAck.error(Errors.INVALID_RECORD));
        }
        if (memory.sizeInBytes() > objectMaxBytes) {
            return Checked.bad(ProduceAck.error(Errors.MESSAGE_TOO_LARGE));
        }
        MutableRecordBatch batch;
        try {
            MutableRecordBatch seen = null;
            int batches = 0;
            for (MutableRecordBatch candidate : memory.batches()) {
                seen = candidate;
                batches++;
            }
            if (batches != 1 || seen.magic() != 2 || seen.isControlBatch() || seen.isTransactional()) {
                return Checked.bad(ProduceAck.error(Errors.INVALID_RECORD));
            }
            seen.ensureValid();
            batch = seen;
        } catch (RuntimeException invalid) {
            return Checked.bad(ProduceAck.error(Errors.INVALID_RECORD));
        }
        int records = (int) (batch.nextOffset() - batch.baseOffset());
        if (records < 1) {
            return Checked.bad(ProduceAck.error(Errors.INVALID_RECORD));
        }
        ByteBuffer view = memory.buffer().duplicate();
        int size = memory.sizeInBytes();
        if (view.remaining() < size) {
            return Checked.bad(ProduceAck.error(Errors.INVALID_RECORD));
        }
        byte[] bytes = new byte[size];
        view.get(bytes);
        long producerId = batch.producerId();
        return Checked.ok(new Append(
            topic.id(),
            partition.index(),
            bytes,
            records,
            (short) 1,
            batch.maxTimestamp(),
            producerId < 0 ? -1L : producerId,
            batch.producerEpoch(),
            batch.baseSequence(),
            batch.lastSequence()
        ));
    }

    private static StoredTopic find(List<StoredTopic> topics, String name) {
        for (StoredTopic topic : topics) {
            if (topic.name().equals(name)) {
                return topic;
            }
        }
        return null;
    }

    private static ProduceResponseData errorResponse(ProduceRequestData data, Errors error) {
        TopicProduceResponseCollection responses = new TopicProduceResponseCollection();
        for (TopicProduceData topicData : data.topicData()) {
            TopicProduceResponse topic = new TopicProduceResponse().setName(topicData.name());
            List<PartitionProduceResponse> partitions = new ArrayList<>();
            for (PartitionProduceData partition : topicData.partitionData()) {
                partitions.add(apply(new PartitionProduceResponse().setIndex(partition.index()), ProduceAck.error(error)));
            }
            topic.setPartitionResponses(partitions);
            responses.add(topic);
        }
        return new ProduceResponseData().setResponses(responses);
    }

    private static ByteBuffer respond(KafkaRequest request, ProduceResponseData data) {
        return KafkaCodec.encodeResponse(
            request.header(),
            request.header().apiVersion(),
            new ProduceResponse(data)
        );
    }

    private record Checked(Append append, ProduceAck error) {
        private static Checked ok(Append append) {
            return new Checked(append, null);
        }

        private static Checked bad(ProduceAck error) {
            return new Checked(null, error);
        }
    }

    private record Slot(PartitionProduceResponse response, CompletableFuture<ProduceAck> future) {
    }
}
