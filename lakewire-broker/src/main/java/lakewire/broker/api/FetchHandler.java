package lakewire.broker.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lakewire.broker.read.LogReader;
import lakewire.broker.read.PartitionRead;
import lakewire.coordinator.CoordinatorException;
import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestData.FetchPartition;
import org.apache.kafka.common.message.FetchRequestData.FetchTopic;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Full fetches only. {@code sessionId} is always 0. acks wait is a virtual-thread park on NOTIFY. */
public final class FetchHandler {

    private static final Logger log = LoggerFactory.getLogger(FetchHandler.class);

    private FetchHandler() {
    }

    public static ByteBuffer handle(KafkaRequest request, LogReader reader) {
        if (reader == null) {
            return RequestRouter.coordinatorUnavailable(request);
        }
        FetchRequestData data = ((FetchRequest) request.body()).data();
        try {
            long generation = reader.generation();
            List<FetchableTopicResponse> responses = read(data, reader);
            if (totalBytes(responses) < data.minBytes() && data.maxWaitMs() > 0) {
                reader.await(generation, data.maxWaitMs());
                responses = read(data, reader);
            }
            return KafkaCodec.encodeResponse(
                request.header(),
                request.header().apiVersion(),
                FetchResponse.of(new FetchResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setSessionId(0)
                    .setResponses(responses))
            );
        } catch (CoordinatorException failure) {
            log.warn("fetch failed: {}", failure.toString());
            return RequestRouter.coordinatorUnavailable(request);
        }
    }

    private static List<FetchableTopicResponse> read(FetchRequestData data, LogReader reader) {
        int remaining = data.maxBytes() <= 0 ? Integer.MAX_VALUE : data.maxBytes();
        List<FetchableTopicResponse> responses = new ArrayList<>();
        for (FetchTopic topic : data.topics()) {
            FetchableTopicResponse topicResponse = new FetchableTopicResponse().setTopic(topic.topic());
            List<PartitionData> partitions = new ArrayList<>();
            for (FetchPartition partition : topic.partitions()) {
                int budget = partition.partitionMaxBytes() <= 0
                    ? remaining
                    : Math.min(remaining, partition.partitionMaxBytes());
                PartitionRead read = reader.fetch(topic.topic(), partition.partition(), partition.fetchOffset(), budget);
                partitions.add(new PartitionData()
                    .setPartitionIndex(partition.partition())
                    .setErrorCode(read.errorCode())
                    .setHighWatermark(read.highWatermark())
                    .setLastStableOffset(read.highWatermark())
                    .setLogStartOffset(read.logStartOffset())
                    .setPreferredReadReplica(-1)
                    .setRecords(read.records()));
                remaining = Math.max(0, remaining - read.records().sizeInBytes());
            }
            topicResponse.setPartitions(partitions);
            responses.add(topicResponse);
        }
        return responses;
    }

    private static int totalBytes(List<FetchableTopicResponse> responses) {
        int bytes = 0;
        for (FetchableTopicResponse topic : responses) {
            for (PartitionData partition : topic.partitions()) {
                if (partition.records() instanceof MemoryRecords records) {
                    bytes += records.sizeInBytes();
                }
            }
        }
        return bytes;
    }
}
