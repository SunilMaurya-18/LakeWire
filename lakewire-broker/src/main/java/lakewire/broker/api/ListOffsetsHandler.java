package lakewire.broker.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lakewire.broker.read.LogReader;
import lakewire.broker.read.OffsetBound;
import lakewire.coordinator.CoordinatorException;
import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Earliest, latest, and batch-granular timestamp lookup. Read-committed is the high watermark. */
public final class ListOffsetsHandler {

    private static final Logger log = LoggerFactory.getLogger(ListOffsetsHandler.class);

    private ListOffsetsHandler() {
    }

    public static ByteBuffer handle(KafkaRequest request, LogReader reader) {
        if (reader == null) {
            return RequestRouter.coordinatorUnavailable(request);
        }
        ListOffsetsRequestData data = ((ListOffsetsRequest) request.body()).data();
        try {
            List<ListOffsetsTopicResponse> topics = new ArrayList<>();
            for (ListOffsetsTopic topic : data.topics()) {
                ListOffsetsTopicResponse topicResponse = new ListOffsetsTopicResponse().setName(topic.name());
                List<ListOffsetsPartitionResponse> partitions = new ArrayList<>();
                for (ListOffsetsPartition partition : topic.partitions()) {
                    OffsetBound bound = reader.list(topic.name(), partition.partitionIndex(), partition.timestamp());
                    partitions.add(new ListOffsetsPartitionResponse()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(bound.errorCode())
                        .setTimestamp(bound.timestamp())
                        .setOffset(bound.offset()));
                }
                topicResponse.setPartitions(partitions);
                topics.add(topicResponse);
            }
            return KafkaCodec.encodeResponse(
                request.header(),
                request.header().apiVersion(),
                new ListOffsetsResponse(new ListOffsetsResponseData().setTopics(topics))
            );
        } catch (CoordinatorException failure) {
            log.warn("list offsets failed: {}", failure.toString());
            return RequestRouter.coordinatorUnavailable(request);
        }
    }
}
