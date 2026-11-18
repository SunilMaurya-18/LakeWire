package lakewire.broker.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lakewire.coordinator.CoordinatorException;
import lakewire.coordinator.LiveBroker;
import lakewire.coordinator.StoredTopic;
import lakewire.coordinator.ClusterView;
import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBrokerCollection;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopicCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Metadata. Leadership is this broker for every partition until zone routing in M7. */
public final class MetadataHandler {

    private static final Logger log = LoggerFactory.getLogger(MetadataHandler.class);

    private MetadataHandler() {
    }

    public static ByteBuffer handle(KafkaRequest request, ClusterView topics, int localNodeId) {
        if (topics == null) {
            return RequestRouter.coordinatorUnavailable(request);
        }
        try {
            MetadataRequestData data = ((MetadataRequest) request.body()).data();
            MetadataResponseData response = new MetadataResponseData();
            List<LiveBroker> brokers = topics.liveBrokers();
            MetadataResponseBrokerCollection nodes = new MetadataResponseBrokerCollection();
            int controller = -1;
            for (LiveBroker broker : brokers) {
                nodes.add(new MetadataResponseBroker()
                    .setNodeId(broker.nodeId())
                    .setHost(broker.host())
                    .setPort(broker.port())
                    .setRack(broker.zone()));
                if (controller < 0 || broker.nodeId() < controller) {
                    controller = broker.nodeId();
                }
            }
            response.setBrokers(nodes);
            response.setClusterId(topics.clusterId());
            response.setControllerId(controller);

            MetadataResponseTopicCollection topicResponses = new MetadataResponseTopicCollection();
            List<StoredTopic> stored = topics.topics();
            if (data.topics() == null) {
                for (StoredTopic topic : stored) {
                    topicResponses.add(describe(topic, localNodeId));
                }
            } else {
                for (MetadataRequestData.MetadataRequestTopic asked : data.topics()) {
                    StoredTopic match = find(stored, asked.name());
                    if (match == null) {
                        topicResponses.add(new MetadataResponseTopic()
                            .setName(asked.name())
                            .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                            .setIsInternal(false));
                    } else {
                        topicResponses.add(describe(match, localNodeId));
                    }
                }
            }
            response.setTopics(topicResponses);
            return KafkaCodec.encodeResponse(
                request.header(),
                request.header().apiVersion(),
                new MetadataResponse(response, request.header().apiVersion())
            );
        } catch (CoordinatorException failure) {
            log.warn("metadata failed: {}", failure.toString());
            return RequestRouter.coordinatorUnavailable(request);
        }
    }

    private static MetadataResponseTopic describe(StoredTopic topic, int localNodeId) {
        List<MetadataResponsePartition> partitions = new ArrayList<>(topic.partitions());
        List<Integer> replicas = List.of(localNodeId);
        for (int part = 0; part < topic.partitions(); part++) {
            partitions.add(new MetadataResponsePartition()
                .setPartitionIndex(part)
                .setErrorCode(Errors.NONE.code())
                .setLeaderId(localNodeId)
                .setLeaderEpoch(-1)
                .setReplicaNodes(replicas)
                .setIsrNodes(replicas)
                .setOfflineReplicas(List.of()));
        }
        return new MetadataResponseTopic()
            .setErrorCode(Errors.NONE.code())
            .setName(topic.name())
            .setIsInternal(false)
            .setPartitions(partitions);
    }

    private static StoredTopic find(List<StoredTopic> topics, String name) {
        for (StoredTopic topic : topics) {
            if (topic.name().equals(name)) {
                return topic;
            }
        }
        return null;
    }
}
