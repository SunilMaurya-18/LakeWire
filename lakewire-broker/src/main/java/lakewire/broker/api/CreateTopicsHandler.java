package lakewire.broker.api;

import java.nio.ByteBuffer;
import lakewire.coordinator.CoordinatorException;
import lakewire.coordinator.ClusterView;
import lakewire.coordinator.TopicDirectory;
import lakewire.coordinator.TopicMutation;
import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResultCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CreateTopics. Replication factor is echoed and not stored. Replica assignments are ignored (A34). */
public final class CreateTopicsHandler {

    private static final Logger log = LoggerFactory.getLogger(CreateTopicsHandler.class);

    private CreateTopicsHandler() {
    }

    public static ByteBuffer handle(KafkaRequest request, ClusterView topics) {
        if (topics == null) {
            return RequestRouter.coordinatorUnavailable(request);
        }
        CreateTopicsRequestData data = ((CreateTopicsRequest) request.body()).data();
        CreatableTopicResultCollection results = new CreatableTopicResultCollection();
        for (CreatableTopic topic : data.topics()) {
            results.add(createOne(topics, topic, data.validateOnly()));
        }
        CreateTopicsResponseData response = new CreateTopicsResponseData().setTopics(results);
        return KafkaCodec.encodeResponse(
            request.header(),
            request.header().apiVersion(),
            new CreateTopicsResponse(response)
        );
    }

    private static CreatableTopicResult createOne(ClusterView topics, CreatableTopic topic, boolean validateOnly) {
        CreatableTopicResult result = new CreatableTopicResult().setName(topic.name());
        short replication = topic.replicationFactor() == -1 ? 1 : topic.replicationFactor();
        int partitions = topic.numPartitions() == -1 ? 1 : topic.numPartitions();
        try {
            Topic.validate(topic.name());
        } catch (InvalidTopicException invalid) {
            return fail(result, Errors.INVALID_TOPIC_EXCEPTION, "invalid topic name");
        }
        if (topic.numPartitions() != -1 && topic.numPartitions() < 1) {
            return fail(result, Errors.INVALID_PARTITIONS, "partitions must be at least 1");
        }
        Long retention = retention(topic);
        if (retention == null) {
            return fail(result, Errors.INVALID_CONFIG, "unsupported topic config");
        }
        try {
            TopicMutation mutation = topics.createTopic(topic.name(), partitions, retention, validateOnly);
            if (mutation.errorCode() != Errors.NONE.code()) {
                return fail(result, Errors.forCode(mutation.errorCode()), errorText(mutation.errorCode()));
            }
            return result
                .setErrorCode(Errors.NONE.code())
                .setNumPartitions(partitions)
                .setReplicationFactor(replication);
        } catch (CoordinatorException failure) {
            log.warn("create topic failed: {}", failure.toString());
            return fail(result, Errors.REQUEST_TIMED_OUT, "coordinator unavailable");
        }
    }

    private static Long retention(CreatableTopic topic) {
        long retention = TopicDirectory.DEFAULT_RETENTION_MS;
        for (CreatableTopicConfig config : topic.configs()) {
            String name = config.name();
            String value = config.value();
            if ("retention.ms".equals(name)) {
                try {
                    retention = Long.parseLong(value);
                } catch (NumberFormatException invalid) {
                    return null;
                }
                if (retention < -1) {
                    return null;
                }
            } else if ("cleanup.policy".equals(name)) {
                if (!"delete".equals(value)) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return retention;
    }

    private static String errorText(short code) {
        Errors error = Errors.forCode(code);
        if (error == Errors.TOPIC_ALREADY_EXISTS) {
            return "topic already exists";
        }
        if (error == Errors.NONE) {
            return null;
        }
        return error.message();
    }

    private static CreatableTopicResult fail(CreatableTopicResult result, Errors error, String message) {
        return result.setErrorCode(error.code()).setErrorMessage(message);
    }
}
