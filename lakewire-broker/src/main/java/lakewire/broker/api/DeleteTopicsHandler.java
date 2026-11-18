package lakewire.broker.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lakewire.coordinator.CoordinatorException;
import lakewire.coordinator.ClusterView;
import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResultCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DeleteTopics marks {@code deleted_at}. Object deletion waits for M8. */
public final class DeleteTopicsHandler {

    private static final Logger log = LoggerFactory.getLogger(DeleteTopicsHandler.class);

    private DeleteTopicsHandler() {
    }

    public static ByteBuffer handle(KafkaRequest request, ClusterView topics) {
        if (topics == null) {
            return RequestRouter.coordinatorUnavailable(request);
        }
        DeleteTopicsRequestData data = ((DeleteTopicsRequest) request.body()).data();
        DeletableTopicResultCollection results = new DeletableTopicResultCollection();
        for (String name : names(data)) {
            results.add(deleteOne(topics, name));
        }
        DeleteTopicsResponseData response = new DeleteTopicsResponseData().setResponses(results);
        return KafkaCodec.encodeResponse(
            request.header(),
            request.header().apiVersion(),
            new DeleteTopicsResponse(response)
        );
    }

    private static DeletableTopicResult deleteOne(ClusterView topics, String name) {
        DeletableTopicResult result = new DeletableTopicResult().setName(name);
        try {
            short code = topics.deleteTopic(name);
            result.setErrorCode(code);
            if (code != Errors.NONE.code()) {
                result.setErrorMessage("unknown topic");
            }
            return result;
        } catch (CoordinatorException failure) {
            log.warn("delete topic failed: {}", failure.toString());
            return result
                .setErrorCode(Errors.REQUEST_TIMED_OUT.code())
                .setErrorMessage("coordinator unavailable");
        }
    }

    private static List<String> names(DeleteTopicsRequestData data) {
        List<String> names = new ArrayList<>();
        if (data.topicNames() != null) {
            names.addAll(data.topicNames());
        }
        if (data.topics() != null) {
            for (DeleteTopicState topic : data.topics()) {
                if (topic.name() != null && !names.contains(topic.name())) {
                    names.add(topic.name());
                }
            }
        }
        return names;
    }
}
