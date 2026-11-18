package lakewire.broker.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lakewire.coordinator.ClusterView;
import lakewire.coordinator.CoordinatorException;
import lakewire.coordinator.LiveBroker;
import lakewire.coordinator.StoredTopic;
import lakewire.coordinator.TopicDirectory;
import lakewire.coordinator.TopicMutation;
import lakewire.protocol.SupportedApi;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicHandlersTest {

    @Test
    void metadataListsLiveBrokersAndThisBrokerAsLeader() {
        FakeCluster cluster = new FakeCluster();
        cluster.topics.add(new StoredTopic(4, "orders", 2, TopicDirectory.DEFAULT_RETENTION_MS));
        MetadataResponse response = metadata(cluster, null);
        assertThat(response.data().clusterId()).isEqualTo("abc");
        assertThat(response.data().controllerId()).isEqualTo(1);
        assertThat(response.data().brokers()).hasSize(2);
        assertThat(response.data().brokers().find(1).rack()).isEqualTo("a");
        var topic = response.data().topics().find("orders");
        assertThat(topic.errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(topic.partitions()).hasSize(2);
        assertThat(topic.partitions().get(0).leaderId()).isEqualTo(7);
        assertThat(topic.partitions().get(0).leaderEpoch()).isEqualTo(-1);
        assertThat(topic.partitions().get(0).replicaNodes()).containsExactly(7);
    }

    @Test
    void unknownTopicIsReportedAndAutoCreateStaysOff() {
        FakeCluster cluster = new FakeCluster();
        MetadataResponse response = metadata(cluster, "missing");
        assertThat(response.data().topics().find("missing").errorCode())
            .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        assertThat(cluster.topics).isEmpty();
    }

    @Test
    void createRejectsBadNamePartitionsAndConfig() {
        FakeCluster cluster = new FakeCluster();
        assertThat(create(cluster, "..", 1, (short) 1, null).errorCode())
            .isEqualTo(Errors.INVALID_TOPIC_EXCEPTION.code());
        assertThat(create(cluster, "ok", 0, (short) 1, null).errorCode())
            .isEqualTo(Errors.INVALID_PARTITIONS.code());
        assertThat(create(cluster, "ok", 1, (short) 1, config("cleanup.policy", "compact")).errorCode())
            .isEqualTo(Errors.INVALID_CONFIG.code());
        assertThat(cluster.topics).isEmpty();
    }

    @Test
    void createDefaultsAndDuplicate() {
        FakeCluster cluster = new FakeCluster();
        var created = create(cluster, "orders", -1, (short) -1, config("retention.ms", "1000"));
        assertThat(created.errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(created.numPartitions()).isEqualTo(1);
        assertThat(created.replicationFactor()).isEqualTo((short) 1);
        assertThat(cluster.topics).singleElement().satisfies(topic -> {
            assertThat(topic.partitions()).isEqualTo(1);
            assertThat(topic.retentionMs()).isEqualTo(1000);
        });
        assertThat(create(cluster, "orders", 1, (short) 1, null).errorCode())
            .isEqualTo(Errors.TOPIC_ALREADY_EXISTS.code());
    }

    @Test
    void validateOnlyDoesNotInsert() {
        FakeCluster cluster = new FakeCluster();
        CreateTopicsResponse response = createRaw(cluster, "ghost", 2, (short) 3, null, true);
        assertThat(response.data().topics().find("ghost").errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(cluster.topics).isEmpty();
    }

    @Test
    void deleteUnknownAndKnown() {
        FakeCluster cluster = new FakeCluster();
        assertThat(delete(cluster, "missing").errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        create(cluster, "orders", 1, (short) 1, null);
        assertThat(delete(cluster, "orders").errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(cluster.topics).isEmpty();
    }

    @Test
    void coordinatorFailureIsRequestTimedOut() {
        FakeCluster cluster = new FakeCluster();
        cluster.fail = true;
        assertThat(create(cluster, "orders", 1, (short) 1, null).errorCode())
            .isEqualTo(Errors.REQUEST_TIMED_OUT.code());
    }

    @Test
    void everySupportedApiHasARouterCase() {
        FakeCluster cluster = new FakeCluster();
        RequestRouter router = new RequestRouter(cluster, 7);
        for (SupportedApi api : SupportedApi.values()) {
            assertThat(RequestRouter.HANDLED).contains(api.apiKey());
            RequestHeader header = new RequestHeader(api.apiKey(), api.minVersion(), "unit", 3);
            AbstractRequest body = body(api);
            ByteBuffer frame = body.serializeWithHeader(header);
            ByteBuffer response = router.process(frame).orElseThrow();
            AbstractResponse parsed = AbstractResponse.parseResponse(response, header);
            assertThat(parsed.errorCounts())
                .as(api.apiKey().name())
                .doesNotContainKey(Errors.UNSUPPORTED_VERSION);
        }
    }

    private static MetadataResponse metadata(FakeCluster cluster, String topic) {
        MetadataRequestData data = new MetadataRequestData()
            .setAllowAutoTopicCreation(true)
            .setTopics(topic == null ? null : List.of(new MetadataRequestTopic().setName(topic)));
        RequestHeader header = new RequestHeader(ApiKeys.METADATA, (short) 9, "unit", 1);
        MetadataRequest request = new MetadataRequest(data, header.apiVersion());
        ByteBuffer response = new RequestRouter(cluster, 7).process(request.serializeWithHeader(header)).orElseThrow();
        return (MetadataResponse) AbstractResponse.parseResponse(response, header);
    }

    private static org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult create(
        FakeCluster cluster,
        String name,
        int partitions,
        short replication,
        CreatableTopicConfig config
    ) {
        return createRaw(cluster, name, partitions, replication, config, false).data().topics().find(name);
    }

    private static CreateTopicsResponse createRaw(
        FakeCluster cluster,
        String name,
        int partitions,
        short replication,
        CreatableTopicConfig config,
        boolean validateOnly
    ) {
        CreatableTopic topic = new CreatableTopic()
            .setName(name)
            .setNumPartitions(partitions)
            .setReplicationFactor(replication);
        if (config != null) {
            CreatableTopicConfigCollection configs = new CreatableTopicConfigCollection();
            configs.add(config);
            topic.setConfigs(configs);
        }
        CreateTopicsRequestData.CreatableTopicCollection topics = new CreateTopicsRequestData.CreatableTopicCollection();
        topics.add(topic);
        CreateTopicsRequestData data = new CreateTopicsRequestData()
            .setValidateOnly(validateOnly)
            .setTopics(topics);
        RequestHeader header = new RequestHeader(ApiKeys.CREATE_TOPICS, (short) 6, "unit", 2);
        CreateTopicsRequest request = new CreateTopicsRequest.Builder(data).build(header.apiVersion());
        ByteBuffer response = new RequestRouter(cluster, 7).process(request.serializeWithHeader(header)).orElseThrow();
        return (CreateTopicsResponse) AbstractResponse.parseResponse(response, header);
    }

    private static org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult delete(
        FakeCluster cluster,
        String name
    ) {
        DeleteTopicsRequestData data = new DeleteTopicsRequestData().setTopicNames(List.of(name));
        RequestHeader header = new RequestHeader(ApiKeys.DELETE_TOPICS, (short) 5, "unit", 4);
        DeleteTopicsRequest request = new DeleteTopicsRequest.Builder(data).build(header.apiVersion());
        ByteBuffer response = new RequestRouter(cluster, 7).process(request.serializeWithHeader(header)).orElseThrow();
        DeleteTopicsResponse parsed = (DeleteTopicsResponse) AbstractResponse.parseResponse(response, header);
        return parsed.data().responses().find(name);
    }

    private static CreatableTopicConfig config(String name, String value) {
        return new CreatableTopicConfig().setName(name).setValue(value);
    }

    private static AbstractRequest body(SupportedApi api) {
        return switch (api.apiKey()) {
            case API_VERSIONS -> new org.apache.kafka.common.requests.ApiVersionsRequest.Builder(api.minVersion())
                .build(api.minVersion());
            case METADATA -> new MetadataRequest(new MetadataRequestData(), api.minVersion());
            case CREATE_TOPICS -> new CreateTopicsRequest.Builder(new CreateTopicsRequestData()).build(api.minVersion());
            case DELETE_TOPICS -> new DeleteTopicsRequest.Builder(new DeleteTopicsRequestData()).build(api.minVersion());
            case PRODUCE -> new org.apache.kafka.common.requests.ProduceRequest.Builder(
                api.minVersion(),
                api.maxVersion(),
                new org.apache.kafka.common.message.ProduceRequestData().setAcks((short) -1)
            ).build(api.minVersion());
            default -> throw new IllegalStateException(api.apiKey().name());
        };
    }

    private static final class FakeCluster implements ClusterView {
        private final List<StoredTopic> topics = new ArrayList<>();
        private boolean fail;

        @Override
        public List<LiveBroker> liveBrokers() {
            if (fail) {
                throw new CoordinatorException("down");
            }
            return List.of(
                new LiveBroker(1, "10.0.0.1", 9092, "a"),
                new LiveBroker(3, "10.0.0.3", 9092, "b")
            );
        }

        @Override
        public String clusterId() {
            return "abc";
        }

        @Override
        public List<StoredTopic> topics() {
            return List.copyOf(topics);
        }

        @Override
        public TopicMutation createTopic(String name, int partitions, long retentionMs, boolean validateOnly) {
            if (fail) {
                throw new CoordinatorException("down");
            }
            for (StoredTopic topic : topics) {
                if (topic.name().equals(name)) {
                    return new TopicMutation(Errors.TOPIC_ALREADY_EXISTS.code(), null);
                }
            }
            if (!validateOnly) {
                topics.add(new StoredTopic(topics.size() + 1L, name, partitions, retentionMs));
            }
            return new TopicMutation(Errors.NONE.code(), validateOnly ? null : (long) topics.size());
        }

        @Override
        public short deleteTopic(String name) {
            if (fail) {
                throw new CoordinatorException("down");
            }
            boolean removed = topics.removeIf(topic -> topic.name().equals(name));
            return removed ? Errors.NONE.code() : Errors.UNKNOWN_TOPIC_OR_PARTITION.code();
        }
    }
}
