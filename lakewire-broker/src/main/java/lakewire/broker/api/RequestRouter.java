package lakewire.broker.api;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import lakewire.broker.read.LogReader;
import lakewire.broker.write.ProduceSink;
import lakewire.coordinator.ClusterView;
import lakewire.coordinator.Coordinator;
import lakewire.protocol.ErrorMapping;
import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import lakewire.protocol.SupportedApi;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Decodes a frame with Kafka's classes and sends it to the handler for its API. */
public final class RequestRouter implements RequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RequestRouter.class);

    /** Kept in step with the switch below. {@code RequestRouterTest} fails if an API is missing. */
    static final Set<ApiKeys> HANDLED = EnumSet.of(
        ApiKeys.API_VERSIONS,
        ApiKeys.METADATA,
        ApiKeys.PRODUCE,
        ApiKeys.FETCH,
        ApiKeys.LIST_OFFSETS,
        ApiKeys.INIT_PRODUCER_ID,
        ApiKeys.CREATE_TOPICS,
        ApiKeys.DELETE_TOPICS
    );

    private final ClusterView topics;
    private final ProduceSink writes;
    private final LogReader reader;
    private final Coordinator producers;

    public RequestRouter() {
        this(null, null, null, null);
    }

    public RequestRouter(ClusterView topics) {
        this(topics, null, null, null);
    }

    public RequestRouter(ClusterView topics, ProduceSink writes) {
        this(topics, writes, null, null);
    }

    public RequestRouter(ClusterView topics, ProduceSink writes, LogReader reader) {
        this(topics, writes, reader, null);
    }

    public RequestRouter(
        ClusterView topics,
        ProduceSink writes,
        LogReader reader,
        Coordinator producers
    ) {
        this.topics = topics;
        this.writes = writes;
        this.reader = reader;
        this.producers = producers;
    }

    @Override
    public Optional<ByteBuffer> process(ByteBuffer frame) {
        KafkaRequest request = KafkaCodec.decodeRequest(frame);
        RequestHeader header = request.header();
        log.debug("{} v{} correlationId={} clientId={}",
            header.apiKey(), header.apiVersion(), header.correlationId(), header.clientId());

        SupportedApi supported = SupportedApi.forApiKey(header.apiKey()).orElse(null);
        if (header.apiKey() != ApiKeys.API_VERSIONS
            && (supported == null || !supported.supportsVersion(header.apiVersion()))) {
            return Optional.of(unsupported(request));
        }

        return switch (header.apiKey()) {
            case API_VERSIONS -> Optional.of(ApiVersionsHandler.handle(request));
            case METADATA -> Optional.of(MetadataHandler.handle(request, topics));
            case PRODUCE -> ProduceHandler.handle(request, topics, writes);
            case FETCH -> Optional.of(FetchHandler.handle(request, reader));
            case LIST_OFFSETS -> Optional.of(ListOffsetsHandler.handle(request, reader));
            case INIT_PRODUCER_ID -> Optional.of(InitProducerIdHandler.handle(request, producers));
            case CREATE_TOPICS -> Optional.of(CreateTopicsHandler.handle(request, topics));
            case DELETE_TOPICS -> Optional.of(DeleteTopicsHandler.handle(request, topics));
            default -> Optional.of(unsupported(request));
        };
    }

    /** A real Kafka API that we don't implement yet: answer with Kafka's own error shape for it. */
    private static ByteBuffer unsupported(KafkaRequest request) {
        AbstractResponse response = request.body().getErrorResponse(Errors.UNSUPPORTED_VERSION.exception());
        return KafkaCodec.encodeResponse(request.header(), request.header().apiVersion(), response);
    }

    static ByteBuffer coordinatorUnavailable(KafkaRequest request) {
        AbstractResponse response = request.body().getErrorResponse(ErrorMapping.COORDINATOR_UNAVAILABLE.error().exception());
        return KafkaCodec.encodeResponse(request.header(), request.header().apiVersion(), response);
    }
}
