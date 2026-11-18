package lakewire.broker.api;

import java.nio.ByteBuffer;
import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Decodes a frame with Kafka's classes and sends it to the handler for its API. */
public final class RequestRouter implements RequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RequestRouter.class);

    @Override
    public ByteBuffer process(ByteBuffer frame) {
        KafkaRequest request = KafkaCodec.decodeRequest(frame);
        RequestHeader header = request.header();
        log.debug("{} v{} correlationId={} clientId={}",
            header.apiKey(), header.apiVersion(), header.correlationId(), header.clientId());

        // Keep this switch in step with SupportedApi: every entry there needs a case here.
        return switch (header.apiKey()) {
            case API_VERSIONS -> ApiVersionsHandler.handle(request);
            default -> unsupported(request);
        };
    }

    /** A real Kafka API that we don't implement yet: answer with Kafka's own error shape for it. */
    private static ByteBuffer unsupported(KafkaRequest request) {
        AbstractResponse response = request.body().getErrorResponse(Errors.UNSUPPORTED_VERSION.exception());
        return KafkaCodec.encodeResponse(request.header(), request.header().apiVersion(), response);
    }
}
