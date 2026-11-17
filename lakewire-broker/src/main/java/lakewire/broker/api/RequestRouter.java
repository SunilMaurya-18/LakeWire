package lakewire.broker.api;


import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests
        .AbstractResponse;
import org.apache.kafka.common.requests
        .RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


public final class RequestRouter implements RequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(RequestRouter.class);

    @Override
    public ByteBuffer process(ByteBuffer frame) {
        KafkaRequest request = KafkaCodec.decodeRequest(frame);
        RequestHeader header = request.header();

        log.debug(
                "{} v{} correlationId={} clientId={}", header.apiKey(), header.apiVersion(), header.correlationId(),
                header.clientId()
        );
        return switch (header.apiKey()) {
            case API_VERSIONS -> ApiVersionsHandler.handle(request);
            default -> unsupported(request);
        };
    }

    private static ByteBuffer unsupported(KafkaRequest request) {
        AbstractResponse response = request.body().getErrorResponse(
                Errors.UNSUPPORTED_VERSION.exception()
        );
        return KafkaCodec.encodeResponse(
                request.header(),
                request.header().apiVersion(),
                response
        );
    }

}
