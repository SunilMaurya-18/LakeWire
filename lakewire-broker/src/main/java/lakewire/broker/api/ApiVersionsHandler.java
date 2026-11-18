package lakewire.broker.api;

import java.nio.ByteBuffer;
import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import lakewire.protocol.SupportedApi;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;

/** ApiVersions (key 18): tells the client which APIs and versions LakeWire speaks. */
public final class ApiVersionsHandler {

    private ApiVersionsHandler() {
    }

    public static ByteBuffer handle(KafkaRequest request) {
        RequestHeader header = request.header();
        ApiVersionsRequest body = (ApiVersionsRequest) request.body();

        if (body.hasUnsupportedRequestVersion() || !SupportedApi.API_VERSIONS.supportsVersion(header.apiVersion())) {
            // KIP-511: answer with a v0 body, UNSUPPORTED_VERSION and our list, so the client retries
            // with the highest version we support.
            ApiVersionsResponseData data = new ApiVersionsResponseData()
                .setErrorCode(Errors.UNSUPPORTED_VERSION.code())
                .setApiKeys(SupportedApi.advertisedApiVersions());
            return KafkaCodec.encodeResponse(header, (short) 0, new ApiVersionsResponse(data));
        }

        Errors error = body.isValid() ? Errors.NONE : Errors.INVALID_REQUEST;
        ApiVersionsResponseData data = new ApiVersionsResponseData()
            .setErrorCode(error.code())
            .setApiKeys(SupportedApi.advertisedApiVersions());
        return KafkaCodec.encodeResponse(header, header.apiVersion(), new ApiVersionsResponse(data));
    }
}
