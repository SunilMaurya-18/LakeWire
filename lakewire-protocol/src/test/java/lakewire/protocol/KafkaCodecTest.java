package lakewire.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

class KafkaCodecTest {

    @Test
    void decodesFlexibleV3RequestBuiltByKafka() {
        ApiVersionsRequest original = new ApiVersionsRequest.Builder((short) 3).build((short) 3);
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "codec-test", 7);
        ByteBuffer frame = original.serializeWithHeader(header);

        KafkaRequest decoded = KafkaCodec.decodeRequest(frame);

        assertThat(decoded.header().apiKey()).isEqualTo(ApiKeys.API_VERSIONS);
        assertThat(decoded.header().apiVersion()).isEqualTo((short) 3);
        assertThat(decoded.header().correlationId()).isEqualTo(7);
        assertThat(decoded.header().clientId()).isEqualTo("codec-test");
        ApiVersionsRequest body = (ApiVersionsRequest) decoded.body();
        assertThat(body.data().clientSoftwareName()).isEqualTo("apache-kafka-java");
        assertThat(body.hasUnsupportedRequestVersion()).isFalse();
    }

    @Test
    void encodedResponseIsReadableByKafka() {
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "codec-test", 11);
        ApiVersionsResponse response = new ApiVersionsResponse(new ApiVersionsResponseData()
            .setErrorCode(Errors.NONE.code())
            .setApiKeys(SupportedApi.advertisedApiVersions()));

        ByteBuffer bytes = KafkaCodec.encodeResponse(header, (short) 3, response);

        ApiVersionsResponse parsed = (ApiVersionsResponse) AbstractResponse.parseResponse(bytes, header);
        assertThat(parsed.data().errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(parsed.data().apiKeys().find((short) 18).maxVersion()).isEqualTo((short) 3);
    }

    @Test
    void unknownApiKeyIsRejected() {
        ByteBuffer frame = ByteBuffer.allocate(10)
            .putShort((short) 9999).putShort((short) 0).putInt(1).putShort((short) -1)
            .flip();

        assertThatThrownBy(() -> KafkaCodec.decodeRequest(frame))
            .isInstanceOf(InvalidRequestException.class);
    }
}
