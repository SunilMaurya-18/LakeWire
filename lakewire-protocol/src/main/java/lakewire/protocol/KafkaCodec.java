package lakewire.protocol;

import java.nio.ByteBuffer;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

/** Glue between raw Kafka frames and kafka-clients' generated message classes. */
public final class KafkaCodec {

    private KafkaCodec() {
    }

    /**
     * Decodes one frame (without its 4-byte length prefix).
     *
     * @throws org.apache.kafka.common.errors.InvalidRequestException for an unknown api_key or a malformed header
     */
    public static KafkaRequest decodeRequest(ByteBuffer frame) {
        RequestHeader header = RequestHeader.parse(frame);
        if (header.apiKey() == ApiKeys.API_VERSIONS && !header.isApiVersionSupported()) {
            // A client newer than this kafka-clients jar: don't parse a body we can't read.
            // Treat it as v0 and remember the real version, as Kafka's own broker does.
            ApiVersionsRequest body =
                new ApiVersionsRequest(new ApiVersionsRequestData(), (short) 0, header.apiVersion());
            return new KafkaRequest(header, body);
        }
        AbstractRequest body =
            AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(), new ByteBufferAccessor(frame)).request;
        return new KafkaRequest(header, body);
    }

    /** Encodes response header + body (without the length prefix, which Netty's LengthFieldPrepender adds). */
    public static ByteBuffer encodeResponse(RequestHeader requestHeader, short responseVersion,
                                            AbstractResponse response) {
        ResponseHeader responseHeader = requestHeader.toResponseHeader();
        ResponseHeaderData headerData = responseHeader.data();
        short headerVersion = responseHeader.headerVersion();
        ApiMessage body = response.data();

        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = headerData.size(cache, headerVersion) + body.size(cache, responseVersion);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        ByteBufferAccessor writer = new ByteBufferAccessor(buffer);
        headerData.write(writer, cache, headerVersion);
        body.write(writer, cache, responseVersion);
        buffer.flip();
        return buffer;
    }
}
