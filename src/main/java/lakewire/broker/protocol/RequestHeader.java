package lakewire.broker.protocol;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public record RequestHeader(short ApiKey, short version, int correlationId, String clientId) {
    public static final int FIXED_HEADER_BYTES = 2 + 2 + 4;

    public static RequestHeader parse(ByteBuf buf) {
        if (buf.readableBytes() < FIXED_HEADER_BYTES) {
            throw new InvalidRequestException("Request header is too short" + buf.readableBytes() + "bytes");
        }
        short apiKey = buf.readShort();
        short version = buf.readShort();
        int correlationId = buf.readInt();
        String clientId = readNullableString(buf);
        return new RequestHeader(apiKey, version, correlationId, clientId);

    }

    public static String readNullableString(ByteBuf buf) {
        if (buf.readableBytes() < 2) {
            throw new InvalidRequestException("Missing client_id length");
        }
        short length = buf.readShort();
        {
            if (length == -1) {
                return null;
            }
            if (length < 0 || length > buf.readableBytes()) {
                throw new InvalidRequestException("Invalid client_id length" + length);
            }
            return buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        }
    }
}
