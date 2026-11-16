package lakewire.broker.protocol;

import io.netty.buffer.ByteBuf;

public final class ByteBufUtils {
    private ByteBufUtils() {
    }

    public static void writeUnsignedVarint(ByteBuf out, int value) {
        while ((value & 0xFFFFFF80) != 0) {
            out.writeByte((byte) (value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.writeByte(value);

    }

}
