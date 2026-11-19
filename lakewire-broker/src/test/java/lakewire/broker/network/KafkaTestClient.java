package lakewire.broker.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

/** A tiny blocking client that speaks Kafka framing: a 4-byte size, then the bytes. */
public final class KafkaTestClient implements AutoCloseable {

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    public KafkaTestClient(int port) throws IOException {
        socket = new Socket("127.0.0.1", port);
        socket.setSoTimeout(5_000);
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
    }

    public void send(ByteBuffer frame) throws IOException {
        byte[] bytes = new byte[frame.remaining()];
        frame.get(bytes);
        out.writeInt(bytes.length);
        out.write(bytes);
        out.flush();
    }

    public ByteBuffer receive() throws IOException {
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        return ByteBuffer.wrap(bytes);
    }

    /** Returns -1 once the server has closed the connection. */
    int readByteOrEof() throws IOException {
        return in.read();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
