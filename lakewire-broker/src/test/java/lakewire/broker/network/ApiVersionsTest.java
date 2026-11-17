package lakewire.broker.network;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import lakewire.broker.config.BrokerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiVersionsTest {

    private KafkaServer server;

    @BeforeEach
    void startServer() {
        server = new KafkaServer(new BrokerConfig("127.0.0.1", 0, 1_048_576));
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop();
    }

    @Test
    void answersApiVersionsV0WithSupportedRange() throws Exception {
        try (Socket socket = connect();
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            sendApiVersionsRequest(out, (short) 0, 42);

            int size = in.readInt();
            assertThat(in.readInt()).as("correlation_id").isEqualTo(42);
            assertThat(in.readShort()).as("error_code").isEqualTo(ErrorCode.NONE);
            assertThat(in.readInt()).as("api count").isEqualTo(1);
            assertThat(in.readShort()).as("api_key").isEqualTo((short) 18);
            assertThat(in.readShort()).as("min_version").isEqualTo((short) 0);
            assertThat(in.readShort()).as("max_version").isEqualTo((short) 3);
            assertThat(size).isEqualTo(4 + 2 + 4 + 6);
        }
    }

    @Test
    void rejectsUnsupportedVersionUsingV0Format() throws Exception {
        try (Socket socket = connect();
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            sendApiVersionsRequest(out, (short) 99, 7);

            in.readInt();
            assertThat(in.readInt()).isEqualTo(7);
            assertThat(in.readShort()).isEqualTo(ErrorCode.UNSUPPORTED_VERSION);
        }
    }

    private Socket connect() throws Exception {
        Socket socket = new Socket("127.0.0.1", server.boundPort());
        socket.setSoTimeout(5_000);
        return socket;
    }

    private static void sendApiVersionsRequest(DataOutputStream out, short version, int correlationId)
            throws Exception {
        byte[] clientId = "test-client".getBytes(StandardCharsets.UTF_8);
        out.writeInt(2 + 2 + 4 + 2 + clientId.length);
        out.writeShort(18);
        out.writeShort(version);
        out.writeInt(correlationId);
        out.writeShort(clientId.length);
        out.write(clientId);
        out.flush();
    }
}
