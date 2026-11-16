package lakewire.broker.network;

import lakewire.broker.config.BrokerProperties;
import org.junit.jupiter.api.Test;

import java.net.Socket;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaServerTest {
    @Test
    void acceptTcpConnectionsAndShutDownCleanly() throws Exception {
        KafkaServer server = new KafkaServer(new BrokerProperties("127.0.0.1", 0, 1_048_576));
        server.start();
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            assertThat(server.isRunning()).isTrue();
            assertThat(socket.isConnected()).isTrue();
        } finally {
            server.stop();
        }
        assertThat(server.isRunning()).isFalse();
    }
}
