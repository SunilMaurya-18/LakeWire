package lakewire.broker.network;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;
import java.net.Socket;
import lakewire.broker.config.BrokerConfig;
import org.junit.jupiter.api.Test;

class KafkaServerTest {

    @Test
    void startsAcceptsConnectionsAndStops() throws Exception {
        KafkaServer server = new KafkaServer(new BrokerConfig("127.0.0.1", 0, 1_048_576));
        server.start();
        try {
            assertThat(server.isRunning()).isTrue();
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 2_000);
                assertThat(socket.isConnected()).isTrue();
            }
        } finally {
            server.stop();
        }
        assertThat(server.isRunning()).isFalse();
    }
}
