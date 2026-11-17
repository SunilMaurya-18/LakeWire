package lakewire.broker;

import lakewire.broker.config.BrokerConfig;
import lakewire.broker.network.KafkaServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LakeWireBroker {

    private static final Logger log = LoggerFactory.getLogger(LakeWireBroker.class);

    private LakeWireBroker() {
    }

    public static void main(String[] args) {
        BrokerConfig config = BrokerConfig.fromEnvironment(System.getenv());
        KafkaServer server = new KafkaServer(config);
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform()
                .name("lakewire-shutdown")
                .unstarted(() -> {
                    log.info("Shutdown signal received, stopping LakeWire broker");
                    server.stop();
                    log.info("LakeWire broker stopped");
                }));
        server.start();
    }
}
