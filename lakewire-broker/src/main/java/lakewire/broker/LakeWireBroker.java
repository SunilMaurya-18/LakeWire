package lakewire.broker;

import java.util.Map;
import lakewire.broker.config.BrokerConfig;
import lakewire.broker.write.WriteSettings;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.storage.ObjectStore;
import lakewire.workers.WorkerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LakeWireBroker {

    private static final Logger log = LoggerFactory.getLogger(LakeWireBroker.class);

    private LakeWireBroker() {
    }

    public static void main(String[] args) {
        Map<String, String> env = System.getenv();
        BrokerConfig config = BrokerConfig.fromEnvironment(env);
        CoordinatorConfig coordinatorConfig = CoordinatorConfig.fromEnvironment(env);
        ObjectStore store = BrokerRuntime.openStore(env, config);
        BrokerRuntime runtime = new BrokerRuntime(
            config,
            coordinatorConfig,
            store,
            WriteSettings.fromEnvironment(env),
            WorkerConfig.fromEnvironment(env)
        );
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform()
            .name("lakewire-shutdown")
            .unstarted(() -> {
                log.info("Shutdown signal received, stopping LakeWire broker");
                runtime.close();
                log.info("LakeWire broker stopped");
            }));
        runtime.start();
    }
}
