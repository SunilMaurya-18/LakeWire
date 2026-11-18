package lakewire.broker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lakewire.broker.api.RequestRouter;
import lakewire.broker.config.BrokerConfig;
import lakewire.broker.network.KafkaServer;
import lakewire.coordinator.BrokerRegistry;
import lakewire.coordinator.Coordinator;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.coordinator.CoordinatorException;
import lakewire.coordinator.TopicDirectory;
import lakewire.protocol.ErrorMapping;
import lakewire.storage.LocalFsObjectStore;
import lakewire.storage.ObjectStore;
import lakewire.storage.S3ObjectStore;
import lakewire.storage.S3StorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One broker process: schema migration, registration, heartbeat, and the Kafka socket.
 * A failed heartbeat fences writes and re-registers (A7). The Postgres lease is left to expire.
 */
public final class BrokerRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BrokerRuntime.class);

    private final BrokerConfig config;
    private final CoordinatorConfig coordinatorConfig;
    private final ObjectStore store;
    private final AtomicBoolean acceptingWrites = new AtomicBoolean(true);
    private Coordinator coordinator;
    private KafkaServer server;
    private Thread heartbeat;
    private volatile long epoch;
    private volatile boolean open;
    private boolean storeClosed;

    public BrokerRuntime(BrokerConfig config, CoordinatorConfig coordinatorConfig, ObjectStore store) {
        this.config = config;
        this.coordinatorConfig = coordinatorConfig;
        this.store = store;
    }

    public static ObjectStore openStore(Map<String, String> env, BrokerConfig config) {
        String mode = env.get("LAKEWIRE_STORAGE");
        if (mode == null || mode.isBlank() || "fs".equals(mode.trim())) {
            try {
                Files.createDirectories(config.dataDir());
            } catch (IOException failure) {
                throw new UncheckedIOException("cannot create " + config.dataDir(), failure);
            }
            return new LocalFsObjectStore(config.dataDir());
        }
        if ("s3".equals(mode.trim())) {
            return new S3ObjectStore(S3StorageConfig.fromEnvironment(env));
        }
        throw new IllegalArgumentException("LAKEWIRE_STORAGE must be fs or s3");
    }

    public synchronized void start() {
        if (open) {
            return;
        }
        try {
            coordinator = new Coordinator(coordinatorConfig);
            coordinator.start();
            server = new KafkaServer(config, new RequestRouter(new TopicDirectory(coordinator), config.brokerId()));
            server.start();
            int advertisedPort = config.advertisedPort() == 0 ? server.boundPort() : config.advertisedPort();
            String endpoint = config.advertisedHost() + ":" + advertisedPort;
            BrokerRegistry registry = new BrokerRegistry(coordinator);
            epoch = registry.register(Integer.toString(config.brokerId()), config.zone(), endpoint);
            acceptingWrites.set(true);
            heartbeat = Thread.ofVirtual().name("lakewire-heartbeat").start(() -> beat(registry, endpoint));
            open = true;
            log.info("broker {} epoch {} listening on {} advertising {}",
                config.brokerId(), epoch, server.boundPort(), endpoint);
        } catch (RuntimeException failure) {
            cleanup();
            throw failure;
        }
    }

    public int port() {
        return server.boundPort();
    }

    public long epoch() {
        return epoch;
    }

    public boolean acceptingWrites() {
        return acceptingWrites.get();
    }

    /** M4 attaches produce futures here. M3 has no in-flight writes yet. */
    public void failInFlight(short errorCode) {
        log.info("failing in-flight writes with error {}", errorCode);
    }

    private void beat(BrokerRegistry registry, String endpoint) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(coordinatorConfig.heartbeatMs());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                if (registry.heartbeat(Integer.toString(config.brokerId()), epoch)) {
                    continue;
                }
                acceptingWrites.set(false);
                failInFlight(ErrorMapping.STALE_BROKER_EPOCH.code());
                epoch = registry.register(Integer.toString(config.brokerId()), config.zone(), endpoint);
                acceptingWrites.set(true);
                log.info("broker {} re-registered at epoch {}", config.brokerId(), epoch);
            } catch (CoordinatorException failure) {
                log.warn("heartbeat failed: {}", failure.toString());
            }
        }
    }

    @Override
    public synchronized void close() {
        cleanup();
    }

    private void cleanup() {
        if (heartbeat != null) {
            heartbeat.interrupt();
            heartbeat = null;
        }
        if (server != null) {
            server.stop();
            server = null;
        }
        if (store != null && !storeClosed) {
            store.close();
            storeClosed = true;
        }
        if (coordinator != null) {
            coordinator.close();
            coordinator = null;
        }
        open = false;
    }
}
