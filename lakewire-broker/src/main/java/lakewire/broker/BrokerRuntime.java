package lakewire.broker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lakewire.broker.api.RequestRouter;
import lakewire.broker.config.BrokerConfig;
import lakewire.broker.network.KafkaServer;
import lakewire.broker.read.BlockCache;
import lakewire.broker.read.HwCache;
import lakewire.broker.read.LogCatalog;
import lakewire.broker.read.LogReader;
import lakewire.broker.read.SegmentIndex;
import lakewire.broker.write.WritePipeline;
import lakewire.broker.write.WriteSettings;
import lakewire.coordinator.BrokerRegistry;
import lakewire.coordinator.Coordinator;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.coordinator.CoordinatorException;
import lakewire.coordinator.HwListener;
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
    private final WriteSettings writeSettings;
    private final AtomicBoolean acceptingWrites = new AtomicBoolean();
    private Coordinator coordinator;
    private WritePipeline pipeline;
    private HwListener hwListener;
    private KafkaServer server;
    private BrokerRegistry registry;
    private String advertisedEndpoint;
    private Thread heartbeat;
    private volatile long epoch;
    private volatile boolean open;
    private boolean storeClosed;

    public BrokerRuntime(BrokerConfig config, CoordinatorConfig coordinatorConfig, ObjectStore store) {
        this(config, coordinatorConfig, store, WriteSettings.defaults());
    }

    public BrokerRuntime(
        BrokerConfig config,
        CoordinatorConfig coordinatorConfig,
        ObjectStore store,
        WriteSettings writeSettings
    ) {
        this.config = config;
        this.coordinatorConfig = coordinatorConfig;
        this.store = store;
        this.writeSettings = writeSettings;
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
            pipeline = new WritePipeline(
                writeSettings,
                store,
                coordinator,
                () -> epoch,
                acceptingWrites,
                config.brokerId(),
                this::recoverFromFence
            );
            pipeline.start();
            TopicDirectory directory = new TopicDirectory(coordinator);
            LogCatalog catalog = new LogCatalog(coordinator.dataSource());
            hwListener = new HwListener(coordinatorConfig);
            HwCache watermarks = new HwCache(catalog, hwListener);
            hwListener.start();
            hwListener.awaitReady(Duration.ofSeconds(10));
            LogReader reader = new LogReader(
                directory,
                watermarks,
                new SegmentIndex(catalog),
                new BlockCache(store, 256L * 1024 * 1024)
            );
            server = new KafkaServer(
                config,
                new RequestRouter(directory, pipeline, reader, coordinator)
            );
            server.start();
            int advertisedPort = config.advertisedPort() == 0 ? server.boundPort() : config.advertisedPort();
            String endpoint = config.advertisedHost() + ":" + advertisedPort;
            registry = new BrokerRegistry(coordinator);
            advertisedEndpoint = endpoint;
            epoch = registry.register(Integer.toString(config.brokerId()), config.zone(), endpoint);
            acceptingWrites.set(true);
            open = true;
            heartbeat = Thread.ofVirtual().name("lakewire-heartbeat").start(this::beat);
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

    /** Completes buffered and in-flight produce futures. A commit already in Postgres still stands. */
    public void failInFlight(short errorCode) {
        log.info("failing in-flight writes with error {}", errorCode);
        if (pipeline != null) {
            pipeline.failAll(errorCode);
        }
    }

    private void beat() {
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
                recoverFromFence();
            } catch (CoordinatorException failure) {
                log.warn("heartbeat failed: {}", failure.toString());
            }
        }
    }

    /** Fail buffered produces, then take a new epoch so clients can retry (A7). */
    private synchronized void recoverFromFence() {
        if (!open || registry == null || advertisedEndpoint == null) {
            return;
        }
        acceptingWrites.set(false);
        failInFlight(ErrorMapping.STALE_BROKER_EPOCH.code());
        epoch = registry.register(Integer.toString(config.brokerId()), config.zone(), advertisedEndpoint);
        acceptingWrites.set(true);
        log.info("broker {} re-registered at epoch {}", config.brokerId(), epoch);
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
        if (pipeline != null) {
            pipeline.close();
            pipeline = null;
        }
        if (hwListener != null) {
            hwListener.close();
            hwListener = null;
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
