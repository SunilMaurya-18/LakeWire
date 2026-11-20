package lakewire.cli;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lakewire.broker.BrokerRuntime;
import lakewire.broker.config.BrokerConfig;
import lakewire.broker.write.WriteSettings;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.storage.LocalFsObjectStore;
import lakewire.workers.WorkerConfig;

/** Embedded Postgres + local FS + one broker. */
public final class DevServer implements AutoCloseable {

    private final EmbeddedPostgres postgres;
    private final BrokerRuntime runtime;
    private final long readyMs;

    private DevServer(EmbeddedPostgres postgres, BrokerRuntime runtime, long readyMs) {
        this.postgres = postgres;
        this.runtime = runtime;
        this.readyMs = readyMs;
    }

    public static Path defaultHome() {
        String home = System.getProperty("user.home");
        if (home == null || home.isBlank()) {
            home = ".";
        }
        return Path.of(home, ".lakewire", "dev");
    }

    public static DevServer start(Path home, int kafkaPort, int metricsPort) {
        long begin = System.nanoTime();
        try {
            Files.createDirectories(home);
            Path pgDir = home.resolve("pg");
            Path dataDir = home.resolve("data");
            Files.createDirectories(pgDir);
            Files.createDirectories(dataDir);
            EmbeddedPostgres postgres = EmbeddedPostgres.builder()
                .setDataDirectory(pgDir)
                .setCleanDataDirectory(false)
                .setRegisterShutdownHook(false)
                .setServerConfig("synchronous_commit", "on")
                .start();
            try {
                CoordinatorConfig coordinator = new CoordinatorConfig(
                    postgres.getJdbcUrl("postgres", "postgres"),
                    "postgres",
                    "",
                    true,
                    CoordinatorConfig.DEFAULT_LEASE_MS,
                    CoordinatorConfig.DEFAULT_HEARTBEAT_MS,
                    CoordinatorConfig.DEFAULT_GROUP_COMMIT
                );
                BrokerConfig config = new BrokerConfig(
                    "127.0.0.1",
                    kafkaPort,
                    BrokerConfig.DEFAULT_MAX_FRAME_BYTES,
                    1,
                    "local",
                    "localhost",
                    kafkaPort,
                    dataDir,
                    metricsPort
                );
                BrokerRuntime runtime = new BrokerRuntime(
                    config,
                    coordinator,
                    new LocalFsObjectStore(dataDir),
                    WriteSettings.defaults(),
                    WorkerConfig.defaults()
                );
                runtime.start();
                long readyMs = (System.nanoTime() - begin) / 1_000_000L;
                return new DevServer(postgres, runtime, readyMs);
            } catch (RuntimeException failure) {
                postgres.close();
                throw failure;
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("lakewire dev failed to start Postgres", failure);
        }
    }

    public int port() {
        return runtime.port();
    }

    public long readyMs() {
        return readyMs;
    }

    @Override
    public void close() {
        runtime.close();
        try {
            postgres.close();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }
}
