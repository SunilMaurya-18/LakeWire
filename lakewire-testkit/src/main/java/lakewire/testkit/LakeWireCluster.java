package lakewire.testkit;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Postgres, SeaweedFS, Toxiproxy in front of both, and N broker containers.
 * Brokers advertise {@code 127.0.0.1:<hostPort>} so a client on the host can reach every one.
 */
public final class LakeWireCluster implements AutoCloseable {

    public static final String BUCKET = "lakewire";
    private static final int BROKER_PORT = 9092;
    private static final Duration START_TIMEOUT = Duration.ofMinutes(8);
    private static final String IMAGE = "lakewire-broker-it:0.1.0";
    private static volatile boolean imageReady;

    private final Network network;
    private final PostgreSQLContainer postgres;
    private final GenericContainer<?> seaweed;
    private final ToxiproxyContainer toxiproxy;
    private final Proxy pgProxy;
    private final Proxy s3Proxy;
    private final List<GenericContainer<?>> brokers;
    private final List<Integer> hostPorts;

    private LakeWireCluster(
        Network network,
        PostgreSQLContainer postgres,
        GenericContainer<?> seaweed,
        ToxiproxyContainer toxiproxy,
        Proxy pgProxy,
        Proxy s3Proxy,
        List<GenericContainer<?>> brokers,
        List<Integer> hostPorts
    ) {
        this.network = network;
        this.postgres = postgres;
        this.seaweed = seaweed;
        this.toxiproxy = toxiproxy;
        this.pgProxy = pgProxy;
        this.s3Proxy = s3Proxy;
        this.brokers = brokers;
        this.hostPorts = hostPorts;
    }

    public static LakeWireCluster start(int brokers) {
        return start(brokers, Map.of());
    }

    public static LakeWireCluster start(int brokers, Map<String, String> extraEnv) {
        if (brokers < 1) {
            throw new IllegalArgumentException("brokers must be positive");
        }
        requireDocker();
        ensureImage();
        Network network = Network.newNetwork();
        PostgreSQLContainer postgres = null;
        GenericContainer<?> seaweed = null;
        ToxiproxyContainer toxiproxy = null;
        List<GenericContainer<?>> nodes = new ArrayList<>();
        try {
            postgres = LakeWireContainers.postgres().withNetwork(network).withNetworkAliases("postgres");
            seaweed = LakeWireContainers.seaweedFs().withNetwork(network).withNetworkAliases("seaweed");
            toxiproxy = LakeWireContainers.toxiproxy().withNetwork(network).withNetworkAliases("toxiproxy");
            Startables.deepStart(postgres, seaweed, toxiproxy).get(START_TIMEOUT.toMinutes(), TimeUnit.MINUTES);
            createBucketInside(seaweed);
            ToxiproxyClient control = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            Proxy pg = control.createProxy("pg", "0.0.0.0:8666", "postgres:5432");
            Proxy s3 = control.createProxy("s3", "0.0.0.0:8667", "seaweed:" + LakeWireContainers.SEAWEEDFS_S3_PORT);
            String jdbc = "jdbc:postgresql://toxiproxy:8666/" + postgres.getDatabaseName()
                + "?connectTimeout=30&socketTimeout=30";
            List<Integer> ports = new ArrayList<>();
            for (int i = 0; i < brokers; i++) {
                int hostPort = freePort();
                ports.add(hostPort);
                nodes.add(broker(i, hostPort, jdbc, postgres.getUsername(), postgres.getPassword(), extraEnv, network));
            }
            nodes.get(0).start();
            if (nodes.size() > 1) {
                Startables.deepStart(nodes.subList(1, nodes.size())).get(20, TimeUnit.MINUTES);
            }
            return new LakeWireCluster(
                network, postgres, seaweed, toxiproxy, pg, s3, List.copyOf(nodes), List.copyOf(ports));
        } catch (Exception failure) {
            nodes.forEach(LakeWireCluster::stopQuietly);
            stopQuietly(toxiproxy);
            stopQuietly(seaweed);
            stopQuietly(postgres);
            closeQuietly(network);
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException("cluster failed to start", failure);
        }
    }

    public String bootstrap(int index) {
        return "127.0.0.1:" + hostPorts.get(index);
    }

    public String bootstrapAll() {
        StringBuilder joined = new StringBuilder();
        for (int i = 0; i < hostPorts.size(); i++) {
            if (i > 0) {
                joined.append(',');
            }
            joined.append(bootstrap(i));
        }
        return joined.toString();
    }

    /** Kafka node id. Broker 0 is id 1 in zone {@code a}. */
    public int brokerId(int index) {
        return index + 1;
    }

    public String zone(int index) {
        return zoneName(index);
    }

    public String jdbcUrl() {
        return postgres.getJdbcUrl();
    }

    public String user() {
        return postgres.getUsername();
    }

    public String password() {
        return postgres.getPassword();
    }

    public Proxy pgProxy() {
        return pgProxy;
    }

    public Proxy s3Proxy() {
        return s3Proxy;
    }

    public String s3Endpoint() {
        return "http://" + seaweed.getHost() + ":" + seaweed.getMappedPort(LakeWireContainers.SEAWEEDFS_S3_PORT);
    }

    public int brokerCount() {
        return brokers.size();
    }

    public void pause(int index) {
        GenericContainer<?> broker = brokers.get(index);
        broker.getDockerClient().pauseContainerCmd(broker.getContainerId()).exec();
    }

    public void unpause(int index) {
        GenericContainer<?> broker = brokers.get(index);
        broker.getDockerClient().unpauseContainerCmd(broker.getContainerId()).exec();
    }

    public void kill(int index) {
        GenericContainer<?> broker = brokers.get(index);
        broker.getDockerClient().killContainerCmd(broker.getContainerId()).withSignal("KILL").exec();
    }

    public void startBroker(int index) {
        GenericContainer<?> broker = brokers.get(index);
        broker.getDockerClient().startContainerCmd(broker.getContainerId()).exec();
        waitForPort(hostPorts.get(index), Duration.ofMinutes(2));
    }

    public int hostPort(int index) {
        return hostPorts.get(index);
    }

    public void restartPostgres() {
        postgres.getDockerClient().restartContainerCmd(postgres.getContainerId()).exec();
        Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)).waitUntilReady(postgres);
    }

    @Override
    public void close() {
        brokers.forEach(LakeWireCluster::stopQuietly);
        stopQuietly(toxiproxy);
        stopQuietly(seaweed);
        stopQuietly(postgres);
        closeQuietly(network);
    }

    private static GenericContainer<?> broker(
        int index,
        int hostPort,
        String jdbc,
        String user,
        String password,
        Map<String, String> extraEnv,
        Network network
    ) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("LAKEWIRE_BROKER_ID", Integer.toString(index + 1));
        env.put("LAKEWIRE_ZONE", zoneName(index));
        env.put("LAKEWIRE_HOST", "0.0.0.0");
        env.put("LAKEWIRE_PORT", Integer.toString(BROKER_PORT));
        env.put("LAKEWIRE_ADVERTISED_HOST", "127.0.0.1");
        env.put("LAKEWIRE_ADVERTISED_PORT", Integer.toString(hostPort));
        env.put("LAKEWIRE_PG_URL", jdbc);
        env.put("LAKEWIRE_PG_USER", user);
        env.put("LAKEWIRE_PG_PASSWORD", password);
        env.put("LAKEWIRE_STORAGE", "s3");
        env.put("LAKEWIRE_S3_ENDPOINT", "http://seaweed:" + LakeWireContainers.SEAWEEDFS_S3_PORT);
        env.put("LAKEWIRE_S3_BUCKET", BUCKET);
        env.put("LAKEWIRE_S3_ACCESS_KEY", "lakewire");
        env.put("LAKEWIRE_S3_SECRET_KEY", "lakewire");
        env.put("LAKEWIRE_S3_PATH_STYLE", "true");
        env.putAll(extraEnv);
        return new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withImagePullPolicy(name -> false)
            .withNetwork(network)
            .withEnv(env)
            .withExposedPorts(BROKER_PORT)
            .withCreateContainerCmdModifier(cmd -> {
                HostConfig host = cmd.getHostConfig() == null ? HostConfig.newHostConfig() : cmd.getHostConfig();
                host.withPortBindings(new PortBinding(Ports.Binding.bindPort(hostPort), new ExposedPort(BROKER_PORT)));
                cmd.withHostConfig(host);
            })
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("lakewire.broker." + (index + 1))))
            .waitingFor(Wait.forLogMessage(".*advertising 127\\.0\\.0\\.1:" + hostPort + ".*", 1)
                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    private static String zoneName(int index) {
        return Character.toString((char) ('a' + index));
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void waitForPort(int port, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port), 500);
                return;
            } catch (IOException ignored) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted waiting for broker port " + port, interrupted);
                }
            }
        }
        throw new IllegalStateException("broker port " + port + " did not accept connections");
    }

    /** Create the bucket from inside the SeaweedFS container so Docker port-mapping EOF is avoided. */
    private static void createBucketInside(GenericContainer<?> seaweed) throws InterruptedException {
        IOException last = null;
        for (int attempt = 1; attempt <= 40; attempt++) {
            try {
                var result = seaweed.execInContainer(
                    "curl",
                    "-fsS",
                    "-X",
                    "PUT",
                    "http://127.0.0.1:" + LakeWireContainers.SEAWEEDFS_S3_PORT + "/" + BUCKET
                );
                if (result.getExitCode() == 0) {
                    return;
                }
            } catch (IOException | UnsupportedOperationException failure) {
                last = failure instanceof IOException io ? io : new IOException(failure);
            }
            Thread.sleep(250);
        }
        if (last != null) {
            throw new IllegalStateException("could not create S3 bucket inside SeaweedFS", last);
        }
        throw new IllegalStateException("could not create S3 bucket inside SeaweedFS");
    }

    /**
     * ImageFromDockerfile uploads the context through a 1 KiB pipe. On Windows that pipe fills
     * and the client never finishes, even after the daemon has built the image (A41).
     */
    private static void ensureImage() {
        if (imageReady) {
            return;
        }
        synchronized (LakeWireCluster.class) {
            if (imageReady) {
                return;
            }
            Path root = repoRoot();
            ProcessBuilder builder = new ProcessBuilder("docker", "build", "-t", IMAGE, ".");
            builder.directory(root.toFile());
            builder.redirectErrorStream(true);
            try {
                Process process = builder.start();
                try (var output = process.getInputStream()) {
                    output.transferTo(System.out);
                }
                int code = process.waitFor();
                if (code != 0) {
                    throw new IllegalStateException("docker build of the broker image failed with exit " + code);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("docker build interrupted", interrupted);
            } catch (IOException failure) {
                throw new IllegalStateException("docker build failed", failure);
            }
            imageReady = true;
        }
    }

    private static Path repoRoot() {
        Path dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null) {
            if (Files.isRegularFile(dir.resolve("mvnw")) && Files.isRegularFile(dir.resolve("Dockerfile"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("repo root not found from " + System.getProperty("user.dir"));
    }

    private static void requireDocker() {
        try {
            if (!DockerClientFactory.instance().isDockerAvailable()) {
                throw new IllegalStateException("Docker is required for LakeWireCluster. Start Docker Desktop and rerun.");
            }
        } catch (IllegalStateException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new IllegalStateException("Docker is required for LakeWireCluster. Start Docker Desktop and rerun.", failure);
        }
    }

    private static void stopQuietly(Startable container) {
        if (container == null) {
            return;
        }
        try {
            container.stop();
        } catch (RuntimeException ignored) {
            // the original failure is the one that matters
        }
    }

    private static void closeQuietly(Network network) {
        try {
            network.close();
        } catch (RuntimeException ignored) {
            // already closed, or it never came up
        }
    }
}
