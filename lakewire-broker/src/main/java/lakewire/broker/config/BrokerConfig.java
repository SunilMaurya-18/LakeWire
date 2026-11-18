package lakewire.broker.config;

import java.nio.file.Path;
import java.util.Map;

public record BrokerConfig(
    String host,
    int port,
    int maxFrameBytes,
    int brokerId,
    String zone,
    String advertisedHost,
    int advertisedPort,
    Path dataDir
) {

    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 9092;
    public static final int DEFAULT_MAX_FRAME_BYTES = 100 * 1024 * 1024;
    public static final int MIN_FRAME_BYTES = 1024;
    public static final String DEFAULT_ZONE = "local";
    public static final String DEFAULT_ADVERTISED_HOST = "localhost";

    public BrokerConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        if (maxFrameBytes < MIN_FRAME_BYTES) {
            throw new IllegalArgumentException("maxFrameBytes must be at least " + MIN_FRAME_BYTES);
        }
        if (brokerId <= 0) {
            throw new IllegalArgumentException("brokerId must be a positive integer");
        }
        if (zone == null || zone.isBlank()) {
            throw new IllegalArgumentException("zone must not be blank");
        }
        if (advertisedHost == null || advertisedHost.isBlank()) {
            throw new IllegalArgumentException("advertisedHost must not be blank");
        }
        if (advertisedPort < 0 || advertisedPort > 65535) {
            throw new IllegalArgumentException("advertisedPort must be between 0 and 65535");
        }
        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir must not be null");
        }
    }

    /** Lesson 5 tests bind an ephemeral port and do not open Postgres. */
    public BrokerConfig(String host, int port, int maxFrameBytes) {
        this(host, port, maxFrameBytes, 1, DEFAULT_ZONE, host, port == 0 ? 0 : port, Path.of("data"));
    }

    public static BrokerConfig fromEnvironment(Map<String, String> env) {
        String host = valueOr(env, "LAKEWIRE_HOST", DEFAULT_HOST);
        int port = parseInt(env, "LAKEWIRE_PORT", DEFAULT_PORT);
        int maxFrameBytes = parseInt(env, "LAKEWIRE_MAX_FRAME_BYTES", DEFAULT_MAX_FRAME_BYTES);
        int brokerId = parseInt(env, "LAKEWIRE_BROKER_ID", 1);
        String zone = valueOr(env, "LAKEWIRE_ZONE", DEFAULT_ZONE);
        String advertisedHost = valueOr(env, "LAKEWIRE_ADVERTISED_HOST", DEFAULT_ADVERTISED_HOST);
        int advertisedPort = parseInt(env, "LAKEWIRE_ADVERTISED_PORT", port);
        Path dataDir = Path.of(valueOr(env, "LAKEWIRE_DATA_DIR", "data"));
        return new BrokerConfig(host, port, maxFrameBytes, brokerId, zone, advertisedHost, advertisedPort, dataDir);
    }

    private static String valueOr(Map<String, String> env, String variable, String defaultValue) {
        String raw = env.get(variable);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return raw.trim();
    }

    private static int parseInt(Map<String, String> env, String variable, int defaultValue) {
        String raw = env.get(variable);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException cause) {
            throw new IllegalArgumentException("Invalid integer for " + variable, cause);
        }
    }
}
