package lakewire.broker.config;

import java.util.Map;

public record BrokerConfig(String host, int port, int maxFrameBytes) {

    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 9092;
    public static final int DEFAULT_MAX_FRAME_BYTES = 100 * 1024 * 1024;
    public static final int MIN_FRAME_BYTES = 1024;

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
    }

    public static BrokerConfig fromEnvironment(Map<String, String> env) {
        String host = env.get("LAKEWIRE_HOST");
        if (host == null || host.isBlank()) {
            host = DEFAULT_HOST;
        }
        int port = parseInt(env, "LAKEWIRE_PORT", DEFAULT_PORT);
        int maxFrameBytes = parseInt(env, "LAKEWIRE_MAX_FRAME_BYTES", DEFAULT_MAX_FRAME_BYTES);
        return new BrokerConfig(host, port, maxFrameBytes);
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
