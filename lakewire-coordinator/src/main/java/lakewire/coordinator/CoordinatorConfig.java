package lakewire.coordinator;

import java.util.Map;

public record CoordinatorConfig(
    String jdbcUrl,
    String username,
    String password,
    boolean migrate,
    long leaseMs,
    long heartbeatMs,
    boolean groupCommit
) {

    public static final long DEFAULT_LEASE_MS = 10_000;
    public static final long DEFAULT_HEARTBEAT_MS = 2_000;

    /** ADR-001 sets this. The lab measures group commit before the default is treated as final. */
    public static final boolean DEFAULT_GROUP_COMMIT = false;

    public CoordinatorConfig {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("jdbcUrl must not be blank");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("username must not be blank");
        }
        if (password == null) {
            throw new IllegalArgumentException("password must not be null");
        }
        if (leaseMs <= 0) {
            throw new IllegalArgumentException("leaseMs must be positive");
        }
        if (heartbeatMs <= 0) {
            throw new IllegalArgumentException("heartbeatMs must be positive");
        }
    }

    public static CoordinatorConfig of(String jdbcUrl, String username, String password) {
        return new CoordinatorConfig(
            jdbcUrl,
            username,
            password,
            true,
            DEFAULT_LEASE_MS,
            DEFAULT_HEARTBEAT_MS,
            DEFAULT_GROUP_COMMIT
        );
    }

    public static CoordinatorConfig fromEnvironment(Map<String, String> env) {
        return new CoordinatorConfig(
            required(env, "LAKEWIRE_PG_URL"),
            required(env, "LAKEWIRE_PG_USER"),
            required(env, "LAKEWIRE_PG_PASSWORD"),
            parseBoolean(env, "LAKEWIRE_MIGRATE", true),
            parseLong(env, "LAKEWIRE_LEASE_MS", DEFAULT_LEASE_MS),
            parseLong(env, "LAKEWIRE_HEARTBEAT_MS", DEFAULT_HEARTBEAT_MS),
            parseBoolean(env, "LAKEWIRE_GROUP_COMMIT", DEFAULT_GROUP_COMMIT)
        );
    }

    private static String required(Map<String, String> env, String variable) {
        String value = env.get(variable);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(variable + " must be set");
        }
        return value.trim();
    }

    private static boolean parseBoolean(Map<String, String> env, String variable, boolean defaultValue) {
        String raw = env.get(variable);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return switch (raw.trim()) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException(variable + " must be true or false");
        };
    }

    private static long parseLong(Map<String, String> env, String variable, long defaultValue) {
        String raw = env.get(variable);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException cause) {
            throw new IllegalArgumentException("Invalid integer for " + variable, cause);
        }
    }
}
