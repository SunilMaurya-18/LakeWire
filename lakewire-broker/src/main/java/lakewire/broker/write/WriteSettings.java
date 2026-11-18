package lakewire.broker.write;

import java.util.Map;

public record WriteSettings(
    int lingerMs,
    int objectMaxBytes,
    long bufferMaxBytes,
    int uploadConcurrency,
    String objectPrefix
) {

    public static final int DEFAULT_LINGER_MS = 200;
    public static final int DEFAULT_OBJECT_MAX_BYTES = 8 * 1024 * 1024;
    public static final long DEFAULT_BUFFER_MAX_BYTES = 64L * 1024 * 1024;
    public static final int DEFAULT_UPLOAD_CONCURRENCY = 4;

    public WriteSettings {
        if (lingerMs < 0) {
            throw new IllegalArgumentException("lingerMs must be >= 0");
        }
        if (objectMaxBytes < 1) {
            throw new IllegalArgumentException("objectMaxBytes must be positive");
        }
        if (bufferMaxBytes < objectMaxBytes) {
            throw new IllegalArgumentException("bufferMaxBytes must be at least objectMaxBytes");
        }
        if (uploadConcurrency < 1) {
            throw new IllegalArgumentException("uploadConcurrency must be positive");
        }
        if (objectPrefix == null) {
            throw new IllegalArgumentException("objectPrefix must not be null");
        }
    }

    public static WriteSettings defaults() {
        return new WriteSettings(
            DEFAULT_LINGER_MS,
            DEFAULT_OBJECT_MAX_BYTES,
            DEFAULT_BUFFER_MAX_BYTES,
            DEFAULT_UPLOAD_CONCURRENCY,
            ""
        );
    }

    public static WriteSettings fromEnvironment(Map<String, String> env) {
        String prefix = env.get("LAKEWIRE_OBJECT_PREFIX");
        return new WriteSettings(
            parseInt(env, "LAKEWIRE_LINGER_MS", DEFAULT_LINGER_MS),
            parseInt(env, "LAKEWIRE_OBJECT_MAX_BYTES", DEFAULT_OBJECT_MAX_BYTES),
            parseLong(env, "LAKEWIRE_BUFFER_MAX_BYTES", DEFAULT_BUFFER_MAX_BYTES),
            parseInt(env, "LAKEWIRE_UPLOAD_CONCURRENCY", DEFAULT_UPLOAD_CONCURRENCY),
            prefix == null ? "" : prefix.trim()
        );
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
