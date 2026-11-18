package lakewire.storage;

import java.net.URI;
import java.util.Map;

public record S3StorageConfig(
    String bucket,
    String region,
    URI endpoint,
    boolean pathStyle,
    boolean conditionalPut,
    String accessKey,
    String secretKey
) {

    public static final String DEFAULT_REGION = "us-east-1";

    public S3StorageConfig {
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("bucket must not be blank");
        }
        if (region == null || region.isBlank()) {
            throw new IllegalArgumentException("region must not be blank");
        }
        boolean keySet = accessKey != null && !accessKey.isBlank();
        boolean secretSet = secretKey != null && !secretKey.isBlank();
        if (keySet != secretSet) {
            throw new IllegalArgumentException("LAKEWIRE_S3_ACCESS_KEY and LAKEWIRE_S3_SECRET_KEY must be set together");
        }
    }

    public static S3StorageConfig fromEnvironment(Map<String, String> env) {
        String accessKey = blankToNull(env.get("LAKEWIRE_S3_ACCESS_KEY"));
        String secretKey = blankToNull(env.get("LAKEWIRE_S3_SECRET_KEY"));
        String endpoint = blankToNull(env.get("LAKEWIRE_S3_ENDPOINT"));
        return new S3StorageConfig(
            required(env, "LAKEWIRE_S3_BUCKET"),
            valueOr(env, "LAKEWIRE_S3_REGION", DEFAULT_REGION),
            endpoint == null ? null : URI.create(endpoint),
            parseBoolean(env, "LAKEWIRE_S3_PATH_STYLE", true),
            parseBoolean(env, "LAKEWIRE_S3_CONDITIONAL_PUT", true),
            accessKey,
            secretKey
        );
    }

    private static String required(Map<String, String> env, String variable) {
        String value = env.get(variable);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(variable + " must be set");
        }
        return value.trim();
    }

    private static String valueOr(Map<String, String> env, String variable, String defaultValue) {
        String value = blankToNull(env.get(variable));
        return value == null ? defaultValue : value;
    }

    private static String blankToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
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
}
