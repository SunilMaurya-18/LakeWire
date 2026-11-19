package lakewire.workers;

import java.util.Map;

/** Periods and TTLs from A17. Env names in A46. */
public record WorkerConfig(
    boolean enabled,
    long orphanTtlMs,
    long gcPeriodMs,
    long retentionPeriodMs,
    long deleteSweepMs,
    long refAuditMs,
    int auditHeadCap,
    long producerTtlMs,
    long producerExpiryPeriodMs,
    long graceMs,
    long lockRetryMs,
    String objectPrefix
) {

    public static final long LOCK_KEY = 87_001_108L;
    public static final long DEFAULT_ORPHAN_TTL_MS = 10L * 60_000;
    public static final long DEFAULT_GC_PERIOD_MS = 60_000;
    public static final long DEFAULT_DELETE_SWEEP_MS = 30_000;
    public static final long DEFAULT_REF_AUDIT_MS = 5L * 60_000;
    public static final int DEFAULT_AUDIT_HEAD_CAP = 100;
    public static final long DEFAULT_PRODUCER_TTL_MS = 7L * 24 * 60 * 60 * 1000;
    public static final long DEFAULT_PRODUCER_EXPIRY_PERIOD_MS = 60L * 60_000;
    public static final long DEFAULT_GRACE_MS = 5L * 60_000;
    public static final long DEFAULT_LOCK_RETRY_MS = 1_000;

    public WorkerConfig {
        if (orphanTtlMs < 0 || gcPeriodMs <= 0 || retentionPeriodMs <= 0 || deleteSweepMs <= 0
            || refAuditMs <= 0 || producerTtlMs < 0 || producerExpiryPeriodMs <= 0
            || graceMs < 0 || lockRetryMs <= 0) {
            throw new IllegalArgumentException("worker periods must be positive and ttls must be >= 0");
        }
        if (auditHeadCap < 0) {
            throw new IllegalArgumentException("auditHeadCap must be >= 0");
        }
        if (objectPrefix == null) {
            objectPrefix = "";
        }
    }

    public static WorkerConfig defaults() {
        return new WorkerConfig(
            true,
            DEFAULT_ORPHAN_TTL_MS,
            DEFAULT_GC_PERIOD_MS,
            DEFAULT_GC_PERIOD_MS,
            DEFAULT_DELETE_SWEEP_MS,
            DEFAULT_REF_AUDIT_MS,
            DEFAULT_AUDIT_HEAD_CAP,
            DEFAULT_PRODUCER_TTL_MS,
            DEFAULT_PRODUCER_EXPIRY_PERIOD_MS,
            DEFAULT_GRACE_MS,
            DEFAULT_LOCK_RETRY_MS,
            ""
        );
    }

    public static WorkerConfig disabled() {
        WorkerConfig base = defaults();
        return new WorkerConfig(
            false,
            base.orphanTtlMs(),
            base.gcPeriodMs(),
            base.retentionPeriodMs(),
            base.deleteSweepMs(),
            base.refAuditMs(),
            base.auditHeadCap(),
            base.producerTtlMs(),
            base.producerExpiryPeriodMs(),
            base.graceMs(),
            base.lockRetryMs(),
            base.objectPrefix()
        );
    }

    public static WorkerConfig fromEnvironment(Map<String, String> env) {
        WorkerConfig base = defaults();
        String prefix = env.get("LAKEWIRE_OBJECT_PREFIX");
        return new WorkerConfig(
            parseBoolean(env, "LAKEWIRE_WORKERS", true),
            parseLong(env, "LAKEWIRE_ORPHAN_TTL_MS", base.orphanTtlMs()),
            parseLong(env, "LAKEWIRE_GC_PERIOD_MS", base.gcPeriodMs()),
            parseLong(env, "LAKEWIRE_RETENTION_PERIOD_MS", base.retentionPeriodMs()),
            parseLong(env, "LAKEWIRE_DELETE_SWEEP_MS", base.deleteSweepMs()),
            parseLong(env, "LAKEWIRE_REF_AUDIT_MS", base.refAuditMs()),
            (int) parseLong(env, "LAKEWIRE_AUDIT_HEAD_CAP", base.auditHeadCap()),
            parseLong(env, "LAKEWIRE_PRODUCER_TTL_MS", base.producerTtlMs()),
            parseLong(env, "LAKEWIRE_PRODUCER_EXPIRY_PERIOD_MS", base.producerExpiryPeriodMs()),
            parseLong(env, "LAKEWIRE_GRACE_MS", base.graceMs()),
            parseLong(env, "LAKEWIRE_WORKER_LOCK_RETRY_MS", base.lockRetryMs()),
            prefix == null ? "" : prefix.trim()
        );
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
