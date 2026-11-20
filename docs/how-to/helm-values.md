# Helm values sketch

There is no Helm chart in v0.1 (`deploy/compose` only). This page is a design sketch of the `values.yaml` a future
chart could expose. Every key maps to a `LAKEWIRE_*` variable in [Config](../reference/config.md); defaults below
match that table. Nothing here is supported or tested yet.

Credentials never go in `values.yaml`. Reference Kubernetes Secrets instead (`LAKEWIRE_PG_PASSWORD`,
`LAKEWIRE_S3_ACCESS_KEY`, `LAKEWIRE_S3_SECRET_KEY`).

```yaml
broker:
  id: 1                      # LAKEWIRE_BROKER_ID (positive int)
  host: 0.0.0.0              # LAKEWIRE_HOST
  port: 9092                 # LAKEWIRE_PORT
  advertisedHost: localhost  # LAKEWIRE_ADVERTISED_HOST
  advertisedPort: 0          # LAKEWIRE_ADVERTISED_PORT (0 = bound port)
  zone: local                # LAKEWIRE_ZONE
  metricsPort: 9094          # LAKEWIRE_METRICS_PORT
  leaseMs: 10000             # LAKEWIRE_LEASE_MS
  heartbeatMs: 2000          # LAKEWIRE_HEARTBEAT_MS
  groupCommit: false         # LAKEWIRE_GROUP_COMMIT
  # maxFrameBytes:           # LAKEWIRE_MAX_FRAME_BYTES (broker default)

postgres:
  url: ""                    # LAKEWIRE_PG_URL (required, JDBC)
  user: ""                   # LAKEWIRE_PG_USER (required)
  passwordSecret:            # LAKEWIRE_PG_PASSWORD (required)
    name: lakewire-pg
    key: password
  migrate: true              # LAKEWIRE_MIGRATE (Flyway migrate vs validate)

storage:
  type: s3                   # LAKEWIRE_STORAGE (fs or s3)
  dataDir: data              # LAKEWIRE_DATA_DIR (fs only)
  objectPrefix: ""           # LAKEWIRE_OBJECT_PREFIX
  s3:
    bucket: ""               # LAKEWIRE_S3_BUCKET (required if s3)
    region: us-east-1        # LAKEWIRE_S3_REGION
    endpoint: ""             # LAKEWIRE_S3_ENDPOINT (empty = AWS)
    pathStyle: true          # LAKEWIRE_S3_PATH_STYLE
    conditionalPut: true     # LAKEWIRE_S3_CONDITIONAL_PUT
    credentialsSecret:       # LAKEWIRE_S3_ACCESS_KEY / LAKEWIRE_S3_SECRET_KEY (both or neither)
      name: lakewire-s3

tuning:
  lingerMs: 200              # LAKEWIRE_LINGER_MS
  objectMaxBytes: 8388608    # LAKEWIRE_OBJECT_MAX_BYTES (8 MiB)
  bufferMaxBytes: 67108864   # LAKEWIRE_BUFFER_MAX_BYTES (64 MiB)
  uploadConcurrency: 4       # LAKEWIRE_UPLOAD_CONCURRENCY

workers:
  enabled: true              # LAKEWIRE_WORKERS (GC, retention, audit)
  orphanTtlMs: 600000        # LAKEWIRE_ORPHAN_TTL_MS (10 min)
  gcPeriodMs: 60000          # LAKEWIRE_GC_PERIOD_MS (1 min)
  retentionPeriodMs: 60000   # LAKEWIRE_RETENTION_PERIOD_MS (1 min)
  deleteSweepMs: 30000       # LAKEWIRE_DELETE_SWEEP_MS (30 s)
  refAuditMs: 300000         # LAKEWIRE_REF_AUDIT_MS (5 min)
  auditHeadCap: 100          # LAKEWIRE_AUDIT_HEAD_CAP
  producerTtlMs: 604800000   # LAKEWIRE_PRODUCER_TTL_MS (7 days)
  producerExpiryPeriodMs: 3600000  # LAKEWIRE_PRODUCER_EXPIRY_PERIOD_MS (1 h)
  graceMs: 300000            # LAKEWIRE_GRACE_MS (5 min)
  # lockRetryMs:             # LAKEWIRE_WORKER_LOCK_RETRY_MS

admin:
  port: 8080                 # LAKEWIRE_ADMIN_PORT (admin only)
  migrate: false             # LAKEWIRE_MIGRATE for the admin deployment
```

Note: `advertisedHost` is what the broker returns in Metadata, so set it to the address clients can reach.
