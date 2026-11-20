# Config (`LAKEWIRE_*`)

No secrets in git. Local values in a gitignored `.env`. `.env.example` has placeholders.

| Variable | Default | Used by |
| --- | --- | --- |
| `LAKEWIRE_HOST` | `0.0.0.0` | broker bind |
| `LAKEWIRE_PORT` | `9092` | Kafka socket |
| `LAKEWIRE_ADVERTISED_HOST` | `localhost` | Metadata |
| `LAKEWIRE_ADVERTISED_PORT` | bind port | `0` = bound port (A33) |
| `LAKEWIRE_BROKER_ID` | `1` | Kafka node id (positive int, A3) |
| `LAKEWIRE_ZONE` | `local` | rack / zone |
| `LAKEWIRE_MAX_FRAME_BYTES` | broker default | max request |
| `LAKEWIRE_METRICS_PORT` | `9094` | `/metrics` (A18; `0` ephemeral, A52) |
| `LAKEWIRE_DATA_DIR` | `data` | local FS store |
| `LAKEWIRE_STORAGE` | `fs` | `fs` or `s3` |
| `LAKEWIRE_PG_URL` | required | JDBC |
| `LAKEWIRE_PG_USER` / `LAKEWIRE_PG_PASSWORD` | required | JDBC |
| `LAKEWIRE_MIGRATE` | `true` | Flyway migrate vs validate (admin false, A22) |
| `LAKEWIRE_LEASE_MS` | `10000` | broker lease |
| `LAKEWIRE_HEARTBEAT_MS` | `2000` | heartbeat |
| `LAKEWIRE_GROUP_COMMIT` | `false` | ADR-001 |
| `LAKEWIRE_S3_BUCKET` | required if s3 | |
| `LAKEWIRE_S3_REGION` | `us-east-1` | |
| `LAKEWIRE_S3_ENDPOINT` | AWS | path-style stores |
| `LAKEWIRE_S3_PATH_STYLE` | `true` | |
| `LAKEWIRE_S3_CONDITIONAL_PUT` | `true` | |
| `LAKEWIRE_S3_ACCESS_KEY` / `LAKEWIRE_S3_SECRET_KEY` | chain | both or neither (A31) |
| `LAKEWIRE_OBJECT_PREFIX` | empty | object keys |
| `LAKEWIRE_LINGER_MS` | `200` | |
| `LAKEWIRE_OBJECT_MAX_BYTES` | 8 MiB | |
| `LAKEWIRE_BUFFER_MAX_BYTES` | 64 MiB | |
| `LAKEWIRE_UPLOAD_CONCURRENCY` | `4` | |
| `LAKEWIRE_WORKERS` | `true` | GC/retention/audit |
| `LAKEWIRE_ORPHAN_TTL_MS` | 10 min | A17 |
| `LAKEWIRE_GC_PERIOD_MS` | 1 min | |
| `LAKEWIRE_RETENTION_PERIOD_MS` | 1 min | |
| `LAKEWIRE_DELETE_SWEEP_MS` | 30 s | |
| `LAKEWIRE_REF_AUDIT_MS` | 5 min | |
| `LAKEWIRE_AUDIT_HEAD_CAP` | `100` | |
| `LAKEWIRE_PRODUCER_TTL_MS` | 7 days | |
| `LAKEWIRE_PRODUCER_EXPIRY_PERIOD_MS` | 1 h | |
| `LAKEWIRE_GRACE_MS` | 5 min | |
| `LAKEWIRE_WORKER_LOCK_RETRY_MS` | A46 | |
| `LAKEWIRE_ADMIN_PORT` | `8080` | admin only (A58) |
