# S3 providers

Set `LAKEWIRE_STORAGE=s3`. Credentials: both `LAKEWIRE_S3_ACCESS_KEY` and `LAKEWIRE_S3_SECRET_KEY`, or the default AWS chain (A31).

| Store | Endpoint | Notes |
| --- | --- | --- |
| AWS S3 | omit `LAKEWIRE_S3_ENDPOINT` | Region `LAKEWIRE_S3_REGION` (default `us-east-1`). Path-style default true; set `LAKEWIRE_S3_PATH_STYLE=false` for virtual-host. |
| SeaweedFS | `http://host:8333` | Compose default. Identity in `.env.example`. |
| MinIO-compatible (`pgsty/silo`) | container S3 port | Testcontainers image; `minio/minio` is gone (A1). |

Conditional PUT (`If-None-Match: *`) is required for fencing. If the probe fails, the broker logs and falls back to HEAD-before-PUT (A42). Set `LAKEWIRE_S3_CONDITIONAL_PUT=false` only for stores that reject the header.

Bucket must already exist (`LAKEWIRE_S3_BUCKET`). Compose creates it via `seaweed-bucket-init`.
