# Architecture

Brokers are diskless. Record batches (Kafka RecordBatch v2) are stored verbatim in S3 (or local FS) as **LKW1** objects. Postgres holds topics, offsets, producer epochs, leases and object refcounts.

1. Produce: buffer until linger / size → conditional PUT → `sl_commit_object` → ack.
2. Fetch: Metadata leader is a **routing hint**. Any live broker can serve any partition. Visible = committed HW from Postgres.
3. Workers: orphan GC, retention, DELETE_PENDING sweep, I7/I8 audit. One worker holds `pg_try_advisory_lock(87001108)` (A43).

See PRD §7 and [ADRs](adrs.md).
