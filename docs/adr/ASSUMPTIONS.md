# Assumptions

Where the PRD is silent or outdated. A1–A24 are copied from `docs/prd/LakeWire-Phase1-Cursor-Prompts.md` Part 5. A25 was added in M0.

| Id | Decision | Reason |
| --- | --- | --- |
| A1 | Maven instead of Gradle; Spring Boot 4.1.2 instead of "Spring Boot 3"; seeded JUnit tests instead of jqwik; SeaweedFS + `pgsty/silo` instead of `minio/minio`; AWS CRT async HTTP client. | User's choice (Maven). Boot 3.x OSS support ended 30 Jun 2026. jqwik 1.10 targets JUnit Platform 1.x, the project is on JUnit 6. `minio/minio` left Docker Hub. The SDK's Netty client is on Netty 4.1 and would clash with Netty 4.2. |
| A2 | `topics.deleted_at TIMESTAMPTZ NULL`; `UNIQUE(name)` replaced by a partial unique index `WHERE deleted_at IS NULL`. | §10.1 says delete "marks the topic"; §8 has no column for it, and a name must be reusable. |
| A3 | `LAKEWIRE_BROKER_ID` must be a positive integer, stored as TEXT in `brokers.id`; it's the Kafka node id. | Kafka node ids are int32; §8 uses TEXT. |
| A4 | Initial ranges: ApiVersions 0–3 (kept), Metadata 1–9, Produce 3–12, Fetch 4–12, ListOffsets 1–6, InitProducerId 0–4, CreateTopics 2–6, DeleteTopics 1–5. Mins = kafka-clients 4.4.0 baselines where Kafka 4.0 removed older ones. Max stays below topic-ID versions (Metadata ≥ 10, Produce/Fetch ≥ 13, CreateTopics 7, DeleteTopics 6) and KIP-939's unstable InitProducerId v6. Changed only with compat tests. | §10.1 "pinned by compatibility tests rather than guessed"; v0.1 has no topic UUIDs. |
| A5 | Metadata `leaderEpoch = -1` (unknown); the broker ignores `currentLeaderEpoch` in Fetch. | Leadership is only a routing hint (§10.1); a constant epoch could make clients ignore leader changes. |
| A6 | LKW1 `broker_id` = u16 length + UTF-8 bytes. | §7.2 gives no width. |
| A7 | On fencing (heartbeat false or commit epoch reject) the broker fails buffered requests with `NOT_LEADER_OR_FOLLOWER`, then re-registers with a new epoch (no process exit). | Simplest recovery that keeps clients retrying. |
| A8 | Cluster id = a random Kafka-style base64 UUID stored once in a one-row `cluster` table (Flyway V1). | Metadata needs a stable cluster id; §8 is silent. |
| A9 | New small module `lakewire-cost` (shared by CLI and admin). Tiered-storage model = classic transfer costs + hot set on broker disk for 1 day, rest on S3 (configurable). | §10.2 and §10.4 both need the model; §15.1 has no home for it. |
| A10 | Metadata never auto-creates topics. | Explicit CreateTopics is in scope; auto-create isn't mentioned. |
| A11 | Topic configs accepted: `retention.ms` (default 7 days, like Kafka) and `cleanup.policy=delete`. Anything else, including `retention.bytes` and `compact`, → `INVALID_CONFIG`. | P0 has only time-based retention; key compaction is P2. Rejecting beats silently ignoring. |
| A12 | Defaults: `buffer.max.bytes` 64 MiB, `upload.max.concurrency` 4, block cache 256 MiB, segment-index cache 100k entries. | PRD gives linger 200 ms and 8 MiB objects only. |
| A13 | Storage retries exhausted → `KAFKA_STORAGE_ERROR` (retriable). | Not listed in §10.1's mapping; it's Kafka's code for this case. |
| A14 | ListOffsets by timestamp is batch-granular (first batch with `maxTimestamp >= t`). | Avoids decompressing batches; the result is ≤ Kafka's exact offset, so consumers only see a few extra records. |
| A15 | InitProducerId with a transactional id → `INVALID_REQUEST`. Unknown producer id in InitProducerId → allocate a new id. | Transactions are a non-goal (§2.3); the PRD names no code. |
| A16 | Client zone = `az=<zone>` at the end of `client.id`, after `,` or `;`. | Figure 1 shows `client.id=…,az=a`. |
| A17 | Orphan TTL 10 min, GC/retention period 1 min, DELETE_PENDING sweep 30 s, ref audit 5 min, producer-state expiry check 1 h; grace 5 min and producer TTL 7 days from the PRD. | TTL must exceed upload + commit time and the lease. |
| A18 | Broker metrics on `:9094/metrics` (JDK HttpServer). 9093 stays reserved for v0.2 gRPC. | §10.2 only gives the admin's actuator endpoint. |
| A19 | Docs pages live as Markdown in `docs/` and are published through the existing `web/` Next.js site as a static export to GitHub Pages. | §13 D16 says GitHub Pages; the repo already has `web/`. |
| A20 | One commit entry = one Produce partition payload, which is exactly one RecordBatch (kafka-clients enforces one batch per partition for Produce v3+), so idempotence works per chunk. | Fits §7.2 chunks and §9.2 arrays without splitting chunks. |
| A21 | A multi-stage `Dockerfile` (not Jib) builds the images, so `git clone` + `docker compose up` is 2 commands. The admin ships as a second image `lakewire-admin`. | §13 D15 check; keeps Spring out of the CLI jar. |
| A22 | Brokers, `lakewire dev` and `lakewire worker` run Flyway `migrate` at start (Flyway's PG lock serialises them); admin only validates. | Simplest way to get a fresh clone to a migrated schema. |
| A23 | `lakewire-testkit` in v0.1 is the shared internal test infrastructure; publishing it to Maven Central is v1.0 (§18). | §15.1 lists it; §18 puts the public module at v1.0. |
| A24 | Zone-aware Metadata (leader in the client's zone) ships in v0.1; the gRPC shared cache does not. | §10.1 and D9 put zone-aware Metadata in v0.1; §6 puts the shared cache in v0.2. |
| A25 | Silo is constructed with Testcontainers `MinIOContainer` after `DockerImageName.asCompatibleSubstituteFor("minio/minio")`. | `MinIOContainer` in Testcontainers 2.0.5 allowlists only `minio/minio`. `pgsty/silo` is the image A1 requires. |
