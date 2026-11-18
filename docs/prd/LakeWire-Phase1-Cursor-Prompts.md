# LakeWire Phase 1 (v0.1 launch): Cursor prompt pack

Source of truth: **LakeWire PRD v2.0** (27 Oct 2026). Phase 1 means
everything the PRD marks **P0 / v0.1** (§6, §10.1, §13 D1–D20, §18).
Starting point: Lesson 5 is done (Netty 4.2 server on :9092, ApiVersions
via kafka-clients generated classes, `RequestRouter`, `RequestHandler`
with a virtual thread per request, per-connection ordering, autoRead
backpressure at 16, graceful `KafkaServer.stop()`, 17 green tests).

How to use this file:

1. Copy the PRD into the repo first:
   `D:\Lakewire\docs\prd\lakewire-prd-v2.0.md` (create `docs\prd`).
   Cursor reads it from there, and every prompt cites its sections.
   Also save **this file** as
   `D:\Lakewire\docs\prd\LakeWire-Phase1-Cursor-Prompts.md`, because
   the prompts point Cursor at its Part 5 assumptions table.
2. Paste the **MASTER PROMPT** (Part 2) into Cursor's agent once. It
   saves the rules to `.cursor/rules/lakewire.mdc` and runs **M0**,
   then stops.
3. Then paste **one milestone prompt at a time** (Part 3), M1 → M12.
   Each one ends with `mvnw.cmd clean verify` green, one local commit,
   and a report. Read the report, then paste the next one.
4. Never let Cursor push. You push yourself when you're happy.

Prerequisites on your Windows machine: JDK 21, Docker Desktop running
(Testcontainers and compose need it), Git, PowerShell.

---

## Part 1: Phase 1 scope

### 1.1 What ships in v0.1 (PRD §6 P0, §10.1 "v0.1")

| Area | v0.1 scope | PRD |
| --- | --- | --- |
| Kafka subset | ApiVersions, Metadata, Produce (acks=all/1, acks=0 best effort), Fetch (long-poll, max.wait, min.bytes, no fetch sessions), ListOffsets (earliest/latest/timestamp), InitProducerId (no transactional IDs), CreateTopics, DeleteTopics. Must work with the Apache Kafka Java client, `kafka-console-*` tools and kcat. | §6, §10.1 |
| Idempotent producers | Producer ID + epoch + per-partition sequence; dups return the original offset; out-of-order rejected; dedupe inside the commit transaction. | §6, §8 |
| Postgres coordinator v2 | `sl_commit_object()` single round trip, batched multi-partition commit, epoch fencing, upload intents, broker registry + heartbeats + leases on the PG clock, LISTEN/NOTIFY `sl_hw`, group-commit flag, Day-3 latency lab + ADR-001. | §6, §7.1, §8, §9.2 |
| Storage | ObjectStore SPI (S3, local FS, fault-injecting wrapper), LKW1 object format, `If-None-Match: *` where supported. | §7.2, §6 |
| Object lifecycle | Orphan GC, time retention, ref-counted deletion with grace, 404-reload, producer-state expiry, single active worker via advisory lock. | §6, §9.1, §8 |
| Multi-broker | Stateless brokers, any broker writes any partition, zone-aware Metadata from `client.id` `az=…`. | §7, §10.1, D9 |
| Verification v1 | Java fault harness: kill, pause, broker↔PG partition, slow/failing S3, PG restart, retention storm nemeses; checkers I1–I5, I7, I8; seeds; 3-min PR smoke, nightly 30-min run. | §6, §11 |
| DX | `lakewire dev` (embedded Postgres + local FS, ready ≤ 10 s), `docker compose up` (3 brokers + Postgres + S3 + Grafana), CLI (`dev`, `topics`, `cost`, `verify`, `inspect`). | §6, §10.4, §12 |
| Observability + cost | Prometheus metrics (commit latency histogram, PUT/GET counts, cache hit ratio), Grafana JSON, `lakewire cost` (§12.1 model, prices from a config file). | §6, §12.1 |
| Admin | Spring Boot REST on :8080 (§10.2 endpoints except `/groups`). | §7.1, §10.2 |
| Launch | Apache-2.0 + NOTICE, CONTRIBUTING, SECURITY, CODE_OF_CONDUCT, GUARANTEES.md, docs site, benchmark + correctness reports, GHCR image, JReleaser, tag v0.1.0. | §13 D16–D20, §15 |

### 1.2 Explicit non-goals for v0.1

- Consumer groups (FindCoordinator, Join/Sync/Heartbeat/Leave,
  OffsetCommit/Fetch): **v0.2**. In v0.1 consumers use `assign()` and
  manual offsets, so I6 is not checked yet (§6 P1, §10.1, §18).
- gRPC peer cache and `BrokerAdmin` on :9093, the shared zone cache:
  **v0.2** (§10.3, §6 P1). Only the in-process cache ships in v0.1.
- Helm chart, OpenTelemetry, SASL/API keys, console, DescribeConfigs
  and friends, compaction, Jepsen workload, DST, Iceberg, quotas,
  coordinator sharding, Spring Boot starter (§6 P1/P2, §14).
- Kafka transactions / exactly-once, sub-10 ms latency, KRaft or
  ZooKeeper APIs, multi-region (§2.3).
- Topic IDs on the wire (Produce/Fetch v13+, Metadata v10+): not needed
  for v0.1; version ranges stay below them (see A4).

### 1.3 Module layout to reach (PRD §15.1 → Maven)

Root `lakewire-parent` (groupId `lakewire`, `0.1.0-SNAPSHOT`), packages
`lakewire.<module>.*`, no `dev.` prefix.

| Path | artifactId | Holds | Depends on |
| --- | --- | --- | --- |
| `lakewire-protocol/` | lakewire-protocol | Kafka codec glue, `SupportedApi`, error mapping (exists) | kafka-clients |
| `lakewire-storage/` | lakewire-storage | ObjectStore SPI, LocalFs, S3, FaultInjecting, LKW1 writer/reader, UUIDv7 | AWS SDK v2 |
| `lakewire-coordinator/` | lakewire-coordinator | Flyway SQL, `sl_*` functions, JDBC client, registry, NOTIFY listener | pgjdbc, HikariCP, Flyway |
| `lakewire-workers/` | lakewire-workers | Orphan GC, retention, DELETE_PENDING sweeper, ref audit, producer-state expiry | storage, coordinator |
| `lakewire-broker/` | lakewire-broker | Netty server, handlers, write path, read path, caches, metrics (exists) | protocol, storage, coordinator, workers |
| `lakewire-cost/` | lakewire-cost | §12.1 cost model (pure Java) — see A9 | none |
| `lakewire-admin/` | lakewire-admin | Spring Boot 4.1.2 REST :8080 | coordinator, storage, workers, cost |
| `lakewire-cli/` | lakewire-cli | picocli `lakewire` command, `dev` mode, shaded jar | broker, workers, cost, verify |
| `lakewire-testkit/` | lakewire-testkit | Shared Testcontainers setup (PG, S3, Toxiproxy, broker image) | Testcontainers |
| `verify/` | lakewire-verify | Workload, nemeses, checkers I1–I5/I7/I8, seeds | testkit, kafka-clients |
| `bench/` | lakewire-bench | Coordinator latency lab, e2e perf scripts, CSVs | coordinator |
| `deploy/compose/` | — | `compose.yaml`, Prometheus + Grafana config | — |
| `docs/` | — | `prd/`, `adr/`, `reports/`, Diátaxis pages | — |
| `web/` | — | Existing Next.js site (landing + docs) | — |

---

## Part 2: MASTER PROMPT (paste once)

Paste everything between the two lines into Cursor's agent with
`D:\Lakewire` open.

---

You are building **LakeWire v0.1**, a diskless Kafka-compatible
streaming log (Java 21, Netty, PostgreSQL coordinator, S3 object
storage). The product spec is `docs/prd/lakewire-prd-v2.0.md`. It is
the **source of truth**. Read all of it before writing code. If that
file is missing, stop and tell me to copy it there.

**Step 1. Save the rules.** Create `.cursor/rules/lakewire.mdc` with
frontmatter `alwaysApply: true` and the full RULES section below, so
they apply to every later chat. Commit it with M0.

**RULES**

Scope and process
1. Build only what the PRD marks P0 / v0.1. Never add features or
   requirements the PRD doesn't contain. Where the PRD is silent,
   pick the simplest sound option, write it down in
   `docs/adr/ASSUMPTIONS.md` (one line each: id, decision, reason),
   and mention it in your report.
2. Work one milestone at a time. For each milestone: implement → write
   tests → run `.\mvnw.cmd clean verify` (plus the milestone's extra
   checks) → fix until green → `git add` + `git commit` with the
   message I give (Conventional Commits style) → **STOP and report**.
   Never start the next milestone until I paste it.
3. Report format: files created/changed, tests added (count), the
   exact `clean verify` summary line(s), extra checks run and output,
   assumptions added, anything that didn't work. If something fails
   and you can't fix it within the milestone's design, stop and
   report the exact error. Don't redesign silently.
4. Git: commit locally only. **Never push, never force-push, never
   rewrite history, never amend a commit you didn't just make.**
5. Don't overwrite `README.md` (it exists). Propose README changes as a
   diff in your report. Don't break `web/` (the Next.js site).

Build and versions
6. Maven only (Maven wrapper, `mvnw.cmd` on Windows). No Gradle, even
   though the PRD says Gradle: that's a deliberate deviation.
7. Pin every dependency and plugin version as a property in the root
   `pom.xml` `<properties>` + `<dependencyManagement>` /
   `<pluginManagement>`. No version ranges, no `LATEST`, no SNAPSHOT
   deps. Docker images are pinned to exact tags too.
8. Use only the versions in the table at the end of these rules. If
   you need a library that isn't listed, verify the exact latest
   stable version on Maven Central (search.maven.org or
   repo1.maven.org metadata) and its license, say so in the report,
   and keep dependencies minimal.
9. **Never invent APIs.** Before using a class or method from a
   library, confirm it exists in the pinned version (open the jar in
   the IDE, the javadoc for that version, or the source). If unsure,
   write a tiny test first. kafka-clients 4.4.0 note: record classes
   (`MemoryRecords`, `DefaultRecordBatch`, `RecordBatch`) live in
   `org.apache.kafka.common.record.internal`.
10. Java 21, `-Xlint:all` clean, no `sun.*`, no preview features.
    Virtual threads for blocking work; the Netty event loop never
    blocks. Prefer records and `final` classes; no Lombok.
11. Spring Boot is allowed **only** in `lakewire-admin`. Postgres +
    Flyway only in the coordinator (and the admin reads through it).
    No Redis anywhere.

Protocol and correctness
12. Encode and decode Kafka messages only with kafka-clients'
    generated message classes (`*RequestData`, `*ResponseData`,
    `AbstractRequest.parseRequest`, `KafkaCodec`). Store and return
    RecordBatch v2 bytes verbatim; only patch `baseOffset` on read.
13. `SupportedApi` is the single registry. ApiVersions advertises only
    implemented APIs; every range is pinned by `SupportedApiTest` and
    proven by a compatibility test with the real Java client. An API
    gets an enum entry only together with its handler and tests.
    Unknown or unsupported API/version → `UNSUPPORTED_VERSION`.
14. Map every failure to a Kafka error code (PRD §10.1 "Error
    mapping"): stale broker epoch / lease or coordinator unavailable →
    `NOT_LEADER_OR_FOLLOWER` or `REQUEST_TIMED_OUT`; sequence gap →
    `OUT_OF_ORDER_SEQUENCE_NUMBER`; unknown/expired producer →
    `UNKNOWN_PRODUCER_ID`; fenced epoch → `INVALID_PRODUCER_EPOCH`.
    Keep the mapping in one class (`lakewire.protocol.ErrorMapping`)
    with a unit test per row. Never leak stack traces to clients.
15. Invariants I1–I8 (PRD §11) are design constraints, not just tests.
    Every lease, TTL, grace and expiry decision uses the **Postgres
    clock** (`now()`), never the broker clock. Visible = committed:
    never return data at or above the HW read from Postgres. An ack
    is sent only after the object is in storage **and** the commit
    transaction returned.
16. All coordinator writes go through the `sl_*` SQL functions in
    `lakewire-coordinator`. Keep `synchronous_commit` at its default.

Tests
17. Unit tests: JUnit 6 + AssertJ. Property-style tests: JUnit
    `@RepeatedTest`/`@ParameterizedTest` driven by a seeded
    `java.util.SplittableRandom`; print the seed on failure so it can
    be replayed. Don't use jqwik (see table note).
18. Integration tests are named `*IT.java`, run by maven-failsafe in
    `verify`, and use Testcontainers 2.0.5 for Postgres, S3 and
    Toxiproxy. They need Docker Desktop running; if Docker is
    missing, fail with a clear message rather than skip silently.
19. Tests never sleep for correctness; use Awaitility or explicit
    latches with timeouts. Every socket/client in tests has a timeout.

Security and hygiene
20. No secrets in git. Config comes from environment variables
    (`LAKEWIRE_*`); local values go in a gitignored `.env`; commit
    only `.env.example` with placeholder values. Never print secrets
    in logs.
21. `.gitattributes`: `* text=auto eol=lf`, `*.cmd text eol=crlf`,
    `*.bat text eol=crlf` (shell scripts inside containers must be LF).

**Pinned versions (verified on Maven Central / registries, Nov 2026)**

| What | Version |
| --- | --- |
| Java | 21 (`maven.compiler.release` 21) |
| kafka-clients | 4.4.0 (keep) |
| Netty BOM | 4.2.21.Final (keep) |
| slf4j / logback | 2.0.20 / 1.6.7 (keep) |
| JUnit BOM / AssertJ | 6.1.3 / 3.27.7 (keep) |
| pgjdbc `org.postgresql:postgresql` | 42.7.14 |
| HikariCP | 7.1.0 |
| Flyway `flyway-core` + `flyway-database-postgresql` | 13.12.2 |
| AWS SDK v2 BOM (`software.amazon.awssdk:bom`) | 2.59.3 |
| AWS CRT HTTP client (`aws-crt-client`, via BOM) | from BOM 2.59.3 |
| Caffeine | 3.3.1 |
| Micrometer `micrometer-registry-prometheus` | 1.17.2 |
| Testcontainers BOM (`testcontainers-postgresql`, `-minio`, `-toxiproxy`, `-junit-jupiter`) | 2.0.5 |
| Awaitility | 4.3.0 |
| picocli | 4.7.7 |
| Zonky `embedded-postgres` / binaries BOM | 2.2.2 / 18.7.0 |
| Spring Boot (admin only, BOM import) | 4.1.2 |
| maven-compiler / surefire / failsafe | 3.16.0 / 3.6.1 / 3.6.1 |
| maven-shade-plugin | 3.6.2 |
| maven-enforcer / exec-maven-plugin | 3.7.0 / 3.7.0 |
| JReleaser Maven plugin | 1.27.0 |
| Docker: Postgres | `postgres:18.7-alpine` |
| Docker: S3 (default) | `chrislusf/seaweedfs:4.54` |
| Docker: S3 (MinIO-compatible) | `pgsty/silo:RELEASE.2026-11-12T00-00-00Z` |
| Docker: Toxiproxy | `ghcr.io/shopify/toxiproxy:2.13.0` |
| Docker: Prometheus / Grafana | `prom/prometheus:v3.16.0` / `grafana/grafana:13.3.1` |
| Docker: JDK build / JRE run | `eclipse-temurin:21.0.13_9-jdk-noble` / `eclipse-temurin:21.0.13_9-jre-noble` |
| Docker: Kafka CLI tools / kcat (tests only) | `apache/kafka:4.4.0` / `edenhill/kcat:1.7.1` |
| GitHub Actions | checkout v7.0.1, setup-java v6.3.0, upload-artifact v7.0.2, docker/login v4.8.0, setup-buildx v4.4.1, build-push v7.4.0, metadata v6.2.0 |

Notes: jqwik 1.10.x runs on JUnit Platform 1.14 (not 6) and its author
discourages AI-agent use, so it's replaced by seeded JUnit tests.
`minio/minio` images are gone from Docker Hub; `pgsty/silo` is the
maintained MinIO-compatible fork used in Testcontainers docs.
Spring Boot 3.x is out of OSS support (3.5 ended 30 Jun 2026), so the
admin uses 4.1.2 instead of the PRD's "Spring Boot 3".

**Step 2.** After saving the rules, run milestone **M0** below exactly
as written, then stop and report.

(Then paste the M0 prompt from Part 3 right here, in the same
message.)

---

## Part 3: Milestone prompts

Every milestone prompt starts with the same header line. Paste them one
at a time, in order.

> Header for every milestone: *Follow `.cursor/rules/lakewire.mdc`
> (if it's missing, stop and tell me). Re-read the PRD sections listed
> below before coding. Do only this milestone, then commit and stop.*

### M0. Foundation: modules, CI, compose, test infrastructure

**PRD:** §7.1, §13 D1, §15.1, §15.2. **Commit:**
`build: v0.1 module skeleton, CI, compose and test infrastructure`

Goal: turn the Lesson 5 project into the Phase 1 multi-module layout
without changing broker behaviour.

Do:
1. Inspect the repo first (`pom.xml` files, `deploy/`, `docs/`, `web/`,
   `.gitignore`, `.mvn/`). Keep every Lesson 5 file and test working.
2. Root `pom.xml`: add modules in dependency order: `lakewire-protocol`,
   `lakewire-storage`, `lakewire-coordinator`, `lakewire-workers`,
   `lakewire-broker`, `lakewire-cost`, `lakewire-testkit`, `verify`
   (artifactId `lakewire-verify`), `bench` (`lakewire-bench`),
   `lakewire-admin`, `lakewire-cli`. Each new module gets a minimal
   `pom.xml`, a `package-info.java`, and one trivial test so the build
   proves it's wired. Wire inter-module dependencies now:
   storage, coordinator, cost → none of ours; workers → storage,
   coordinator; broker → protocol, storage, coordinator, workers;
   testkit → (Testcontainers only); verify → testkit; bench →
   coordinator, testkit; admin → coordinator, storage, workers, cost;
   cli → broker, workers, cost, verify. Add every version from the rules table as a
   property + managed dependency/plugin. Import the Testcontainers and
   AWS SDK BOMs in root `dependencyManagement`. Do **not** import the
   Spring Boot BOM at the root (admin only, M11).
3. Add maven-failsafe (3.6.1) to the root build: `*IT.java` runs in
   `integration-test` + `verify`. Surefire keeps `*Test.java`.
4. `lakewire-testkit`: `LakeWireContainers` with factory methods for
   `postgres:18.7-alpine` (Testcontainers `PostgreSQLContainer` from
   package `org.testcontainers.postgresql`), SeaweedFS S3
   (`GenericContainer` running `server -s3`, wait on the S3 port),
   Silo via `MinIOContainer`, and Toxiproxy (`ToxiproxyContainer` from
   `org.testcontainers.toxiproxy`). Verify each class/package in the
   2.0.5 jars before use. One `ContainersSmokeIT` starts PG + SeaweedFS
   and runs `SELECT 1` and a bucket create/list.
5. `deploy/compose/compose.yaml` (dev deps only for now): Postgres 18.7
   (volume at `/var/lib/postgresql`, which is the 18+ layout), SeaweedFS
   S3 with a bucket bootstrap, healthchecks. Values come from `.env`.
   Add `.env.example` (placeholders only) and make sure `.env` stays
   gitignored. Don't delete anything already in `deploy/`.
6. `.gitattributes` per the rules. `LICENSE` = the exact Apache-2.0
   text downloaded from `https://www.apache.org/licenses/LICENSE-2.0.txt`.
   `NOTICE` with the PRD §15.2 trademark line.
7. CI `.github/workflows/ci.yml`: on push + pull_request, ubuntu,
   Temurin 21 via setup-java v6.3.0 with Maven cache, run
   `./mvnw -B clean verify`, upload surefire/failsafe reports on
   failure. Make sure `mvnw` is executable in git
   (`git update-index --chmod=+x mvnw`).
8. `docs/adr/ASSUMPTIONS.md` seeded with all of Part 5 (A1–A24) from
   `docs/prd/LakeWire-Phase1-Cursor-Prompts.md`, and
   `docs/adr/README.md` (ADR index). Later milestones cite these ids.

Tests: existing 17 still pass; one trivial test per new module;
`ContainersSmokeIT`.

Done when:
- `.\mvnw.cmd clean verify` is green on Windows with Docker running.
- `docker compose -f deploy/compose/compose.yaml up -d` brings PG + S3
  to healthy.
- With the broker running (IntelliJ run configuration of
  `LakeWireBroker`, as in Lesson 4), `docker run --rm edenhill/kcat:1.7.1 -L -b
  host.docker.internal:9092` reaches it and the broker log shows the
  ApiVersions exchange (PRD D1 check). kcat may then report a Metadata
  error; that's expected until M3.

---

### M1. Coordinator: schema, `sl_commit_object`, epochs, intents, latency lab

**PRD:** §7.1 (Committer, Broker registry), §8, §9.1 items 1 and 4,
§9.2, §9.3 (NOTIFY), §11 I2, §13 D2–D3. **Commit:**
`feat(coordinator): schema v1, sl_commit_object, registry and ADR-001`

Goal: Postgres is the single source of truth for offsets, objects,
segments and brokers, behind a single-round-trip commit function.

Files (module `lakewire-coordinator`):
- `src/main/resources/db/migration/V1__schema.sql`: the PRD §8 tables
  **exactly** (topics, partitions, brokers, upload_intents, objects,
  segments + index, producer_ids, producer_state), plus the deltas in
  assumptions A2 (topic soft delete) and A8 (one-row `cluster` table
  for the cluster id). No consumer-group tables (v0.2).
- `db/migration/R__sl_functions.sql` (Flyway repeatable): all `sl_*`
  functions, `CREATE OR REPLACE`.
- Java: `CoordinatorConfig` (JDBC URL/user/password from env),
  `Coordinator` (DataSource via HikariCP, runs Flyway `migrate()` on
  start unless `LAKEWIRE_MIGRATE=false`; Flyway's own PG lock makes
  concurrent brokers safe), `CommitRequest`/`CommitResult` records,
  `BrokerRegistry`, `HwListener`.

`sl_commit_object` contract (one call per object, arrays via `unnest`):

```sql
sl_commit_object(
  p_object_id uuid, p_key text, p_size bigint,
  p_broker_id text, p_broker_epoch bigint,
  p_topic_ids bigint[], p_parts int[],
  p_byte_starts bigint[], p_byte_lens int[],
  p_n_records int[], p_n_batches smallint[],
  p_max_ts bigint[],
  p_producer_ids bigint[], p_producer_epochs smallint[],
  p_first_seqs int[], p_last_seqs int[])
RETURNS TABLE (idx int, base_offset bigint,
  error_code smallint, log_start_offset bigint)
```

Algorithm, in one transaction (PRD §9.1, §9.2):
1. Epoch + lease check: the broker row must have `epoch = p_broker_epoch`
   and `lease_expires_at > now()`. Otherwise return every entry with
   `NOT_LEADER_OR_FOLLOWER` and write nothing.
2. Ambiguous-commit replay (§11.1 note): if an `objects` row with
   `key = p_key` exists, return the stored results for its segments
   (same offsets) and write nothing.
3. Intent: `DELETE FROM upload_intents WHERE object_key = p_key AND
   broker_id = … AND broker_epoch = … RETURNING`. No row → return every
   entry with `NOT_LEADER_OR_FOLLOWER` (GC or fencing won the race;
   the client retries). This closes the GC-vs-late-commit race.
4. Lock partition rows `FOR UPDATE` in `(topic_id, part)` order. A
   missing partition or a soft-deleted topic → that entry gets
   `UNKNOWN_TOPIC_OR_PARTITION`.
5. Idempotence hook: entries with `producer_id >= 0` go through
   `sl_check_sequence` (stub in M1 that accepts everything; real logic
   in M6). Entries are processed in array order per partition.
6. Assign offsets set-based: per partition, accepted entries get
   consecutive base offsets in array order;
   `UPDATE partitions SET next_offset = next_offset + total … RETURNING`.
7. Insert `objects` (`state='COMMITTED'`, `live_refs` = accepted
   segment count, `live_bytes` = their bytes). If nothing was accepted,
   insert it as `DELETE_PENDING` with `delete_after = now() + grace`
   so GC removes it. Insert `segments`
   (`last_offset = base + n_records - 1`).
8. `pg_notify('sl_hw', …)` per touched partition with payload
   `topic_id:part:next_offset`. NOTIFY is only delivered on commit.

Error codes are Kafka numeric codes (from `Errors.X.code()`); keep the
SQL constants in one place and test them against `Errors`.

Other functions: `sl_register_broker(id, zone, endpoint)`: upsert,
`epoch = epoch + 1`, lease `now() + 10 s`, returns epoch.
`sl_heartbeat(id, epoch)`: extends the lease only if the epoch
matches; returns false when fenced. `sl_create_upload_intent(key,
broker, epoch)`. Lease/heartbeat values come from config (defaults
10 s / 2 s, PRD §7.1).

`HwListener`: one dedicated non-pooled pgjdbc connection, `LISTEN
sl_hw`, a virtual thread looping on `PGConnection.getNotifications(ms)`,
reconnects with backoff, dispatches to subscribers.

Group commit (§9.2, P0 flag): `GroupCommitter` can run several ready
`sl_commit_object` calls in one transaction when they become ready
within ≤ 5 ms. Flag `LAKEWIRE_GROUP_COMMIT` (default from ADR-001).

Day-3 latency lab (module `bench`, `bench/coordinator/`): a runnable
main (exec-maven-plugin) against a Testcontainers PG that measures
commit P50/P99 for partitions per commit {1, 16, 128, 512, 1024} ×
committers {1, 3, 6, 12} × strategy {per-row `FOR UPDATE` loop,
set-based `unnest`, group commit}. Writes
`bench/coordinator/results-<date>.csv` and a self-contained SVG chart
(generated in Java, no new dependency). Then write
`docs/adr/ADR-001-commit-strategy.md`: context, the numbers, decision,
consequences. The machine and versions go in the CSV header.

Tests:
- `CommitFunctionIT`: single object, multi-partition offsets, deleted
  topic, stale epoch, expired lease (set the lease into the past via
  SQL), missing intent, replay of an already committed key, zero
  accepted entries → DELETE_PENDING.
- `ConcurrentCommitIT` (PRD D2): 8 threads × random partitions × 10k
  commits total; then assert, per partition, segments cover
  `[0, next_offset)` with no overlap and no hole (I2) and the sum of
  records equals `next_offset`. Seeded and replayable.
- `BrokerRegistryIT`: epoch increments on re-register; heartbeat with
  an old epoch returns false.
- `HwListenerIT`: a commit produces a notification; a rolled-back
  transaction produces none.

Done when: PRD D2 and D3 checks hold. 10k concurrent commits show no
duplicate or missing offsets, and `bench/coordinator/` has a CSV +
chart + ADR-001 naming the chosen strategy.

---

### M2. Storage: ObjectStore SPI and the LKW1 format

**PRD:** §7.1 (Write path), §7.2, §6 (object lifecycle: `If-None-Match`),
§11.1 (fault-injecting wrapper), §13 D4. **Commit:**
`feat(storage): ObjectStore SPI, local FS, S3, fault injection, LKW1`

Files (module `lakewire-storage`, package `lakewire.storage`):
- `ObjectStore` SPI: `put(key, bytes)` (create-only), `getRange(key,
  start, len)`, `head(key)`, `delete(key)`, `list(prefix)`; async
  (`CompletableFuture`) signatures; typed exceptions
  `ObjectNotFoundException`, `ObjectAlreadyExistsException`,
  `TransientStorageException`.
- `LocalFsObjectStore`: root dir; create-only via write to a temp file
  then `Files.move(…, ATOMIC_MOVE)` and fail if the target exists;
  works on Windows paths.
- `S3ObjectStore`: `S3AsyncClient` with the AWS CRT HTTP client
  (`AwsCrtAsyncHttpClient`; this avoids the SDK's netty-nio-client,
  which is built on Netty 4.1 and would clash with our Netty 4.2 BOM).
  Config: endpoint override, region, path-style, credentials from the
  default provider chain or `LAKEWIRE_S3_*` env vars. Set request and
  response checksum behaviour to `WHEN_REQUIRED` for S3-compatible
  stores. PUT uses `PutObjectRequest.ifNoneMatch("*")` when
  `LAKEWIRE_S3_CONDITIONAL_PUT=true` (probe at startup: second PUT of a
  probe key must return 412; log the result). Map 404/412/5xx to the
  typed exceptions. Bounded retries with jittered backoff for 5xx; a
  412 on a *retry* means "maybe ours": HEAD and compare size + the
  CRC32C stored as user metadata before treating it as success.
- `FaultInjectingObjectStore` wrapper: seeded, configurable 5xx rate,
  extra PUT latency, 404 on GET, for tests and the M9 nemesis.
- `ObjectKeys`: `{prefix}/v2/{yyyy}/{mm}/{dd}/{hh}/{broker}-{epoch}-
  {uuidv7}.lkw` (UTC path). `UuidV7` generator per RFC 9562 (no new
  dependency), monotonic within a process.
- `lakewire.storage.lkw1`: `Lkw1Writer` and `Lkw1Reader` exactly per
  §7.2: header magic `LKW1`, `format_version` u8 = 1, `broker_id`,
  `broker_epoch` u64, `created_ms` u64; body = per-partition chunks of
  verbatim RecordBatch v2 bytes with `baseOffset = 0`; index entries
  `{topic_id u64, part u32, byte_start u64, byte_len u32, n_records
  u32, n_batches u16, max_ts i64, crc32c u32}`; footer `index_offset
  u64 | index_crc32c u32 | magic "LKW1"`. Big-endian. Encode
  `broker_id` as u16 length + UTF-8 bytes (A6). CRC32C via
  `java.util.zip.CRC32C`. Reader validates both magics and both CRCs
  and can read the index without Postgres.
- `BatchOffsets`: patch the 8-byte `baseOffset` of each batch in a
  chunk to `base + cumulative records`. The Kafka batch CRC doesn't
  cover `baseOffset` (§7.2), so `DefaultRecordBatch.ensureValid()`
  must still pass after patching; test that.

Tests:
- `Lkw1RoundTripTest` (PRD D4): 10,000 seeded random objects (random
  partitions, batch counts, sizes, compression types from real
  `MemoryRecords` built with kafka-clients' builders) → write → read
  index + every chunk byte-for-byte equal; corrupt one byte → reader
  fails with a clear error.
- `BatchOffsetsTest`: patched batches pass `ensureValid()`, offsets
  are correct, other bytes untouched.
- `ObjectStoreContractTest` (abstract) run against LocalFs, plus
  `S3ObjectStoreIT` against SeaweedFS **and** Silo containers:
  create-only semantics, range GET, 404, list, delete, the
  conditional-PUT probe result per store recorded in the test log.
- `FaultInjectingObjectStoreTest`: same seed → same fault sequence.

Done when: the 10k-case round-trip test passes and the contract tests
are green on LocalFs, SeaweedFS and Silo.

---

### M3. Topics: Metadata, CreateTopics, DeleteTopics (single broker)

**PRD:** §6 P0 protocol row, §8 (topics, partitions), §10.1 rows
Metadata/CreateTopics/DeleteTopics, §10.1 error mapping, §17 (return
UNSUPPORTED_VERSION cleanly). **Commit:**
`feat(broker): Metadata, CreateTopics and DeleteTopics`

Do:
1. Broker startup wiring (`LakeWireBroker`): config adds
   `LAKEWIRE_BROKER_ID` (integer, A3), `LAKEWIRE_ZONE`,
   `LAKEWIRE_ADVERTISED_HOST/PORT`, PG and storage settings. On start:
   coordinator (migrate), `sl_register_broker`, heartbeat virtual
   thread every 2 s; on fencing (heartbeat returns false) stop
   accepting writes, fail in-flight work, re-register with a new
   epoch (A7). Graceful stop releases nothing in PG; the lease just
   expires.
2. Change `RequestProcessor` to allow "no response" (needed for
   acks=0 in M4): return `Optional<ByteBuffer>`; `RequestHandler`
   skips the write but still keeps ordering and the in-flight count.
   Update the Lesson 5 tests.
3. `ErrorMapping` in `lakewire-protocol` (rule 14) with its table test.
4. `MetadataHandler`: brokers = live rows (`lease_expires_at > now()`)
   as nodes with their advertised endpoint and zone as `rack`;
   cluster id = a stable value stored once (A8); controller id = the
   lowest live node id. Topic list from PG (excluding soft-deleted).
   Each partition's leader = this broker in M3 (zone routing is M7),
   `replicas = isr = [leader]`, `leaderEpoch = -1` (A5). Unknown topic
   → `UNKNOWN_TOPIC_OR_PARTITION`; auto-create is off (A10).
5. `CreateTopicsHandler`: validate name (Kafka's rules: legal chars,
   length ≤ 249, not `.`/`..`), partitions ≥ 1 (default 1 when -1),
   replication factor accepted but ignored (S3 replicates; return the
   requested value, or 1 when -1), configs allow only `retention.ms`
   and `cleanup.policy=delete` (A11), `validateOnly` honoured. Insert
   topic + partition rows in one transaction via `sl_create_topic`.
   Errors: `TOPIC_ALREADY_EXISTS`, `INVALID_TOPIC_EXCEPTION`,
   `INVALID_PARTITIONS`, `INVALID_CONFIG`.
6. `DeleteTopicsHandler`: `sl_delete_topic` sets `deleted_at = now()`
   (A2). Data release happens via retention-style cleanup in M8.
7. `SupportedApi`: add Metadata, CreateTopics, DeleteTopics with the
   ranges in A4. Every case in `RequestRouter` must exist for every
   enum entry (a test enforces it).

Tests:
- Handler unit tests with a fake coordinator.
- `SupportedApiTest` updated (exact ranges pinned).
- `TopicsIT` with PG container + real broker: Java `Admin` client
  creates, lists, describes and deletes topics; duplicate create →
  `TopicExistsException`; describe a deleted topic →
  `UnknownTopicOrPartitionException`.
- `KcatMetadataIT` (Testcontainers `edenhill/kcat:1.7.1`, broker on the
  host via `host.testcontainers.internal` with
  `Testcontainers.exposeHostPorts`): `kcat -L` lists the topic.

Done when: `kafka-topics.sh --create/--list/--describe/--delete`
(from `apache/kafka:4.4.0`) works against the broker, and `kcat -L`
shows topics and partitions.

---

### M4. Produce: buffer → upload → commit → ack

**PRD:** §7.1 (Write path, Committer), §9.1 item 4, §10.1 Produce,
§12 (produce latency target), §13 D5. **Commit:**
`feat(broker): produce path with buffering, upload and commit`

Design (package `lakewire.broker.write`):
1. `ProduceHandler`: parse with `ProduceRequestData`; for each
   partition validate: topic/partition exists; records are
   `MemoryRecords` with exactly one batch, magic 2, CRC valid
   (`ensureValid()`), not a control or transactional batch
   (`INVALID_RECORD`); size ≤ `object.max.bytes`
   (`MESSAGE_TOO_LARGE`). Enqueue valid chunks into the write buffer
   and get a future per partition; block the virtual thread until all
   complete or the request's `timeoutMs` passes (`REQUEST_TIMED_OUT`).
   `acks=0` → enqueue and send no response (best effort, §10.1).
   `acks=1` and `acks=all` behave identically: ack only after PUT +
   commit. Any other acks → `INVALID_REQUIRED_ACKS`.
2. `WriteBuffer`: one per broker across all partitions; seals an object
   when `linger.ms` (default 200) elapses since the first chunk or
   `object.max.bytes` (default 8 MiB) is reached. Bounded memory
   (`buffer.max.bytes`, A12): when full, `ProduceHandler` waits (the
   per-connection serial order then stops the socket reads) and fails
   with `REQUEST_TIMED_OUT` if the request timeout expires.
3. `Uploader`: per sealed object: `sl_create_upload_intent` **before**
   the PUT → `Lkw1Writer` → `ObjectStore.put` (create-only). Bounded
   concurrent uploads (`upload.max.concurrency`, A12) via a semaphore.
4. `Committer`: commits sealed objects **in seal order** (uploads may
   finish out of order) with `sl_commit_object`, optionally group
   commit (ADR-001). Maps `CommitResult` rows back to partition
   futures (base offset, log start offset, error). Ambiguous commit
   (SQL exception after send): reconnect, then replay; step 2 of the
   function returns the stored offsets if it had committed (§11.1).
   PG unreachable past the request timeout → `REQUEST_TIMED_OUT`;
   storage retries exhausted → `KAFKA_STORAGE_ERROR` (A13).
5. Response: `ProduceResponseData` per partition with `baseOffset`,
   `logAppendTimeMs = -1`, `logStartOffset`, error code.
6. `SupportedApi`: Produce 3–12 (A4).

Tests:
- `WriteBufferTest`: linger and size sealing, ordering, memory bound.
- `ProduceHandlerTest`: each validation error → correct code.
- `ProduceIT` (PG + SeaweedFS + broker): Java producer with
  `enable.idempotence=false`, acks=all and acks=1, 3 partitions, 10k
  records: all acked; PG `segments` cover `[0, HW)`; every object key
  in PG exists in the bucket; LKW1 index matches the segments rows.
- `ProduceFaultIT`: `FaultInjectingObjectStore` with 30% PUT 5xx →
  all acked records still committed exactly once (non-idempotent: no
  ack without commit).
- `Acks0IT`: no response frame is written.

Done when (PRD D5): `kafka-console-producer.sh --bootstrap-server …
--topic t --producer-property enable.idempotence=false` writes lines
and rows are visible in PG (`segments`, `objects`), with data only in
the bucket and nothing on broker disk.

---

### M5. Fetch, ListOffsets, NOTIFY wake-ups (MILESTONE 1)

**PRD:** §7.1 (Read path), §9.3 (all bullets), §10.1 Fetch/ListOffsets,
§11 I1, I2, I4, §12, §13 D6–D7. **Commit:**
`feat(broker): fetch with long-poll, list offsets, read cache`

Design (package `lakewire.broker.read`):
1. `HwCache`: per partition `{hw, logStart}`; updated by `HwListener`
   NOTIFY; on a miss, read from PG. **Rule §9.3:** when the fetch
   offset is ≥ cached HW, re-read HW from PG before deciding between
   wait and `OFFSET_OUT_OF_RANGE`.
2. `SegmentIndex`: Caffeine cache of immutable segment rows per
   partition (by base offset), loaded from PG; lookup "segments with
   `last_offset >= fetchOffset` ordered by base, up to N bytes".
3. `BlockCache`: Caffeine, byte-weighted LRU (A12), key
   `(object_key, byte_start, byte_len)`, value = chunk bytes. Records
   hit/miss for the cache-hit-ratio metric (M10).
4. `FetchHandler`: `FetchRequestData`, full fetches only (respond with
   `sessionId = 0`; no fetch sessions in v0.1). Per partition: below
   `log_start_offset` → `OFFSET_OUT_OF_RANGE`; above HW after the PG
   re-read → `OFFSET_OUT_OF_RANGE`; equal to HW → nothing yet. Read
   chunks, patch base offsets (`BatchOffsets`), never include a batch
   whose `last_offset >= HW`. Respect `partitionMaxBytes` and
   `maxBytes`, but always return at least the first batch even if it
   is larger (Kafka semantics). Response fields: `highWatermark`,
   `lastStableOffset = HW` (no transactions), `logStartOffset`,
   `preferredReadReplica = -1`.
5. Long-poll: if total bytes < `minBytes`, wait (virtual thread, no
   busy loop) on a NOTIFY for any requested partition, up to
   `maxWaitMs`, then re-evaluate once and answer.
6. 404 rule (§9.1 item 2): a GET 404 → invalidate that partition's
   index entries, reload, retry once; still missing and now below log
   start → `OFFSET_OUT_OF_RANGE`; otherwise `KAFKA_STORAGE_ERROR` and
   an ERROR log tagged `I7`.
7. `ListOffsetsHandler`: `-2` earliest = `log_start_offset`, `-1`
   latest = HW; a timestamp → first segment with `max_ts >= t`, then the
   first batch in it with `maxTimestamp >= t` (batch granularity, A14);
   none → offset -1 per Kafka. Isolation level read-committed == HW.
8. `SupportedApi`: Fetch 4–12, ListOffsets 1–6 (A4).

Milestone-1 E2E (verify module, first checkers):
- `verify/…/checker/AckLog` (key, value hash, partition, offset) and
  `I1AckedReadableChecker`, `I2ContiguityChecker` (full read +
  `segments` scan for overlaps/holes).
- `EndToEndIT`: Java producer (idempotence off) writes 50k records to
  6 partitions while a Java consumer with `assign()` +
  `seekToBeginning` reads; I1 and I2 green; no file appears under the
  broker's working directory except logs.

Tests also: `FetchHandlerTest` (HW boundary, max bytes, first-batch
rule), `LongPollIT` (a fetch at HW returns within ~commit latency of a
produce, not after `maxWaitMs`; a fetch with no data waits ≈
`maxWaitMs`), `NotFoundReloadIT`, `ListOffsetsIT`.

Done when (PRD D6/D7 MILESTONE 1): stock tools produce and consume
through LakeWire: `kafka-console-producer` (idempotence off) then
`kafka-console-consumer.sh --topic t --partition 0 --offset earliest`
(with `--partition`, the consumer uses `assign()`, no group) and
`kcat -C -t t -p 0 -o beginning -e` print the same lines. Data is only
in the bucket, offsets only in PG. Also write
`docs/reports/coordinator-latency-v0.1.md` from the M1 CSV. (The 60-s
demo GIF is a manual task for you.)

---

### M6. Idempotent producers and InitProducerId

**PRD:** §6 P0 idempotent row, §8 (producer_ids, producer_state,
expiry note), §10.1 InitProducerId + error mapping, §11 I3, I5,
§13 D8. **Commit:** `feat: idempotent producers and InitProducerId`

Do:
1. `InitProducerIdHandler` (`InitProducerIdRequestData`): non-null
   `transactionalId` → reject with `INVALID_REQUEST` (A15). Otherwise
   `sl_init_producer_id(p_id, p_epoch)`: `-1` → new row, epoch 0;
   known id + matching epoch → bump epoch (if the epoch would exceed
   32767, allocate a new id); known id + other epoch →
   `INVALID_PRODUCER_EPOCH`; unknown id → allocate a new id (A15).
   PG unavailable → `COORDINATOR_NOT_AVAILABLE` (retriable).
2. Real `sl_check_sequence` inside `sl_commit_object` (same txn, row
   locked `FOR UPDATE`), per entry with `producer_id >= 0`:
   - producer id not in `producer_ids`, or no `producer_state` row and
     `first_seq != 0` → `UNKNOWN_PRODUCER_ID`;
   - request epoch < `producer_ids.producer_epoch` →
     `INVALID_PRODUCER_EPOCH`;
   - state epoch < request epoch: accept only if `first_seq = 0`
     (reset state), else `OUT_OF_ORDER_SEQUENCE_NUMBER`;
   - same epoch and `(first_seq, last_seq)` equals an entry in
     `recent` → duplicate: return that entry's `base_offset`, error 0,
     write no segment;
   - same epoch and `first_seq = last_seq + 1` (with Kafka's wrap at
     `Integer.MAX_VALUE` → 0) → accept;
   - anything else → `OUT_OF_ORDER_SEQUENCE_NUMBER`.
   On accept update `last_seq`, `producer_epoch`, `updated_at`, and
   keep the last 5 `{first_seq, last_seq, base_offset}` in `recent`.
   Also `producer_ids.last_used = now()`.
3. `ProduceHandler` passes `producerId`, `producerEpoch`,
   `baseSequence`, `lastSequence` from the batch into the commit
   entry.
4. `SupportedApi`: InitProducerId 0–4 (A4).

Tests:
- `SequenceCheckIT`: a table-driven test for every rule above,
  including wrap-around and the 5-entry window (6th-oldest duplicate →
  `OUT_OF_ORDER_SEQUENCE_NUMBER`).
- `IdempotentProducerIT`: default Java producer (idempotence **on**,
  acks=all, `max.in.flight=5`) writes 20k records while Toxiproxy
  drops/resets the client↔broker connection and while the broker↔PG
  link gets latency spikes; then I3 and I5 checkers (new, in `verify`)
  are green: no duplicates of `(producer, seq)` per partition, order
  preserved.
- `DuplicateRetryIT`: resend the exact same Produce frame with
  `KafkaTestClient` → same base offset, one segment in PG.

Done when (PRD D8): the default Java producer works unchanged, and a
duplicate retry returns the original offset.

---

### M7. Multi-broker: registry, fencing, zone-aware Metadata

**PRD:** §7 (Figure 1, any broker serves any partition), §7.1 Broker
registry, §10.1 Metadata, §11.1 pause nemesis, §12 (recovery ≤ 15 s),
§13 D9. **Commit:**
`feat: multi-broker cluster with epoch fencing and zone-aware metadata`

Do:
1. Zone parsing: client zone from a `client.id` suffix `az=<zone>`
   (accept `,az=a` or `;az=a` at the end, A16); else none.
2. `MetadataHandler`: brokers = all live brokers. Partition leader:
   among live brokers in the client's zone (sorted by node id), pick
   `floorMod(hash(topic, partition), n)`; no zone or no broker in that
   zone → same rule over all live brokers. Leadership is only a
   routing hint (§10.1): every broker accepts Produce/Fetch for every
   partition, so a stale client never gets `NOT_LEADER_OR_FOLLOWER`
   just for routing.
3. Fencing end-to-end: the epoch + lease check in `sl_commit_object`
   rejects a paused broker's late commit; the broker then fails its
   buffered requests with `NOT_LEADER_OR_FOLLOWER` (clients refresh
   metadata and retry) and re-registers.
4. Broker container image: a multi-stage `Dockerfile` at the repo root
   (`eclipse-temurin:21.0.13_9-jdk-noble` builds with `./mvnw -B -DskipTests
   package -pl lakewire-cli -am`; runtime `…-jre-noble`, non-root user,
   entrypoint `lakewire`). Until M11 the entrypoint may run
   `lakewire.broker.LakeWireBroker` directly; keep it switchable.
5. `lakewire-testkit`: `LakeWireCluster` starts PG, S3, Toxiproxy (in
   front of PG and S3) and N broker containers from the Dockerfile
   (`ImageFromDockerfile`). Brokers advertise `localhost:<hostPort>`;
   pick free host ports first and bind them via
   `withCreateContainerCmdModifier`, so clients on the host can reach
   every broker. Kill/pause through the container's Docker client
   (`killContainerCmd`, `pauseContainerCmd`, `unpauseContainerCmd`).

Tests:
- `ZoneMetadataTest`: zone parsing and leader choice, deterministic.
- `ThreeBrokerIT`: 3 brokers in zones a/b/c; clients with
  `client.id=x,az=b` get zone-b leaders; produce via one broker,
  fetch via another (I1).
- `ZombieBrokerIT` (PRD D9 check): pause a broker mid-upload for
  longer than the lease (and the orphan TTL, set short in the test),
  let the cluster continue, unpause: its late commit is rejected, no
  acked record is lost (I1), the client saw a retriable error.
- `BrokerKillIT`: kill one broker under load; clients resume ≤ 15 s
  (§12 target, report the measured value).

Done when: 3 brokers run; a paused broker's late commit is rejected.

---

### M8. Object lifecycle: GC, retention, ref-counted deletion

**PRD:** §6 P0 lifecycle row, §7.1 Workers, §8 producer-state expiry,
§9.1 (whole section), §10.1 DeleteTopics, §11 I7, I8, §11.1 retention
storm, §13 D10. **Commit:**
`feat(workers): orphan GC, retention, ref-counted deletion, audits`

Module `lakewire-workers`; runs inside every broker
(`LAKEWIRE_WORKERS=true` default) and later as `lakewire worker`.
Exactly one active instance: a dedicated PG connection holding
`pg_try_advisory_lock(<constant>)`; losing the connection means losing
leadership, so stop all jobs and retry acquisition.

Jobs (periods and TTLs configurable, defaults in A17):
1. **Orphan GC:** `DELETE FROM upload_intents WHERE created_at < now()
   - orphan_ttl RETURNING object_key` (in its own transaction, first),
   then delete those keys from storage. Then a listing sweep: any
   bucket key under the prefix with no `objects` row **and** no
   `upload_intents` row (checked in one snapshot) is uncommittable
   (commit needs the intent), so delete it.
2. **Retention:** per topic with `retention.ms` (default 7 days, A11):
   in one transaction delete the oldest segments of each partition
   whose `max_ts < now_ms - retention`, **only as a prefix** (stop at
   the first segment that is not expired, so offsets stay contiguous),
   set `log_start_offset` to the next remaining base offset (or
   `next_offset`), and decrement `objects.live_refs/live_bytes` with
   `DELETE … RETURNING object_id, byte_len`. When `live_refs` hits 0:
   `state = 'DELETE_PENDING'`, `delete_after = now() + grace` (default
   5 min, PRD). Put this in `sl_apply_retention`.
3. **Deleted topics:** same function path drops all segments of
   soft-deleted topics, then removes their partition, producer_state
   and topic rows once no segments remain.
4. **DELETE_PENDING sweeper:** `now() > delete_after` → storage DELETE
   (idempotent), then delete the row. A failure between the two is
   retried; the listing sweep catches any leak.
5. **Ref audit (I7, never deletes):** `live_refs = count(segments)`
   and `live_bytes = sum(byte_len)` per object; every COMMITTED
   object exists (HEAD, sampled with a configurable cap). Mismatch →
   ERROR log + metric `lakewire_audit_violations_total{invariant}`.
6. **Producer-state expiry:** delete `producer_state` rows untouched
   for `producer.state.ttl` (default 7 days, §8); the producer then
   gets `UNKNOWN_PRODUCER_ID` and re-initialises.

Also: `verify` checkers `I7NoDanglingRefChecker` and
`I8NoLeakChecker` (bucket listing vs `objects` + `upload_intents`:
every key without a row is younger than orphan TTL + GC period).

Tests:
- `RetentionIT`: multi-partition objects with different retention;
  an object is deleted only after all its segments are gone and the
  grace has passed.
- `OrphanGcIT`: the upload-intent race from §9.1 item 4: upload,
  stall past TTL, GC runs, the late commit fails with no acked data
  loss; plus the listing sweep.
- `AdvisoryLockIT`: two worker instances, only one active; kill its
  connection → the other takes over.
- `RetentionStormIT` (PRD D10 check): very short retention while
  consumers read old offsets: they get data or `OFFSET_OUT_OF_RANGE`,
  never corrupted bytes; I7 and I8 green.

Done when: the retention storm test shows I7/I8 green.

---

### M9. Verification harness v1: nemeses, checkers, seeds, `lakewire verify`

**PRD:** §4.1 pillar 1, §6 P0 Verification v1, §11 (I1–I5, I7, I8),
§11.1, §11.2 item 2, §12 correctness target, §13 D11–D13. **Commit:**
`feat(verify): fault-injection harness, nemeses and checkers`

Module `verify` (`lakewire.verify`):
1. `Workload`: N idempotent producers write keyed records whose value
   encodes `(producerIndex, seq, sha256(payload))`; consumers read
   all partitions with `assign()`; both log a history (acks, reads)
   to a JSONL file per run.
2. `Nemesis` implementations (PRD §11.1): `BrokerKill`
   (`docker kill -s KILL`, timed to land during uploads and during
   commits), `BrokerPause` (pause longer than lease and orphan TTL),
   `PartitionPg` (Toxiproxy `timeout` and `reset_peer` toxics on the
   broker↔PG proxy), `SlowS3` (Toxiproxy latency on broker↔S3 plus
   `FaultInjectingObjectStore` 5xx/slow PUT/404), `PgRestart`
   (restart the PG container), `RetentionStorm`. No clock-skew
   nemesis (§11.1 explains why).
3. `Scheduler`: seeded (`--seed`), picks nemeses and timings from the
   seed; logs the seed first; same seed → same schedule.
4. Checkers I1, I2, I3, I4 (consumer histories vs final log: no
   offset ≥ HW, no "phantom then gone"), I5, I7, I8. Output
   `verify-report.json` + a Markdown summary: checker version,
   nemeses, seed, ops, violations.
5. `lakewire verify --nemesis kill,pause,partition-pg --duration 10m
   --seed 42` (wired into the CLI in M11; for now a `main`) runs
   against a `LakeWireCluster` (3 brokers) and exits non-zero on any
   violation.
6. Fix the findings: backpressure, error codes, ambiguous-commit
   handling (PRD D13). Each real bug found gets a regression test and
   a one-paragraph entry in `docs/reports/bugs-found.md`.
7. CI: `.github/workflows/nemesis.yml` with a 3-minute smoke on every
   PR (fixed seed list) and a nightly 30-minute run with random seeds
   (`schedule:` cron), uploading reports as artifacts.

Tests: unit tests for every checker with hand-made histories that
must pass and ones that must fail (lost write, duplicate, gap,
phantom read, dangling ref, leak). `VerifySmokeIT` = 3-min run.

Done when (PRD D11, D13): `lakewire verify` runs a 10-minute job end
to end, then 50 consecutive nemesis runs are green (run them as a
loop with distinct seeds; commit the summary to
`docs/reports/soak-<date>.md`). Before launch you need ≥ 100 green
runs (§12, §18): keep the loop running overnight.

---

### M10. Observability, cost calculator, benchmarks (MILESTONE 2)

**PRD:** §6 P0 observability row, §12, §12.1, §13 D14, D17.
**Commit:** `feat: metrics, Grafana dashboard, lakewire cost, benchmarks`

Do:
1. Broker metrics with Micrometer 1.17.2 `PrometheusMeterRegistry`
   (package `io.micrometer.prometheusmetrics`), served by the JDK
   `com.sun.net.httpserver.HttpServer` on `LAKEWIRE_METRICS_PORT`
   (default 9094, A18) at `/metrics`. At minimum:
   `lakewire_commit_latency_seconds` (histogram, P50/P99),
   `lakewire_produce_latency_seconds`, `lakewire_fetch_latency_seconds`,
   `lakewire_s3_put_total`, `lakewire_s3_get_total` (with outcome tag),
   `lakewire_cache_hits_total` / `lakewire_cache_misses_total`
   (dashboard computes the hit ratio), `lakewire_object_bytes`,
   `lakewire_buffer_bytes`, `lakewire_audit_violations_total`.
2. `deploy/grafana/lakewire-dashboard.json` (+ provisioning files) and
   `deploy/prometheus/prometheus.yml`.
3. `lakewire-cost`: `CostModel` implementing §12.1 exactly: per GiB
   written across Z zones for classic Kafka RF=3 (producer→leader
   cross-AZ (Z-1)/Z × $0.02, replication 2 × $0.02), LakeWire
   (zone-local produce ≈ $0, PUTs = GiB / object size × PUT price, GETs
   = zones × objects × GET price), and Kafka tiered storage (same
   transfer as classic; storage split hot/remote, A9). Storage per
   GB-month shown separately. Prices come from
   `lakewire-cost/src/main/resources/prices/aws-us-east-1.properties`
   (values from §12.1, with source URLs and the check date in
   comments) and can be overridden with `--prices <file>`. Always
   print the §12.1 "honest caveats". Test: the §12.1 table values
   (≈ $0.053 vs ≈ $0.0008 per GiB at 8 MiB objects, 3 zones) are
   reproduced to 4 decimal places.
4. Benchmarks (`bench/e2e/`): scripts (PowerShell + bash) that run
   `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh`
   from `apache/kafka:4.4.0` against the compose cluster, plus an
   e2e latency probe (Java, produce → consume timestamps). Output raw
   CSVs with hardware, versions and configs in the header. Laptop
   results are labelled as laptop results.

Tests: `MetricsEndpointIT` scrapes `/metrics` and finds every metric
name above; `CostModelTest`.

Done when (PRD D14 MILESTONE 2): a 3-broker cluster survives the M9
faults with checkers green, and the first benchmark CSVs are committed
under `bench/`.

---

### M11. DX: CLI, admin REST, `lakewire dev`, compose

**PRD:** §2.2 (Rahul), §4.1 pillar 5, §6 P0 DX row, §10.2, §10.4,
§12 (startup ≤ 10 s), §13 D15. **Commit:**
`feat: lakewire CLI, dev mode, admin REST and compose cluster`

Do:
1. `lakewire-cli` (picocli 4.7.7, subcommands): `dev`, `broker`,
   `worker`, `topics create|list|describe|delete` (e.g. `topics create
   orders -p 12 --retention 7d`, via the Kafka `Admin` client),
   `cost --gib-per-day 500 --zones 3 --retention 7d`, `verify …` (M9),
   `inspect object <key or s3:// URI or local path>` (decode LKW1
   header + index without Postgres). Shaded jar with
   `ServicesResourceTransformer` (Flyway, AWS SDK and picocli use
   `META-INF/services`). Launchers `bin/lakewire` (bash) and
   `bin/lakewire.cmd`.
2. `lakewire dev`: embedded Postgres (Zonky 2.2.2, binaries 18.7.0,
   data under `%USERPROFILE%\.lakewire\dev` or `~/.lakewire/dev`),
   `LocalFsObjectStore`, one broker on :9092, workers on, metrics on.
   Prints "ready" with the bootstrap address. Must be ready ≤ 10 s on
   a warm machine (measure and report).
3. `lakewire-admin` (Spring Boot 4.1.2, BOM import in this module only;
   `spring-boot-starter-webmvc`, `-actuator`, `-jdbc`,
   `micrometer-registry-prometheus`): endpoints of §10.2 except
   `/groups` (v0.2): `GET /api/v1/cluster`, `GET/POST /api/v1/topics`,
   `DELETE /api/v1/topics/{name}`,
   `GET /api/v1/topics/{name}/partitions/{p}/segments`,
   `GET /api/v1/objects/{id}`, `GET /api/v1/cost?gib_per_day=…&zones=3`,
   `POST /api/v1/verify/audit` (runs the M8 I7/I8 audit once),
   `GET /actuator/prometheus`. All writes go through the `sl_*`
   functions. Admin never runs Flyway migrations (only validates).
   Check `mvnw dependency:tree -pl lakewire-admin` for conflicts with
   the parent pins (logback 1.6.7 vs Boot's own); if Boot fails with
   the parent's version, pin the version Boot 4.1.2 manages **in this
   module only** and record it as an assumption.
4. `deploy/compose/compose.yaml` full cluster: Postgres, SeaweedFS (+
   bucket init), 3 brokers (zones a/b/c, ports 9092/9192/9292,
   built from the root `Dockerfile`), admin on 8080, Prometheus,
   Grafana with the dashboard provisioned. Healthchecks and
   `depends_on: condition: service_healthy`. All secrets from `.env`.

Tests: picocli command tests (parsing, help output); `DevModeIT`
(start `lakewire dev` in-process, produce/consume with the Java
client, measure ready time); `AdminApiIT` (`@SpringBootTest` +
Testcontainers PG); `InspectCommandTest` on an LKW1 fixture.

Done when (PRD D15): on a fresh clone, `docker compose -f
deploy/compose/compose.yaml up -d` (after `git clone`) gives a running
3-broker cluster: ≤ 2 commands. Also `lakewire dev` + kcat
produce/consume works (the README quickstart shape in §15.3).

---

### M12. Docs, reports and release engineering (launch prep)

**PRD:** §4.1, §13 D16–D19, §15.1–§15.5, §16.1 (landing page),
§18. **Commit:** `docs: v0.1 docs, guarantees, reports and release tooling`

Do:
1. `GUARANTEES.md`: each promise (I1–I5, I7, I8; I6 marked v0.2)
   with the checker class that tests it, the nemeses that target it,
   and a link to the latest report in `docs/reports/`.
2. Docs in `docs/` with the Diátaxis structure (§15.4): tutorials
   (quickstart, Spring Boot app using `assign()`), how-to (S3
   providers, tuning linger), reference (config env vars, metrics,
   REST, CLI, compatibility matrix clients × APIs with versions),
   explanation (architecture, ADRs, correctness model, cost model,
   Postgres-clock leases, the upload-intent race). Publish them
   through the existing `web/` Next.js site as a static export to
   GitHub Pages (`.github/workflows/pages.yml`); inspect `web/` first
   and don't break its current pages (A19).
3. Reports with raw data (D17): `docs/reports/benchmark-v0.1.0.md`
   (from M10 CSVs) and `docs/reports/correctness-v0.1.0.md` (checker
   version, nemeses, number of runs, seeds, violations found and
   fixed, §11 wording).
4. Community files: `CONTRIBUTING.md` (§15.4: dev setup in 2
   commands, module map, "run verify before PR", DCO sign-off, commit
   style, how to add a nemesis or checker), `SECURITY.md` (GitHub
   private vulnerability reporting; supported versions), 
   `CODE_OF_CONDUCT.md` (Contributor Covenant 2.1 text from its
   official site; leave the contact as `TODO-CONTACT` for me to fill),
   `.github/ISSUE_TEMPLATE/`, PR template, and
   `docs/launch/good-first-issues.md` with the 10 issues from §15.4
   written out (title + body), ready for me to create.
5. Release tooling: `.github/workflows/release.yml` on tag `v*`:
   build, test, push the image `ghcr.io/sunilmaurya-18/lakewire` (and
   `…/lakewire-admin`) with docker/login v4.8.0 + build-push v7.4.0,
   then JReleaser 1.27.0 (`jreleaser.yml` at the root) creates the
   GitHub release with the changelog and the CLI zip. Locally,
   prove it with JReleaser's dry run (`-Djreleaser.dry.run=true`) and
   `docker build .`. Set the project version to `0.1.0` in a separate
   commit only when I say so; do **not** create or push the tag.
6. README: do not overwrite. Put a proposed diff (badges, quickstart,
   links to GUARANTEES and reports, per §15.3) in your report.

Done when: a clean-machine dry run of CONTRIBUTING's 2-command setup
works, JReleaser dry run passes, `docker build .` succeeds, docs build
locally. (D19 "someone else completes the quickstart unaided" and the
D16 "docs deployed" check happen after you push.)

---

## Part 4: Launch checklist (PRD §13 D17–D20, §15, §18)

Engineering gates (all must be true before tagging):

- [ ] `.\mvnw.cmd clean verify` green locally and in CI on `main`.
- [ ] ≥ 100 nemesis runs with 0 invariant violations for I1–I5, I7,
      I8; seeds listed in `docs/reports/correctness-v0.1.0.md` (§12,
      §18).
- [ ] Compatibility: Java client 4.4.0 (idempotence on), console
      tools, kcat all pass the compat ITs; matrix published (§18).
- [ ] §12 targets measured on the laptop and reported honestly, met
      or not: produce P50 ≤ 300 ms / P99 ≤ 800 ms, e2e P99 ≤ 1.5 s,
      commit P99 < 100 ms at 128 partitions/commit, ≥ 50 MiB/s with 3
      brokers, recovery ≤ 15 s, `lakewire dev` ready ≤ 10 s.
- [ ] D17: `benchmark-v0.1.0.md` and `correctness-v0.1.0.md` with raw
      CSVs committed. Optional real-S3 run only with a hard budget
      cap, billing alert and teardown script (§17).
- [ ] Fresh clone → running in ≤ 2 commands, ≤ 10 s for `lakewire dev`
      (§18 DX).
- [ ] `ApiVersions` advertises exactly the implemented APIs, ranges
      pinned by `SupportedApiTest`.
- [ ] No secrets in git history (`git log -p | Select-String` for
      keys, or gitleaks), `.env` ignored, `.env.example` placeholders.

Repository and community (§15.2–§15.4):

- [ ] `LICENSE` (Apache-2.0 verbatim), `NOTICE` (trademark line).
- [ ] `README.md` updated by you from Cursor's proposed diff (§15.3:
      one-liner + badges + 20-s GIF, why + cost table, 3-command
      quickstart, how it works, correctness, compat matrix, roadmap,
      comparison, contributing, license).
- [ ] `GUARANTEES.md`, `CONTRIBUTING.md` (DCO), `SECURITY.md`,
      `CODE_OF_CONDUCT.md` (fill in the contact), issue/PR templates.
- [ ] 10 good-first-issues created from
      `docs/launch/good-first-issues.md`; GitHub Discussions on;
      public roadmap board (§15.4).
- [ ] Docs site deployed on GitHub Pages (D16).
- [ ] Domain `lakewire.dev` / `lakewire.io` verified and bought, GitHub
      org `lakewire` claimed (§4.3, §17).

Release (D19–D20):

- [ ] D19 clean-machine dry run; **someone else** completes the
      quickstart unaided.
- [ ] JReleaser dry run green; GHCR image builds in the release
      workflow.
- [ ] D18: launch blog post, 3-minute demo video (problem → demo →
      kill a broker live → checkers green), landing page with the
      cost calculator; drafts reviewed by 2 friends.
- [ ] Bump version to `0.1.0`, commit, then **you** create and push
      tag `v0.1.0` (`git tag -a v0.1.0 -m "LakeWire v0.1.0"`,
      `git push origin v0.1.0`) and make the repo public.
- [ ] D20: Show HN ≈ 8:00 PM IST ("Show HN: LakeWire – a
      Kafka-compatible log on S3 + Postgres, with a published
      fault-injection report"), founder comment ready; then Reddit
      (r/apachekafka, r/java, r/dataengineering, r/programming,
      spaced over 3 days), X / LinkedIn / dev.to thread on the
      upload-intent race; answer every comment for 48 h (§15.5).
- [ ] After launch: bump to `0.1.1-SNAPSHOT`; start the v0.1.x triage
      window (§14).

---

## Part 5: Assumptions and decisions (where the PRD is silent or outdated)

Cursor copies the relevant ones into `docs/adr/ASSUMPTIONS.md`.

| Id | Decision | Why |
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
