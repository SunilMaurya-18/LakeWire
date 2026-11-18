# LakeWire PRD v2.0

PRODUCT REQUIREMENTS DOCUMENT · v2.0

## LakeWire

Diskless, Kafka®-compatible streaming on just Postgres + S3, with its guarantees tested in public

> “Point your Kafka clients at it. Your data lives only in a bucket, Postgres keeps the order, and every release ships with a reproducible proof that acknowledged writes are not lost.”

| Field | Value |
| --- | --- |
| Owner | Sunil Maurya · B.Tech CSE (2nd year), PSIT Kanpur |
| Repository (planned) | [github.com/SunilMaurya-18/lakewire](https://github.com/SunilMaurya-18/lakewire) |
| Status | Draft v2.0 · 27 Oct 2026 · supersedes v1.0 (21 Oct 2026) |
| License (planned) | Apache-2.0 |
| Sprint | 20 days full-time: Wed 28 Oct → Mon 16 Nov 2026 (v0.1 public launch) |
| Then | Weekends (2 days/week) → v0.2 (Dec), v0.3 (Feb), v1.0 (Mar 2027) |

All latency, throughput and cost numbers for LakeWire in this document are targets or model outputs, not measured results. Third-party facts carry a source number [n]; the source list is in the Appendix (checked 27 Oct 2026). Kafka® is a trademark of the Apache Software Foundation. LakeWire is not affiliated with the ASF.

## Contents

1. Executive summary
2. Problem and target users
3. Competitive landscape (sourced)
4. Positioning and differentiation
5. What changes from v1.0
6. Scope and priorities (P0/P1/P2)
7. Architecture
8. Data model
9. Design fixes: object lifecycle, coordinator, visibility
10. APIs: Kafka subset, REST admin, gRPC
11. Correctness and verification
12. Non-functional targets and cost model
13. 20-day sprint plan (day by day)
14. Weekend roadmap to v1.0
15. Open-source launch plan
16. Startup signals and the resume story
17. Risks and mitigations
18. Success metrics
Appendix: Sources

## 1 Executive summary

What it is. LakeWire is an open-source, diskless streaming log that speaks a subset of the Apache Kafka wire protocol. Stateless Java 21 brokers write record batches straight to S3-compatible object storage. PostgreSQL is the single source of truth for offsets, metadata, producer state and broker epochs. Brokers keep no data on disk, so you can kill, add or replace any broker at any time.

Why v2. v1.0 was a solid learning project with its own gRPC API. v2.0 turns it into a product: existing Kafka clients work unchanged, the default Java producer settings (idempotence on) are supported, and the project ships with something most of the market lacks: published, reproducible correctness evidence and a transparent cost model.

Differentiation angle: "the verifiable diskless Kafka". LakeWire does not try to beat WarpStream or AutoMQ at scale. It aims to be the diskless Kafka-compatible log that is easiest to trust and understand: only two dependencies (Postgres + any S3 bucket), a small Java codebase you can read in an afternoon, a fault-injection suite that runs in CI on every pull request, a public "guarantees" page backed by test reports, and an honest cost calculator. Section 4 explains why this angle is the right one.

| Day 7 · Tue 3 Nov | Day 14 · Tue 10 Nov | Day 20 · Mon 16 Nov |
| --- | --- | --- |
| Stock Kafka CLI tools produce and consume through LakeWire. Data lives only in S3/MinIO and offsets only in Postgres. The coordinator latency report is done. | A 3-broker cluster with idempotent producers survives kill, pause and partition faults with every invariant checker green. First laptop benchmarks and the cost calculator are working. | v0.1.0 public launch: Apache-2.0 repo, docs site, lakewire dev one-command start, benchmark and correctness reports, demo video, Show HN. |

## 2 Problem and target users

### 2.1 The problem

Classic Kafka in the cloud keeps three replicas of every partition on broker disks spread across availability zones (AZs). That design was built for data centers, and in the cloud it costs money in two places:

- Cross-AZ network. AWS charges $0.01/GB in each direction for traffic between AZs in a region, so $0.02 per GB crossing [17]. With RF=3 across three AZs, each byte is replicated to two other zones, and about two-thirds of producer traffic also crosses a zone to reach the partition leader. WarpStream (a vendor, so treat it as a claim) puts this at $0.053 per GiB through a three-zone Kafka cluster [1]. Our own arithmetic in §12.2 gives the same result.

- Broker disks. Replicated block storage, provisioned with headroom, costs far more per GiB-month than object storage. WarpStream's claim is $0.60 vs $0.02 per GiB-month [1]. S3 Standard lists $0.023/GB-month in us-east-1 [18].

- Operations. Stateful brokers mean partition reassignment, rebalancing, and slow recovery after a node loss.

The fix the industry has converged on is diskless topics: write batches straight to object storage, which is already replicated across zones, and keep only metadata in a coordinator. The trade-off is latency. Each produce has to wait for batching, a PUT and a metadata commit. Aiven's analysis breaks this down as linger 0–250 ms + upload 120–200 ms (400–500 ms worst case) + commit 10–200 ms [9]. Diskless therefore suits logging, observability, CDC, analytics and data-lake feeds rather than sub-10 ms trading paths.

### 2.2 Target users

| Persona | Need | What LakeWire gives them |
| --- | --- | --- |
| Priya, platform engineer (startup or mid-size, AWS/ GCP) | Cut the Kafka bill for logs, metrics and CDC topics, where about 1 s latency is fine. Wants to self-host and keep data in her own bucket. | Kafka-compatible endpoint, Helm chart, a cost calculator using her own numbers, and zone-aware routing. |
| Rahul, Java/Spring developer | Local and CI Kafka that starts in seconds and is cheap to run in staging. | lakewire dev (one process, no Docker needed), a Testcontainers module, and Spring Kafka working out of the box. |
| Meera, data engineer | Wants topics queryable as tables without running a Connect cluster. | Iceberg/Parquet export (P1/P2). |
| Reviewer / contributor / interviewer | Wants to understand and trust a distributed system. | Readable code, ADRs, a guarantees page backed by fault-injection reports, and good-first-issues. |

### 2.3 Non-goals (v1.0)

- Kafka transactions / exactly-once across partitions. This is deliberate. Jepsen's Bufstream analysis found unresolved problems in the Kafka transaction protocol itself [12]. Idempotent producers (per-partition dedupe) are in scope.

- Sub-10 ms produce latency. That would need a disk or replicated WAL tier, which is how AutoMQ's commercial BYOC edition gets there [4].

- Multi-region active/active, full ACL model, KRaft/ZooKeeper APIs, and Kafka Streams-specific internals beyond what basic clients need.

## 3 Competitive landscape (sourced)

This is a crowded, fast-moving space. Being honest about it makes LakeWire more credible to reviewers. Every fact below carries a source number from the Appendix. Vendor performance and cost figures are the vendors' own claims.

| Project | Status / license | Architecture | Notes relevant to LakeWire |
| --- | --- | --- | --- |
| WarpStream | Proprietary BYOC. Acquired by Confluent (9 Sep 2024) [2]. IBM completed its acquisition of Confluent on 17 Mar 2026 [3]. | Single stateless Go "Agent" binary that speaks Kafka. Data goes to the customer's bucket; metadata goes to WarpStream's managed control plane [1]. | The category's original product. Claims $0 interzone networking and 100+ production customers [1]. Not open source, and you can't self-host the control plane. |
| AutoMQ | Apache-2.0 since Apr 2025 (was BSL) [5]. About 10.9k GitHub stars (GitHub API, 27 Oct 2026). | Fork of Apache Kafka with the storage layer replaced by an S3-based stream store [4]. | Docs list open-source S3-WAL produce P99 at 600+ ms vs <10 ms with the EBS/FSx WAL in the commercial BYOC edition [4]. Large codebase (it's a Kafka fork). |

| Project | Status / license | Architecture | Notes relevant to LakeWire |
| --- | --- | --- | --- |
| Apache Kafka KIP-1150 / Aiven Inkless | KIP-1150 "Diskless Topics" accepted 2 Mar 2026 as a motivational KIP. Implementation is in follow-up KIPs 1163/1164 [6][7]. Inkless is a public fork that accepts no outside patches [8]. | Inkless uses a PostgreSQL batch coordinator [9]. | Validates our design (Postgres coordinator). Also shows its limit: commits rise from 10–20 ms to 100– 200 ms at 500–1000+ partitions [9]. A 1 GiB/s benchmark reported e2e P50 ≈ 650 ms and P99 ≈ 1.5 s [10]. |
| Bufstream | Closed source. Sold to CoreWeave on 8 May 2026 and no longer publicly available [13]. | Kafka-compatible; S3 plus Postgres/ Spanner/etcd metadata; Iceberg archives [11]. | Commissioned a Jepsen analysis that found three safety issues, including lost acknowledged writes, all fixed by 0.1.3 [12]. Shows that public correctness testing is a selling point. |
| Redpanda | Community Edition is source-available under BSL 1.1 [15]. | Tiered Storage (local disk first). Cloud Topics in Redpanda Cloud use object storage as primary storage, with local disk only as a write buffer [14]. | Strong product, but not permissively licensed, and the diskless mode lives in Redpanda's cloud. |
| Apache Kafka Tiered Storage (KIP-405) | Apache-2.0 [23]. | Old segments move to object storage. The hot set is still replicated across AZs. | Cuts storage cost but not replication traffic. |
| Nisshi (née Tansu) | Apache-2.0, Rust. About 1.9k stars (27 Oct 2026) [16]. | Stateless Kafka-compatible broker with pluggable storage (Postgres, S3, SQLite, memory); Iceberg/Delta output [16]. | Closest open-source peer. Its pitch is pluggable storage plus lakehouse output. LakeWire should not compete on breadth. |
| Small hobby projects | e.g. klite (Go, Apache-2.0), camu (Go, AGPL-3.0) | Single-binary S3-backed logs | Shows that "diskless Kafka clone" alone no longer stands out. |

What this means. Postgres-as-coordinator is not novel: Inkless and Bufstream both use it. Kafka compatibility is table stakes. A from-scratch student project will not win on scale or features. It can win on transparency. Nobody in the open-source space ships per-release, reproducible correctness reports plus a published coordinator-scaling study, and the best-known public Jepsen report in this category was for a product that has since left the market [12][13].

## 4 Positioning and differentiation

One-liner: LakeWire: Kafka-compatible streaming with no broker disks. Just Postgres and a bucket, and every guarantee tested in public.

### 4.1 The angle: verifiable and simple, not big

| Pillar | What it means concretely | Why it is credible for a solo builder |
| --- | --- | --- |
| 1. Verified guarantees | A GUARANTEES.md page lists each promise (no lost acked writes, gap-free offsets, idempotent dedupe, and so on). Each promise links to the checker that tests it and the latest fault-injection report. A short nemesis suite runs in CI on every PR, and a long soak runs before every release. From v0.3, LakeWire also runs Jepsen's open-source Kafka workload [21] and publishes the results. | Kafka compatibility makes it possible to reuse Jepsen's existing Kafka test instead of writing one. Bugs found get published as a strength (Bufstream precedent [12]). |
| 2. Two dependencies only | Postgres + any S3 API (AWS S3, GCS interop, SeaweedFS, MinIO, local filesystem for dev). Redis is removed as a requirement: leases and epochs live in Postgres, and caching is in-process with peer sharing. | Fewer moving parts means fewer failure modes to test. Every team already runs Postgres. |
| 3. Transparent cost and latency | lakewire cost and a web calculator show $/GiB from public price sheets, with the formulas in the open. Benchmarks publish raw CSVs, hardware and configs. | Aiven shows that published numbers attract attention [9][10]. Honest numbers beat big claims. |
| 4. Coordinator research in the open | Inkless reports Postgres commit latency as the bottleneck at 500+ partitions [9]. LakeWire publishes its own measured curves for batched commits and coordinator sharding. | Builds directly on v1's Postgres focus. It is novel content for a blog post and HN. |
| 5. JVM-native developer experience | lakewire dev runs one Java process with embedded Postgres and a local filesystem store, plus a Testcontainers module and a Spring Boot test starter. | Plays to Sunil's Java/Spring strength. Most new peers are Rust or Go. |

### 4.2 Why this angle beats the alternatives

- "Cheapest diskless Kafka" loses to WarpStream and AutoMQ, which have teams and real production data [1][4].

- "Most features / lakehouse" loses to Nisshi on breadth [16], and Bufstream's version of it has gone private [13].

- "Verifiable and simple" is open. It needs depth rather than headcount, it creates content people share (failure reports, coordinator curves), and it is exactly what interviewers probe: "How do you know it's correct?"

### 4.3 Name

The name LakeWire was chosen on 27 Oct 2026: "lake" stands for object storage, "wire" for the Kafka wire protocol it speaks. GitHub, npm and web checks found no existing software called LakeWire. Still to do: verify and buy the domain (lakewire.dev / lakewire.io) and claim the GitHub org lakewire .

## 5 What changes from v1.0

| Area | v1.0 | v2.0 decision and reason |
| --- | --- | --- |
| Client API | Custom gRPC Produce/Fetch + Java SDK | Kafka wire-protocol subset is the data plane. Existing clients, CLIs and Spring Kafka work, and the Jepsen Kafka workload can be reused. gRPC is kept only for internal broker-to-broker cache RPCs. |
| On-object record format | Custom record encoding | Kafka RecordBatch v2 stored verbatim. Fetch is a byte copy plus a baseOffset patch, which is safe because the batch CRC covers attributes onward, not baseOffset [24]. |
| Delivery | At-least-once; duplicates on retry are documented | Idempotent producers (P0). Kafka ≥3.0 producers enable idempotence by default [20], so without it stock clients fail or need config changes. |

| Area | v1.0 | v2.0 decision and reason |
| --- | --- | --- |
| Leases | Redis lease + Postgres epoch | Postgres only (heartbeat row + epoch). Redis becomes an optional cache. One less dependency, and the Postgres clock is the single time source. |
| Object deletion | Retention deletes segments; object deletion unspecified | Reference-counted objects + sweeper + grace period (§9.1). Fixes the multi-partition object gap. |
| Coordinator | One transaction per segment; measure in week 4 | Batched/group commit, single round-trip SQL, latency lab on Day 3, sharding designed for v1.0 (§9.2). |
| Learning plan | 4 separate weeks before building | Just-in-time learning: 60–90 min each morning on the exact topic of the day. |
| Dev object store | MinIO | Pluggable: the MinIO community edition entered maintenance mode in Dec 2025 [22]. Default to the local-FS store in dev mode; CI tests against MinIO (pinned image) and SeaweedFS. |
| Admin UI | React dashboard (week 13) | Grafana dashboard JSON in v0.1. A small web console arrives in v0.3 (P1), after correctness work. |

## 6 Scope and priorities

P0 must ship in v0.1 (Day 20). P1 v0.2–v0.3 (weekends, Dec–Feb). P2 v1.0 or later, only if time allows. Every feature has a "fit" verdict so scope stays realistic for one person.

| Pri | Feature | Scope / acceptance | Fit verdict |
| --- | --- | --- | --- |
| P0 | Kafka protocol subset (headline) | ApiVersions, Metadata, Produce (acks=all/1), Fetch (long-poll, max.wait), ListOffsets, InitProducerId, CreateTopics, DeleteTopics. Must pass with the Apache Kafka Java client, kafka-console-* , and kcat. | Yes. Reuse kafka-clients ' generated message classes for encode/decode (spike on Day 1). |
| P0 | Idempotent producers | Producer ID + epoch + per-partition sequence. Duplicates return the original offset; out-of-order sequences are rejected. Dedupe happens inside the commit transaction. | Yes. Required for default clients [20]. |
| P0 | Postgres coordinator v2 | Batched multi-partition commit in one round-trip, epoch fencing, broker registry/heartbeats, LISTEN/NOTIFY wakeups for long-poll. | Yes. Core of the project. |
| P0 | Object lifecycle | Immutable objects ( If-None-Match: * where supported [19]), orphan GC, time-based retention, ref-counted deletion with grace. | Yes. |
| P0 | Verification v1 | Java fault-injection harness: kill/pause/partition/S3-and-PG latency nemeses. Checkers for invariants I1–I8 (§11). Short run in CI on every PR. | Yes. This is the differentiator. |
| P0 | DX: one-command start | lakewire dev (embedded Postgres + local FS), docker compose up (3 brokers + Postgres + S3 + Grafana), a CLI (topics, cost, verify). | Yes. |
| P0 | Observability base + cost calculator | Prometheus metrics (commit latency histogram, PUT/GET counts, cache hit ratio), Grafana JSON, lakewire cost . | Yes. Small. |
| P1 | Consumer groups (Kafka classic protocol) | FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch, List/DescribeGroups. Group state in Postgres; any broker can coordinate. | Yes, v0.2. In v0.1, consumers use assign() + manual offsets. |

| Pri | Feature | Scope / acceptance | Fit verdict |
| --- | --- | --- | --- |
| P1 | Zone-aware routing + shared cache | The client's zone comes from client.id suffix or config. Metadata returns same-zone brokers as "leaders". Object ranges are cached via consistent hashing within a zone, so each object is fetched from S3 about once per zone. | Yes, v0.2. Needed for the cost story. |
| P1 | Compaction | (a) Merge small objects; (b) rewrite low-utilization objects (live bytes < 50%); (c) key-based compaction later. | (a)(b) v0.3; (c) P2. |
| P1 | Jepsen Kafka workload + deterministic simulation | Run jepsen.tests.kafka [21] against a LakeWire cluster. DST of the broker+coordinator core with seeded scheduler and fake time/S3/PG. | v0.3. Strong signal. |
| P1 | OpenTelemetry tracing | Spans: produce → buffer → PUT → commit → ack; fetch → index → cache/S3. | Cheap with the OTel Java agent plus a few manual spans. |
| P1 | Helm chart | Broker Deployment + HPA on CPU, external Postgres/S3 secrets, ServiceMonitor. | v0.2. A full operator isn't needed: brokers are stateless. |
| P1 | Auth: SASL/PLAIN API keys | Keys hashed in Postgres, TLS listener. | v0.3. |
| P1 | Web console-lite | React (or server-rendered) pages: topics, partitions, lag, objects and GC, commit latency. | v0.3, after correctness work. |
| P2 | Iceberg/Parquet export ("topics as tables") | Append-only exporter writes Parquet + Iceberg commits (REST catalog). Key/value as binary columns first; schema-mapped columns after the schema registry. | v1.0 stretch. Nisshi already does this [16], so it's not our differentiator. |
| P2 | Multi-tenancy + quotas | Tenant prefix on topics, per-key byte-rate quotas (token bucket per broker + Postgres budget). | v1.0. |
| P2 | Schema registry-lite | Confluent-compatible subset (subjects, versions, compatibility BACKWARD) stored in Postgres. | Post-1.0 unless users ask. |
| P2 | Coordinator sharding | Partitions hashed to N Postgres shards (§9.2). | Design + experiment in v1.0. |
| P2 | Spring Boot starter | Spring Kafka already works over the protocol, so the starter becomes lakewire-spring-boot-testcontainers (dev service + @ServiceConnection ). | Small; v1.0. |
| P2 | Kubernetes operator | Only if the Helm chart proves insufficient. | Likely never. Stateless brokers don't need one. |

## 7 Architecture

<!-- Figure 1 transcribed as text: original box layout and arrow geometry are not reproduced. -->

**Figure 1 arrow labels:** 1 produce; 2 PUT; 3 commit; 4 high-watermark / segment-index lookup; 5 range GET; 6 fetch response.

**AVAILABILITY ZONE A**

- **Kafka clients**
  - Java client · Spring Kafka
  - franz-go · librdkafka
  - kcat · console tools
  - `client.id=…,az=a`
- **Stateless brokers (Java 21)**
  - Kafka protocol server (Netty, :9092)
  - Write buffer → uploader → committer
  - Fetch + zone cache (gRPC peer ring)
  - Producer-ID / group coordinator logic

**AVAILABILITY ZONE B (C similar)**

- **Kafka clients**
  - producers / consumers in zone B
  - `client.id=…,az=b`
- **Stateless brokers (Java 21)**
  - same binary · any broker serves any partition
  - Metadata returns same-zone brokers as leaders
  - so client↔broker traffic never crosses a zone
  - scale on CPU · kill any broker any time

**Object storage (any S3 API)**

- AWS S3 · GCS · SeaweedFS · MinIO
- local filesystem in dev mode
- immutable multi-partition objects (Kafka RecordBatch v2, verbatim)
- durable across zones by the provider

**PostgreSQL coordinator**

- source of truth: offsets, HW, segment index, object refcounts, producer state, epochs, groups
- batched commit · LISTEN/NOTIFY
- (v1.0: N shards by partition hash)

**Background workers**

- orphan GC · retention · ref sweeper
- compactor (v0.3) · Iceberg export (P2)
- leader-elected via PG advisory lock

Figure 1. LakeWire v2 architecture. ① produce → buffer (linger/size) → ② one PUT per object holding many partitions → ③ one Postgres transaction checks the epoch, dedupes idempotent sequences, assigns offsets, then acks. ④ high-watermark / segment-index lookup (NOTIFY wakes long-polls) → ⑤ range GET through the same-zone cache → ⑥ fetch response.

### 7.1 Components

| Component | Responsibility | Tech |
| --- | --- | --- |
| Protocol server | Kafka framing, ApiVersions negotiation, request routing; one virtual thread per in-flight request; per-connection ordering preserved. | Netty 4, Java 21 virtual threads, kafka-clients message classes |
| Write path | Per-broker buffer across all partitions, flushed on linger.ms (default 200) or object.max.bytes (default 8 MiB, tune 4–8 based on [9]); bounded concurrent uploads; backpressure when the buffer is full. | AWS SDK v2 (async), ObjectStore SPI |
| Committer | Calls sl_commit_object() ; optional group commit of several ready objects in one transaction; maps results to Produce responses. | JDBC (pgjdbc) + HikariCP |
| Read path | HW check → segment index (cached; immutable entries) → zone cache → S3 range GET; patches baseOffset ; long-poll on NOTIFY. | Caffeine cache, gRPC peer ring (v0.2) |
| Broker registry | Register on start (epoch = epoch + 1), heartbeat every 2 s, lease 10 s, measured on the Postgres clock ( now() ), so broker clock skew doesn't matter. | Postgres |
| Workers | Orphan GC, retention, ref sweeper, compactor, exporter; single active instance via pg_try_advisory_lock . | Runs inside any broker or as lakewire worker |
| Admin plane | REST admin, CLI, Grafana dashboard; console later. | Spring Boot 3 (admin module only), picocli |

### 7.2 Object format (LKW1)

```text
s3://{bucket}/{prefix}/v2/{yyyy}/{mm}/{dd}/{hh}/{broker}-{epoch}-{uuidv7}.lkw
Header : magic "LKW1" | format_version u8 | broker_id | broker_epoch u64 | created_ms u64
Body   : per-partition chunks, each = 1..n Kafka RecordBatch v2, stored byte-for-byte
         (baseOffset = 0 on disk; real offsets are assigned at commit and patched on read)
Index : repeated { topic_id u64, part u32, byte_start u64, byte_len u32,
                    n_records u32, n_batches u16, max_ts i64, crc32c u32 }
Footer : index_offset u64 | index_crc32c u32 | magic "LKW1"
```

Why verbatim batches: Fetch needs no re-encoding, compression stays client-side, and the Kafka batch CRC excludes baseOffset [24], so patching offsets on read keeps client-side CRC checks valid. The index lets workers and debug tools read an object without Postgres.

## 8 Data model (PostgreSQL, Flyway-managed)

Changes vs v1 are marked NEW or CHANGED. All coordinator writes go through a small set of SQL functions, so the invariants live in one reviewable place.

```sql
CREATE TABLE topics (id BIGSERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL, partitions INT NOT NULL,
  config JSONB NOT NULL DEFAULT '{}',            -- CHANGED: retention.ms/bytes, cleanup.policy
  tenant TEXT NOT NULL DEFAULT 'default',        -- NEW (P2 multi-tenancy)
  created_at TIMESTAMPTZ DEFAULT now());

CREATE TABLE partitions (topic_id BIGINT REFERENCES topics, part INT,
  next_offset BIGINT NOT NULL DEFAULT 0,          -- = high watermark (committed, visible)
  log_start_offset BIGINT NOT NULL DEFAULT 0,
  coord_shard SMALLINT NOT NULL DEFAULT 0,        -- NEW (sharding, v1.0)
  PRIMARY KEY (topic_id, part));

CREATE TABLE brokers (id TEXT PRIMARY KEY, zone TEXT NOT NULL, endpoint TEXT NOT NULL,
  epoch BIGINT NOT NULL, lease_expires_at TIMESTAMPTZ NOT NULL);   -- CHANGED: lease moved from Redis

CREATE TABLE upload_intents (object_key TEXT PRIMARY KEY,          -- NEW: closes GC-vs-late-commit race
  broker_id TEXT NOT NULL, broker_epoch BIGINT NOT NULL, created_at TIMESTAMPTZ DEFAULT now());

CREATE TABLE objects (id UUID PRIMARY KEY, key TEXT UNIQUE NOT NULL, size_bytes BIGINT NOT NULL,
  broker_id TEXT, broker_epoch BIGINT,
  live_refs INT NOT NULL,                          -- NEW: segments still pointing here
  live_bytes BIGINT NOT NULL,                      -- NEW: for compaction utilisation
  state TEXT NOT NULL CHECK (state IN ('COMMITTED','DELETE_PENDING')),
  delete_after TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT now());

CREATE TABLE segments (topic_id BIGINT, part INT, base_offset BIGINT, last_offset BIGINT,
  object_id UUID NOT NULL REFERENCES objects, byte_start BIGINT, byte_len INT,
  n_batches SMALLINT, max_ts BIGINT, PRIMARY KEY (topic_id, part, base_offset));
CREATE INDEX ON segments (object_id);

CREATE TABLE producer_ids (producer_id BIGSERIAL PRIMARY KEY,       -- NEW: idempotence
  producer_epoch SMALLINT NOT NULL DEFAULT 0, last_used TIMESTAMPTZ DEFAULT now());
CREATE TABLE producer_state (producer_id BIGINT, topic_id BIGINT, part INT,   -- NEW
  producer_epoch SMALLINT NOT NULL, last_seq INT NOT NULL,
  recent JSONB NOT NULL,   -- last 5 batches: [{first_seq,last_seq,base_offset}]
  updated_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY (producer_id, topic_id, part));

-- v0.2: consumer_groups(group_id PK, generation, protocol_type, protocol, leader, state)
--       group_members(group_id, member_id, client_id, session_timeout_ms, metadata BYTEA, assignment BYTEA)
--       group_offsets(group_id, topic_id, part, committed, metadata, generation, committed_at)
-- v0.3: api_keys(id, tenant, secret_hash, created_at, revoked_at)
```

Producer state expiry: rows untouched for producer.state.ttl (default 7 days) are deleted by a worker; a producer that returns afterwards gets UNKNOWN_PRODUCER_ID and re-initialises, which matches Kafka client behaviour.

## 9 Design fixes: object lifecycle, coordinator, visibility

### 9.1 Multi-partition objects: delete only when nothing references them

The gap in v1. One object holds chunks from many partitions with different retention settings. Retention deletes segments, and an object can only be deleted when no segment references it. Compaction has the same issue: rewriting some partitions out of an object must not delete it while other partitions still point at it.

<!-- Figure 2 transcribed as text: original boxes and arrow styling are not reproduced. -->

```text
INTENT → COMMITTED → DELETE_PENDING → GONE
```

| State | Detail | Transition to next state |
| --- | --- | --- |
| INTENT | upload_intents row | PUT ok + commit txn |
| COMMITTED | live_refs = k > 0 | retention / compaction drop last segment |
| DELETE_PENDING | live_refs = 0, grace timer | now() > delete_after |
| GONE | S3 DELETE, row removed | — |

Orphan GC (INTENT → GONE): intent older than TTL is deleted first (DELETE … RETURNING), then the object; a late commit finds no intent and fails

Figure 2. Object lifecycle. Segment deletion and the live_refs decrement happen in the same transaction.

1. Reference counting in the same transaction. Retention ( DELETE FROM segments … RETURNING object_id, byte_len ) and compaction swaps decrement objects.live_refs / live_bytes in the transaction that removes the references. When live_refs hits 0, set DELETE_PENDING and delete_after = now() + grace .

2. Grace period (default 5 min, longer than the maximum fetch duration). A fetch that has already resolved an index entry can finish its range GET before the object disappears. If a reader still gets a 404, it reloads the index and retries once.

3. Sweeper as a safety net. A periodic audit checks live_refs = (SELECT count(*) FROM segments WHERE object_id = …) and alerts on any mismatch (invariant I7). It never deletes on its own conclusion without the state machine.

4. Upload intents close a subtle race found while designing v2: broker uploads → pauses longer than the orphan TTL → GC deletes the "orphan" → broker wakes and commits → an acknowledged write points at a missing object. With intents, GC deletes the intent row first, and sl_commit_object requires the intent to exist (plus a valid epoch). This is a good blog/interview story.

5. Space amplification is tracked as 1 − live_bytes/size_bytes . In v0.3, the compactor rewrites objects below 50% utilisation and merges small objects.

### 9.2 Coordinator latency and scaling

Aiven reports that Postgres commit latency becomes the bottleneck as partition counts grow (10–20 ms → 100–200 ms at 500–1000+ partitions) [9]. LakeWire treats this as the #1 technical risk and measures it on Day 3, before building on top.

| Technique | How | When |
| --- | --- | --- |
| Single round-trip commit | sl_commit_object(...) SQL function takes arrays ( unnest ), locks partition rows in (topic_id, part) order, and returns per-batch offsets or errors. One network round trip per object. | P0, Day 2–3 |
| Set-based offset assignment | UPDATE partitions p SET next_offset = p.next_offset + d.n FROM unnest(...) d … RETURNING , compared against a per-row FOR UPDATE loop in the Day 3 lab. | P0 |
| Group commit | The committer merges objects that become ready within ≤ 5 ms into one transaction, which amortises fsync and round trips at high broker counts. | P0 (flag), tuned in v0.2 |
| Fewer, bigger objects | Zone-aware routing concentrates a partition's producers on fewer brokers. Linger and size trade latency for commit rate. | v0.2 |

| Technique | How | When |
| --- | --- | --- |
| Coordinator sharding | coord_shard = hash(topic, part) mod N , with each shard a separate Postgres. Brokers keep one buffer per shard, so every object commits to exactly one shard and no 2PC is needed. Cost: up to N× more PUTs, which the cost model reports. | P2, v1.0 experiment |

Day-3 lab deliverable: bench/coordinator/ with a CSV and chart of commit P50/P99 against partitions per commit (1, 16, 128, 512, 1024) × concurrent committers (1, 3, 6, 12) × strategy. The result feeds ADR-001. It becomes the first blog post section.

### 9.3 Fetch visibility rules

- Visible = committed. The high watermark is partitions.next_offset . A broker never returns data at or above the HW it read from Postgres. Uploaded-but-uncommitted objects are invisible by construction.

- Fresh HW on the edge. Brokers cache HW, updated by NOTIFY sl_hw . Postgres delivers notifications only when the transaction commits, so a NOTIFY never announces uncommitted data. When a fetch offset is ≥ the cached HW, the broker re-reads HW from Postgres before deciding between "wait" (long-poll) and OFFSET_OUT_OF_RANGE . A stale cache can only delay data, never hide or invent it.

- Read-your-acked-writes: after a Produce ack, a fetch on any broker at that offset returns the data, or waits ≤ max.wait.ms and then returns it (checked by invariant I1/I4).

- Below log start → OFFSET_OUT_OF_RANGE (Kafka semantics). Index entries are immutable, so they're safely cacheable. Compaction swaps create new entries, and stale entries are caught by the 404-reload rule.

## 10 APIs

### 10.1 Kafka protocol subset (data plane, port 9092)

| API | Release | LakeWire behaviour |
| --- | --- | --- |
| ApiVersions | v0.1 | Advertises only the implemented APIs and version ranges that the current Java client, librdkafka and franz-go negotiate. Ranges are pinned by compatibility tests rather than guessed. |
| Metadata | v0.1 | Every partition's "leader" is a live broker in the client's zone (from client.id suffix az=… , otherwise a hash). Leadership is a routing hint, not ownership; any broker can write any partition. |
| Produce | v0.1 | acks=all / 1 both mean "durable in S3 and committed in Postgres". acks=0 is accepted best-effort. Idempotent batches are deduped. Errors are mapped to retriable Kafka codes. |
| Fetch | v0.1 | Long-poll with max.wait.ms / min.bytes , woken by NOTIFY. Batches are returned verbatim with patched baseOffset . No fetch sessions in v0.1 (full fetches only). |
| ListOffsets | v0.1 | Earliest/latest. Timestamp lookup uses segments.max_ts . |
| InitProducerId | v0.1 | Allocates producer ID/epoch from producer_ids . Transactional IDs are rejected (non-goal). |
| CreateTopics / DeleteTopics | v0.1 | Delete marks the topic, and retention-style cleanup releases refs. |
| FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch | v0.2 | Classic group protocol, with group state in Postgres and any broker acting as coordinator (one active per group via a row lock and generation). |

| API | Release | LakeWire behaviour |
| --- | --- | --- |
| DescribeConfigs, ListGroups, DescribeGroups, SaslHandshake/ Authenticate | v0.3 | Enough for kafka-ui-style tools and SASL/PLAIN with API keys. |

Error mapping: stale broker epoch / coordinator unavailable → NOT_LEADER_OR_FOLLOWER or REQUEST_TIMED_OUT (clients refresh and retry). Sequence gap → OUT_OF_ORDER_SEQUENCE_NUMBER . Unknown or expired producer → UNKNOWN_PRODUCER_ID . Fenced producer epoch → INVALID_PRODUCER_EPOCH . Unsupported API/version → UNSUPPORTED_VERSION . Protocol reference: kafka.apache.org/protocol.

### 10.2 Admin REST (port 8080)

```text
GET    /api/v1/cluster                                  brokers, zones, epochs
GET    /api/v1/topics                                   list + HW + bytes
POST   /api/v1/topics                                   {name, partitions, config}
DELETE /api/v1/topics/{name}
GET    /api/v1/topics/{name}/partitions/{p}/segments
GET    /api/v1/objects/{id}                              refs, utilisation, state
GET    /api/v1/groups                                   (v0.2)
GET    /api/v1/cost?gib_per_day=…&zones=3
POST   /api/v1/verify/audit                              ref-count + leak audit
GET    /actuator/prometheus                             metrics
```

### 10.3 Internal gRPC (port 9093)

```proto
service PeerCache {                      // v0.2, zone-local
  rpc GetRange(RangeRequest)
    returns (stream Chunk);
}
message RangeRequest {
  string object_key = 1;
  int64 byte_start = 2;
  int32 byte_len = 3;
}
service BrokerAdmin {
  rpc Drain(DrainRequest) returns (Ack);
  rpc DumpState(Empty) returns (State);
}
```

### 10.4 CLI (picocli, GraalVM native image later)

```bash
lakewire dev                      # embedded Postgres + local FS + broker on :9092
lakewire topics create orders -p 12 --retention 7d
lakewire cost --gib-per-day 500 --zones 3 --retention 7d   # Kafka RF=3 vs LakeWire vs tiered
lakewire verify --nemesis kill,pause,partition-pg --duration 10m --seed 42
lakewire inspect object s3://bucket/v2/…/b1-7-0192….lkw # decode index without Postgres
```

## 11 Correctness: invariants, nemeses, verification

Correctness is the product. Every release publishes a Correctness Report listing the checker version, nemeses, number of runs, seeds and any violations found and fixed. A bug found by our own harness counts as a feature of the process.

| ID | Invariant | How it is checked |
| --- | --- | --- |
| I1 | Every acknowledged record is eventually readable at its acked offset, with identical bytes. | Workload logs acks (key, value hash, offset); a final full read compares them. |
| I2 | Offsets per partition are unique and contiguous from log start to HW. | Full read + segment-table scan (no overlaps or holes). |
| I3 | Per-producer, per-partition order is preserved. | Sequence numbers in the payload are monotonic in read order. |
| I4 | No consumer ever observes an offset ≥ committed HW at fetch time, or unacked or uncommitted data that later disappears. | Consumer histories compared with the final log (no "phantom then gone"). |

| ID | Invariant | How it is checked |
| --- | --- | --- |
| I5 | An idempotent producer's retries never create duplicates. | Unique (producer, seq) per partition in the final log under injected timeouts. |
| I6 | Committed group offsets per group/ partition are monotonic per generation. (v0.2) | OffsetCommit history checker. |
| I7 | No object referenced by a segment is deleted. | Audit: each segment's object exists (HEAD); live_refs equals the actual count. |
| I8 | No leaked objects: every object without a row is younger than orphan TTL + GC period. | Bucket listing vs objects + upload_intents . |

### 11.1 Nemeses (fault injectors)

| Nemesis | Mechanism | Targets |
| --- | --- | --- |
| Broker kill / restart | docker kill -s KILL during upload and during commit | I1, I5, I8 |
| Broker pause (zombie) | docker pause / SIGSTOP longer than lease and orphan TTL, then resume | Epoch fencing, upload-intent race, I1, I7 |
| Partition broker↔Postgres | Toxiproxy timeout / reset_peer toxics | Ambiguous commits ("did my txn commit?"), I1, I5 |
| Slow / failing S3 | Toxiproxy latency, plus a fault-injecting ObjectStore wrapper (5xx, slow PUT, 404 on GET) | Backpressure, retries, I8 |
| Postgres restart / failover | Restart the container; in v0.3, a Patroni-style replica promotion | Recovery, I1–I4 |
| Retention + compaction storm | Very short retention while reading old offsets | Grace period, 404-reload, I7 |

Clock skew is deliberately not a nemesis: every lease, TTL and grace decision uses the Postgres clock, which is a design choice worth explaining in the docs. Ambiguous commits are handled by recording the object key in the transaction. On reconnect, the committer checks whether its object row exists before retrying.

### 11.2 Verification ladder

1. Unit + property tests (jqwik) for the object codec, offset patching and the commit function (randomised arrays against a model).

2. Fault harness v1 (Day 11, P0): Java workload + Testcontainers + Toxiproxy. A 3-minute smoke runs on every PR; a nightly 30-minute run uses random seeds. Seeds are logged so failures can be replayed.

3. Linearizability of the coordinator (v0.2): record the history of commit/read operations and check it with Porcupine (Go) or Knossos-style checking. Treat "offset assignment per partition" as a register/queue model.

4. Jepsen Kafka workload (v0.3, P1): run Jepsen's jepsen.tests.kafka workload [21] against the compose cluster. It detects anomalies such as lost writes, duplicates, and non-monotonic or inconsistent offsets. Publish the run with the exact commit hash. Jepsen found real issues in Bufstream 0.1.0 [12], so this is a credible bar.

5. Deterministic simulation testing (v0.3 experiment): run broker logic with a simulated clock, network, S3 and Postgres model in a single thread under a seeded scheduler. Start with the write path and GC (where the races live). Seeds reproduce failures exactly.

## 12 Non-functional targets and cost model

| Metric | Target (v0.1, laptop) | Notes |
| --- | --- | --- |
| Produce latency (acks=all) | P50 ≤ 300 ms, P99 ≤ 800 ms | Dominated by linger + PUT. Real S3 will differ; report both. Aiven's analysis gives typical diskless numbers [9]. |
| End-to-end latency (produce → consume) | P99 ≤ 1.5 s | NOTIFY wakeups avoid polling delay. |
| Commit latency (coordinator) | P99 < 100 ms at 128 partitions/commit | Measured on Day 3, then re-measured each release. |
| Throughput | ≥ 50 MiB/s on one laptop (3 brokers) | Limited by local S3 emulation; a real-S3 run is optional and budgeted. |
| Recovery after broker kill | Clients resume ≤ 15 s | Lease 10 s + metadata refresh. |
| Correctness | 0 invariant violations / ≥ 100 nemesis runs | Published per release. |
| Startup | lakewire dev ready ≤ 10 s | Embedded Postgres + local FS. |

All numbers above are targets, not results. Benchmark reports will publish raw CSVs, hardware, versions and scripts.

### 12.1 Cost model (what lakewire cost computes)

The model uses AWS list prices: inter-AZ transfer is $0.01/GB in each direction [17], so each GB crossing a zone costs $0.02. S3 Standard (us-east-1) costs $0.005 per 1,000 PUTs, $0.0004 per 1,000 GETs and $0.023/GB-month for the first tier [18]. Prices change, so the calculator reads them from a config file.

| Per GiB written (3 zones) | Classic Kafka RF=3 | LakeWire (8 MiB objects) | Derivation |
| --- | --- | --- | --- |
| Producer → leader cross-AZ | ≈ $0.0133 | ≈ $0 | Kafka: 2/3 of producers hit a remote leader. LakeWire: zone-local broker. |
| Replication | ≈ $0.0400 | $0 (S3 replicates) | 2 follower copies × $0.02 |
| S3 PUTs | — | ≈ $0.00064 | 128 objects/GiB × $0.005/1k |
| S3 GETs (one read per zone) | — | ≈ $0.00015 | 3 × 128 × $0.0004/1k (zone cache absorbs the rest) |
| Transfer + request total | ≈ $0.053 | ≈ $0.0008 | Matches WarpStream's published $0.053/GiB figure for Kafka [1] |
| Storage (per GB-month) | EBS × 3 replicas + headroom | $0.023 × 1 | Shown separately; depends on retention |

Honest caveats the calculator prints. Consumer cross-AZ traffic depends on fetch-from-follower setup. Postgres (e.g. a managed instance) is a fixed monthly cost that dominates at low volume. Small objects (low traffic + short linger) raise PUT costs, and the per-shard buffers in a sharded coordinator multiply PUTs. Latency is higher than disk-based Kafka.

## 13 20-day sprint (Wed 28 Oct → Mon 16 Nov 2026)

Each day is about 8–10 focused hours. Learn = what to read first (≤ 1 h); Done when = an objective check, committed to the repo. Milestone rows are highlighted. If a day slips, cut from P0 polish (console-free, fewer docs pages), never from verification.

| Day | Learn | Build | Done when |
| --- | --- | --- | --- |
| D1 · Wed 28 Oct | Kafka protocol guide; kafka-clients message classes | Gradle multi-module repo, CI (build + test), compose (Postgres + S3 emulator), Flyway V1. Spike: decode ApiVersions with Kafka's generated classes. Verify and buy the domain (§4.3). | kcat -L reaches the server and gets ApiVersions back |
| D2 · Thu 29 Oct | Postgres locking, unnest , MVCC | sl_commit_object() v1 + epoch check + upload intents; concurrency tests with 8 threads × random partitions. | No duplicate or missing offsets in 10k concurrent commits |
| D3 · Fri 30 Oct | Aiven diskless deep-dive [9] | Coordinator latency lab: FOR UPDATE loop vs set-based vs group commit; 1–1024 partitions; 1–12 committers. ADR-001. | CSV + chart in bench/coordinator ; strategy chosen |
| D4 · Sat 31 Oct | S3 conditional writes [19]; RecordBatch format [24] | ObjectStore SPI (S3, local FS, fault-injecting wrapper); LKW1 writer/reader + index; property tests. | Round-trip property test passes 10k cases |
| D5 · Sun 1 Nov | Produce request semantics | Metadata (single broker) + Produce path: buffer → upload → commit → ack; backpressure. | kafka-console-producer writes; rows visible in PG |
| D6 · Mon 2 Nov | Fetch long-poll semantics | Fetch + ListOffsets, baseOffset patching, NOTIFY wake-up, LRU block cache. | kafka-console-consumer reads what was produced |
| D7 · Tue 3 Nov MILESTONE 1 | — | E2E tests with the Java client; I1/I2 checkers; record a 60-s demo GIF; write up the coordinator report. | Stock Kafka tools produce/consume via LakeWire; data only in S3, offsets only in PG |
| D8 · Wed 4 Nov | KIP-679 [20]; idempotent producer design | InitProducerId, producer_state dedupe in commit txn, error mapping. | Default Java producer (idempotence on) works; dup retries return the original offset |
| D9 · Thu 5 Nov | Leases and fencing | Multi-broker: registry, heartbeats, epochs, zone-aware Metadata. SIGSTOP zombie test. | 3 brokers; paused broker's late commit is rejected |
| D10 · Fri 6 Nov | — | Orphan GC, time retention, ref-counted deletion + grace + 404-reload; I7/I8 audit. | Retention storm test: I7/I8 green |
| D11 · Sat 7 Nov | Jepsen Kafka workload docs [21] | Fault harness v1: Toxiproxy, kill/pause/partition nemeses, checkers I1–I5, seed replay. | lakewire verify runs a 10-min job end-to-end |
| D12 · Sun 8 Nov | Diwali: light day. | Start an overnight soak (randomised nemesis seeds), triage only. | Soak started; issues filed |
| D13 · Mon 9 Nov | — | Fix soak findings; backpressure; Kafka error codes; ambiguous-commit handling. | 50 consecutive nemesis runs green |
| D14 · Tue 10 Nov MILESTONE 2 | — | Perf scripts (producer/consumer perf tools), Prometheus + Grafana dashboard, lakewire cost . | 3-broker cluster survives faults with checkers green; first benchmark CSVs |
| D15 · Wed 11 Nov | picocli, embedded Postgres | CLI, REST admin, lakewire dev . | Fresh clone → running in ≤ 2 commands |
| D16 · Thu 12 Nov | Diátaxis docs model | Docs site (quickstart, concepts, architecture, ADRs, ops) + README. | Docs deployed on GitHub Pages |

| Day | Learn | Build | Done when |
| --- | --- | --- | --- |
| D17 · Fri 13 Nov | — | Benchmark matrix; benchmark report + correctness report; optional real-S3 run with a hard budget cap. | Reports with raw data committed |
| D18 · Sat 14 Nov | — | Launch blog post, 3-min demo video, landing page. | Drafts reviewed by 2 friends |
| D19 · Sun 15 Nov | — | Release engineering: GHCR image, JReleaser, CONTRIBUTING, SECURITY, CODE_OF_CONDUCT, 10 good-first-issues, clean-machine dry run. | Someone else completes the quickstart unaided |
| D20 · Mon 16 Nov LAUNCH v0.1 | — | Tag v0.1.0, repo public, Show HN ≈ 8:00 PM IST (≈ 6:30 AM US Pacific), then Reddit / X / LinkedIn / dev.to; answer every comment for 48 h. | v0.1.0 released; launch posts live |

## 14 Weekend roadmap after launch (2 days/week)

After the holiday: 5 class days per week, 2 full days for LakeWire (≈ 16–18 h/week). Dates are planning anchors, and the plan is aggressive. If a release slips, move P2 items, not verification.

| Window | Release | Scope | Exit criteria |
| --- | --- | --- | --- |
| 21 Nov – 6 Dec (3 weekends) | v0.1.x patches | Triage launch feedback, fix client-compat bugs (librdkafka, franz-go), merge first external PRs, write the "upload-intent race" post. | Issue response < 48 h; compat matrix in CI |
| 12 Dec – 27 Dec (3 weekends) | v0.2 "Groups & Zones" | Consumer groups (classic protocol), zone-aware peer cache (gRPC), Helm chart, OpenTelemetry traces across produce → PUT → commit, group-commit tuning, coordinator linearizability check. | Spring Kafka app with consumer groups passes nemesis suite incl. I6 |
| Jan 2027 (≈ 2 weeks) | Buffer | End-semester exams (dates TBC). Only issue triage. | — |
| 23 Jan – 14 Feb (4 weekends) | v0.3 "Verified" | Jepsen jepsen.tests.kafka run [21], DST experiment on write path + GC, object compaction, SASL/PLAIN + API keys, console-lite (topics, groups, cost, last correctness run). | Published Jepsen-workload report; compaction keeps I7 green |
| 20 Feb – 14 Mar (4 weekends) | v1.0 | Iceberg export (topic → Parquet → Iceberg table), quotas / tenant isolation, LKW1 format freeze + compatibility tests, real-S3 benchmark report, coordinator sharding experiment. | Upgrade-compat test v0.3 → v1.0; 1 external production-like user |

Weekend rhythm. Saturday: deep build (one feature, behind a flag). Sunday morning: verification and fixes. Sunday evening: a weekly changelog post, triage and one "good first issue" written. A public cadence is itself a startup signal.

## 15 Open-source launch plan

### 15.1 Repository layout

```text
lakewire/
├─ lakewire-protocol/    Kafka codec glue
├─ lakewire-broker/      Netty server, write/read path
├─ lakewire-coordinator/ SQL functions, Flyway
├─ lakewire-storage/     ObjectStore SPI, LKW1
├─ lakewire-workers/     GC, retention, compactor
├─ lakewire-admin/       Spring Boot REST
├─ lakewire-cli/         picocli
├─ lakewire-testkit/     Testcontainers module
├─ verify/              nemeses, checkers, seeds
├─ bench/               coordinator, e2e, cost
├─ deploy/              compose/, helm/
├─ console/             (v0.3)
└─ docs/                site, adr/, reports/
```

### 15.2 License: Apache-2.0

- Explicit patent grant, which companies' legal teams look for.
- Same license as Apache Kafka and kafka-clients, which we depend on, so no friction.
- Peers' choice: AutoMQ moved to Apache-2.0 [5]; Nisshi is Apache-2.0 [16]. Redpanda's BSL [15] shows the trust cost of source-available licensing.
- Maximises adoption, the only thing that matters at stage zero. A startup can later monetise a hosted/managed layer (open core) without relicensing.
- Add a NOTICE file: "Apache Kafka® is a trademark of the ASF; LakeWire is not affiliated." Use "Kafka-compatible", never "Kafka" in the product name.

### 15.3 README outline (first screen decides everything)

1. One-liner + badges (CI, correctness report, license, release) + 20-second demo GIF.

2. "Why": the 3-AZ cost table (§12.1) in 4 rows, then "honest trade-off: higher latency".

3. Quickstart: docker run … lakewire dev → kcat produce/consume (3 commands).

4. How it works: Figure 1 + 5 bullets. Links to architecture docs and ADRs.

5. Correctness: invariants list, latest report link, "how to run the nemesis suite yourself".

6. Compatibility matrix (clients × APIs), status and roadmap, comparison table with honest notes, contributing, license.

### 15.4 Docs, contributing, community

| Asset | Content |
| --- | --- |
| Docs site | Diátaxis structure: Tutorials (quickstart, Spring Boot app), How-to (Helm, S3 providers, tuning linger), Reference (config, metrics, REST, CLI), Explanation (architecture, ADRs, correctness model, cost model). |
| CONTRIBUTING.md | Dev setup in 2 commands, module map, "run verify before PR", DCO sign-off, commit style, how to add a nemesis or checker. |
| Good first issues (10 at launch) | Add a Kafka error-code mapping test; a CLI topics describe command; a Grafana panel for cache hit ratio; a GCS/Azure ObjectStore adapter; doc translations; a librdkafka compat test; a lakewire inspect pretty-printer; a cost-config for GCP; a jqwik generator for record batches; a Helm values doc. |
| Community | GitHub Discussions (no Discord until there are users), a public roadmap project board, a weekly changelog, and every bug found by the harness turned into a short write-up. |

### 15.5 Launch channels and assets

| Channel | Angle / timing |
| --- | --- |
| Show HN | "Show HN: LakeWire – a Kafka-compatible log on S3 + Postgres, with a published fault-injection report". Mon 16 Nov ≈ 8:00 PM IST. Founder comment ready: why, trade-offs, what's not done. |

| Channel | Angle / timing |
| --- | --- |
| Reddit | r/apachekafka, r/java, r/dataengineering, r/programming: a different post each, spaced over 3 days, following each sub's rules. |
| X / LinkedIn / dev.to | Thread: "I found a data-loss race in my own design before writing code" (upload-intent story) + architecture diagram + demo video. |
| Newsletters & podcasts | Pitch to data-engineering and Java newsletters after 2 weeks of traction. |
| Assets | Benchmark report (raw CSV), correctness report, 3-min video (problem → demo → kill a broker live → checkers green), landing page with the cost calculator. |

## 16 Startup signals and career angle

### 16.1 Make it look (and be) like a company

- Landing page: one-liner, interactive cost calculator, "Verified" badge linking to the latest correctness report, waitlist ("managed LakeWire").

- Public metrics page: stars, contributors, downloads (GHCR pulls), open/closed issue time.

- Weekly changelog and a public roadmap board.

- Design partners: 2–3 student teams or small startups running it for side projects; their feedback becomes issues.

- Write a one-page "investor memo" (problem, wedge, why now: diskless is the industry's direction [6][7], open-core path).

### 16.2 Resume and interview angle

Fill in the bracketed numbers only from published reports:

- "Built LakeWire, an open-source Kafka-compatible streaming log on S3 + PostgreSQL (Java 21, Netty). It works with unmodified Kafka clients and Spring Kafka."

- "Designed a single-round-trip multi-partition commit protocol with idempotent dedupe; measured commit P99 of [X] ms at [N] partitions."

- "Built a fault-injection harness (8 invariants, 6 nemeses) that found [K] bugs before release, including a GC-vs-late-commit data-loss race."

- "Launched publicly: [S] GitHub stars, [C] contributors in [M] months."

Interview story: the trade-off of disk vs object storage, the commit-latency bottleneck, and how invariants and nemeses changed the design.

## 17 Risks and mitigations

| Risk | Likelihood | Mitigation |
| --- | --- | --- |
| Scope overload (one person, 20 days) | High | P0 list frozen on D1; daily "done when" checks; cut docs and polish before verification; groups deferred to v0.2. |
| Kafka protocol complexity / client quirks | High | Reuse kafka-clients generated message classes; minimal API set; compat tests per client in CI; return UNSUPPORTED_VERSION cleanly. |
| Postgres commit latency at scale | Medium | Measured on Day 3 (ADR-001); set-based commit + group commit; sharding design ready (§9.2); publish limits honestly. |
| Subtle correctness bugs (GC, fencing, ambiguous commits) | Medium | Upload intents, Postgres-clock leases, the nemesis suite on every PR, later Jepsen and DST. |
| Burnout / exams after the holiday | Medium | Weekend-only cadence, exam buffer in January, Diwali light day, no promises of dates in public beyond "next release". |
| Domain not yet secured | Low | Verify and buy lakewire.dev / lakewire.io and claim the GitHub org on D1 (§4.3). |
| Local S3 choice (MinIO community changes [22]) | Low | Pluggable ObjectStore; local FS for dev; test against at least two S3-compatible stores. |
| Real-S3 benchmark cost | Low | Hard budget cap, short runs, billing alerts, teardown script; laptop results labelled as such. |

| Risk | Likelihood | Mitigation |
| --- | --- | --- |
| "Why not just use WarpStream / AutoMQ / Inkless?" | Certain | Positioning is not "better at scale" but "the verifiable, readable one". Comparison table cites sources and states where others are stronger. |

## 18 Success metrics

| Area | v0.1 (Day 20) | v0.3 (Feb 2027) | v1.0 (Mar 2027) |
| --- | --- | --- | --- |
| Correctness | 0 violations / ≥ 100 runs; I1–I5, I7, I8 | Jepsen Kafka workload report published | DST on write path + GC; all I1–I8 |
| Compatibility | Java client, console tools, kcat | + librdkafka, franz-go, Spring Kafka groups | Compat matrix in CI per release |
| Performance (targets) | §12 targets on laptop | Commit P99 tracked per release | Real-S3 report published |
| DX | Quickstart ≤ 2 commands, ≤ 10 s | Helm install documented | Testcontainers module on Maven Central |
| Community (goals) | Launch posts live | 3 external contributors | 1 external user; 100+ stars in month 1 (stretch 500) |

Definition of success for Sunil. A public, working, honestly benchmarked and verified system that a senior engineer can read in an afternoon, plus three or four write-ups that show how it was designed and tested. Stars are a bonus; the artefacts are the point.

## Appendix: Sources

Accessed 27 Oct 2026. Star counts are from the GitHub API on that date and change over time.

1. WarpStream: executive summary (cost of 3-AZ Kafka, $0.053/GiB). <https://www.warpstream.com/executive-summary>
2. Confluent acquires WarpStream (press release). <https://www.confluent.io/press-release/confluent-acquires-warpstream-to-advance-next-gen-byoc-data-streaming/>
3. WarpStream blog: "WarpStream is dead, long live WarpStream". <https://www.warpstream.com/blog/warpstream-is-dead-long-live-warpstream>
4. AutoMQ docs: licensing and enterprise features. <https://docs.automq.com/automq/what-is-automq/licensing-and-enterprise-features>
5. AutoMQ: relicense commit to Apache-2.0. <https://github.com/AutoMQ/automq/commit/4596c26983889d2ff271c4dd5acf552987f90282>
6. KIP-1150: Diskless Topics. <https://cwiki.apache.org/confluence/spaces/KAFKA/pages/345377898/KIP-1150+Diskless+Topics>
7. Aiven: KIP-1150 accepted and the road ahead. <https://aiven.io/blog/kip-1150-accepted-and-the-road-ahead>
8. Aiven Inkless repository. <https://github.com/aiven/inkless>
9. Aiven: Understanding Kafka performance, diskless topics deep dive. <https://aiven.io/blog/understanding-apache-kafka-performance-diskless-topics-deep-dive>
10. Aiven: Benchmarking diskless (Inkless) topics, part 1. <https://aiven.io/blog/benchmarking-diskless-inkless-topics-part-1>
11. Bufstream documentation. <https://buf.build/docs/bufstream/>
12. Jepsen: Bufstream 0.1.0 analysis. <https://jepsen.io/analyses/bufstream-0.1.0>
13. Buf: CoreWeave acquires Bufstream. <https://buf.build/blog/coreweave-acquires-bufstream>
14. Redpanda: Cloud Topics (write to object storage). <https://www.redpanda.com/data-streaming/cloud-topics-write-to-object-storage>
15. Redpanda licensing overview. <https://docs.redpanda.com/current/get-started/licensing/overview/>
16. Nisshi repository. <https://github.com/nisshi-io/nisshi>
17. AWS EC2 on-demand pricing: data transfer. <https://aws.amazon.com/ec2/pricing/on-demand/>
18. Amazon S3 pricing. <https://aws.amazon.com/s3/pricing/>
19. AWS: Amazon S3 conditional writes (Aug 2024). <https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/>
20. KIP-679: Producer will enable the strongest delivery guarantee by default. <https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default>
21. Jepsen jepsen.tests.kafka workload. <https://jepsen-io.github.io/jepsen/jepsen.tests.kafka.html>
22. InfoQ: MinIO and S3 API alternatives (Dec 2025). <https://www.infoq.com/news/2025/12/minio-s3-api-alternatives/>
23. KIP-405: Kafka Tiered Storage. <https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage>
24. Apache Kafka documentation: RecordBatch format. <https://kafka.apache.org/documentation/#recordbatch>
