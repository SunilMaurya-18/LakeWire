# Good first issues (create these on GitHub)

Bodies are ready to paste. Labels: `good first issue`.

---

## 1. Add a Kafka error-code mapping test

**Title:** Add a missing row test to `ErrorMappingTest`

**Body:**
`lakewire.protocol.ErrorMapping` maps coordinator/storage failures to Kafka codes (PRD §10.1). Each row needs a unit test. Pick an untested branch (or add `KAFKA_STORAGE_ERROR` for exhausted S3 retries, A13) and assert the short code the Java client sees. Do not leak stack traces. See `ErrorMappingTest`.

---

## 2. Richer `topics describe`

**Title:** Print HW and `retention.ms` in `lakewire topics describe`

**Body:**
`TopicsCommand.Describe` prints partition count and leader id. Extend it to print high watermark / log start (ListOffsets) and `retention.ms` from topic configs. Keep picocli; add a test next to `LakewireCliTest`. `--bootstrap` already exists.

---

## 3. Grafana panel for cache hit ratio

**Title:** Dashboard panel: cache hit ratio from hits/misses

**Body:**
`deploy/grafana/lakewire-dashboard.json` already has a hit-ratio query in places. Add a dedicated panel titled "Block cache hit ratio" using
`sum(rate(lakewire_cache_hits_total[5m])) / (sum(rate(lakewire_cache_hits_total[5m])) + sum(rate(lakewire_cache_misses_total[5m])))`.
Screenshot after compose up.

---

## 4. GCS/Azure ObjectStore adapter

**Title:** `ObjectStore` implementation for GCS or Azure Blob

**Body:**
SPI is `lakewire.storage.ObjectStore`. v0.1 ships `S3ObjectStore` and `LocalFsObjectStore`. Add one of GCS or Azure with conditional-put or documented fallback (A29/A42). Pin versions in the root `pom.xml`. Tests: Testcontainers or a recorded fake; never real cloud credentials in CI.

---

## 5. Doc translations

**Title:** Translate `docs/tutorials/quickstart.md` (language: …)

**Body:**
Add `docs/tutorials/quickstart.<lang>.md` and a link from `docs/index.md`. Keep commands identical. Do not rewrite `README.md`.

---

## 6. librdkafka compat test

**Title:** Failsafe IT: librdkafka / kcat produce+fetch beyond Metadata

**Body:**
`KcatMetadataIT` covers Metadata. Add an IT that produces and consumes with `edenhill/kcat:1.7.1` against `LakeWireCluster` or in-process broker. Timeouts on every client. Document the result in `docs/reference/compatibility.md`.

---

## 7. `lakewire inspect` pretty-printer

**Title:** Pretty-print LKW1 index in `inspect object`

**Body:**
`InspectCommand` dumps header + index. Print a table: batch base offset, max timestamp, byte_start, length. Keep decoding on `Lkw1Reader`; no Postgres. Extend `InspectCommandTest`.

---

## 8. Cost config for GCP

**Title:** `prices/gcp-us-central1.properties` for `lakewire cost`

**Body:**
Copy the AWS properties shape in `lakewire-cost/src/main/resources/prices/`. Use list prices with source URLs and a check date in comments. `--prices` already overrides. Add a test that the file loads; do not change the §12.1 AWS golden numbers.

---

## 9. Seeded record-batch generator

**Title:** Seeded JUnit generator for RecordBatch v2 bytes

**Body:**
jqwik is out of scope (A1). Add a `SplittableRandom`-seeded helper that builds legal RecordBatch v2 payloads for codec tests. Print the seed on failure. Put it in `lakewire-protocol` tests. Do not use `sun.*`.

---

## 10. Helm values doc

**Title:** Document a Helm `values.yaml` sketch (chart itself is later)

**Body:**
There is no Helm chart in v0.1 (`deploy/compose` only). Write `docs/how-to/helm-values.md` listing the `LAKEWIRE_*` keys from `docs/reference/config.md` as values you would expect in a future chart. Link it from `docs/index.md`. Do not add a chart.
