# Benchmark report v0.1.0

Laptop-labelled 1-broker SeaweedFS runs (A53), not a 3-broker compose cluster. Raw CSVs:

- `bench/e2e/laptop-producer-perf.csv`
- `bench/e2e/laptop-consumer-perf.csv`
- `bench/e2e/laptop-e2e-latency.csv`
- `bench/coordinator/results-2026-11-18.csv` (write-up: `docs/reports/coordinator-latency-v0.1.md`)

Hardware (CSV headers): Windows 11 amd64, 12 CPUs, kafka-clients 4.4.0, SeaweedFS 4.54, LakeWire `0.1.0-SNAPSHOT`. Timestamp `2026-11-19T19:13:30Z`.

## E2E probe (32 records, linger-dominated)

| Metric | Value | §12 target | Met? |
| --- | --- | --- | --- |
| Produce wall (32 records) | 198 ms | P50 ≤ 300 ms / P99 ≤ 800 ms | **not comparable** — batch wall, not per-record histogram |
| Records/s (producer CSV) | 161.6 | ≥ 50 MiB/s @ 3 brokers | **no** (1 broker, tiny payload, laptop) |
| E2E | same 32-record window | P99 ≤ 1.5 s | window 198 ms for 32 records; not a P99 |

The CSV note says to use `E2eLatencyProbe` against compose for per-record P50/P99. That compose run is not in this report.

## Coordinator commit (Testcontainers Postgres, `fsync=off`)

From `coordinator-latency-v0.1.md`. Production path is set-based `sl_commit_object`.

| Partitions | Committers | Set-based P50 / P99 (ms) | Target P99 < 100 ms @ 128 | Met? |
| --- | --- | ---: | --- | --- |
| 128 | 1 | 23.3 / 36.2 | yes (single committer) | **yes** |
| 128 | 12 | 54.4 / 921.1 | same-row lock convoy | **no** (lab contention, not idle commit) |

## Other §12 targets

| Metric | This report |
| --- | --- |
| Recovery ≤ 15 s | `BrokerKillIT` (cluster); not re-measured here |
| `lakewire dev` ≤ 10 s | warm target; first Zonky unpack was ~13.6–14.7 s (A57) |
