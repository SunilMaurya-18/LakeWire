# LakeWire guarantees

v0.1 publishes these invariants. Each row names the checker, the nemeses that try to break it, and the latest report.

Latest report: [docs/reports/correctness-v0.1.0.md](docs/reports/correctness-v0.1.0.md).

Every lease, TTL, grace and expiry uses the **Postgres clock** (`now()`), never the broker clock.

| ID | Promise | Checker | Nemeses | Status |
| --- | --- | --- | --- | --- |
| I1 | Every acknowledged record is eventually readable at its acked offset, with identical bytes. | `lakewire.verify.checker.I1AckedReadableChecker` | kill, pause, partition-pg, pg-restart | v0.1 |
| I2 | Offsets per partition are unique and contiguous from log start to HW. | `lakewire.verify.checker.I2ContiguityChecker` | retention-storm, pg-restart | v0.1 |
| I3 | Per-producer, per-partition order is preserved. | `lakewire.verify.checker.I3SequenceOrderChecker` | kill, partition-pg | v0.1 |
| I4 | No consumer observes an offset ≥ committed HW, or unacked data that later disappears. | `lakewire.verify.checker.I4PhantomChecker` | pause, partition-pg, slow-s3 | v0.1 |
| I5 | An idempotent producer's retries never create duplicates. | `lakewire.verify.checker.I5NoDuplicateChecker` | kill, partition-pg | v0.1 |
| I6 | Committed group offsets per group/partition are monotonic per generation. | — | — | **v0.2** (consumer groups) |
| I7 | No object referenced by a segment is deleted. | `lakewire.verify.checker.I7NoDanglingRefChecker` | pause, retention-storm | v0.1 |
| I8 | No leaked objects older than orphan TTL + GC period. | `lakewire.verify.checker.I8NoLeakChecker` | kill, slow-s3 | v0.1 |

Run the suite yourself: `lakewire verify --nemesis kill,pause,partition-pg --duration 3m --seed 42`.
