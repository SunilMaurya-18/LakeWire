# Correctness report v0.1.0

PRD §11 wording: a release lists checker version, nemeses, number of runs, seeds, and violations found and fixed. A bug found by the harness counts as a feature of the process.

## Checker version

Classes in `verify/src/main/java/lakewire/verify/checker/` at this commit:

- I1 `I1AckedReadableChecker`
- I2 `I2ContiguityChecker`
- I3 `I3SequenceOrderChecker`
- I4 `I4PhantomChecker`
- I5 `I5NoDuplicateChecker`
- I6 not implemented (v0.2)
- I7 `I7NoDanglingRefChecker`
- I8 `I8NoLeakChecker`

Harness: `lakewire.verify.LakeWireVerify`.

## Nemeses

`kill`, `pause`, `partition-pg`, `slow-s3`, `pg-restart`, `retention-storm` (`NemesisKind`).

## Runs recorded locally (M9–M12)

| Run | Duration | Seed | Nemeses | Result |
| --- | --- | --- | --- | --- |
| PR smoke (`VerifySmokeIT`) | 3 m | 42 | kill, pause, partition-pg | PASS (M9; re-run after CLI image fix) |
| `ThreeBrokerIT` | one shot | n/a | zone metadata | PASS |
| `BrokerKillIT` | one shot | n/a | kill | PASS |
| `ZombieBrokerIT` | one shot | n/a | pause | PASS |
| `IdempotentProducerIT` | one shot | n/a | I5 | PASS |
| `RetentionStormIT` | one shot | n/a | retention-storm | PASS |
| Nightly 30 m | CI `nemesis.yml` | `github.run_id` | all six | not in this laptop report |
| 50 × 10 m soak | A48 | — | — | **not run locally** (hours). Launch gate §12 still requires ≥100 runs. |

This file does **not** claim 100 overnight runs. Seeds for those belong here when they exist.

## Violations found and fixed

See [bugs-found.md](bugs-found.md):

1. Fetch hid committed records behind a stale HW cache (I1 under partition-pg).
2. I2 treated log start as offset 0 after retention.

No open invariant violations in the runs above.
