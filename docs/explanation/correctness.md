# Correctness model

Correctness is the product (PRD §11). Invariants I1–I5, I7, I8 are checked after a seeded nemesis schedule. I6 is v0.2.

Nemeses (`lakewire.verify.nemesis.NemesisKind`): `kill`, `pause`, `partition-pg`, `slow-s3`, `pg-restart`, `retention-storm`.

Clock skew is not a nemesis: leases use Postgres `now()`. Ambiguous commits: the object key is in the transaction; on retry the committer sees the existing row (A35).

Report: [correctness-v0.1.0.md](../reports/correctness-v0.1.0.md). Promises: [GUARANTEES.md](../../GUARANTEES.md).
