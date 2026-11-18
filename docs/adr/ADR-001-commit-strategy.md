# ADR-001: Commit strategy

Status: Accepted

## Context

`sl_commit_object` assigns offsets for every partition in one object. The Day-3 lab compared three ways to do that, on Testcontainers `postgres:18.7-alpine` (server 18.7):

- `row-loop`: `sl_commit_object_rowloop`, one `SELECT … FOR UPDATE` and `UPDATE` per accepted entry
- `set-based`: `sl_commit_object`, one `UPDATE … RETURNING` per object
- `group-commit`: several ready `sl_commit_object` calls in one transaction, after a wait of at most 5 ms

The run is `bench/coordinator/results-2026-11-18.csv` and `bench/coordinator/commit-latency-2026-11-18.svg`. Machine: Windows 11 amd64, 12 processors, Java 26.0.2.1. `PostgreSQLContainer` sets `fsync=off`. `synchronous_commit` stayed at the server default. This lab does not measure the fsync amortization group commit is for.

Each commit updated partitions `0..N-1` of one topic, so the committers queued on the same rows. The timer covers the commit call only. A 1 s heartbeat kept the 10 s lease alive. Samples per thread: 12 at 1, 16 and 128 partitions; 4 at 512; 3 at 1024. At 512 and 1024, P99 is close to the max of a small sample.

P50 / P99 milliseconds:

| Partitions | Committers | row-loop | set-based | group-commit |
| --- | --- | --- | --- | --- |
| 1 | 1 | 9.5 / 10.6 | 11.4 / 12.0 | 28.2 / 42.1 |
| 128 | 1 | 25.4 / 32.2 | 23.3 / 36.2 | 27.7 / 33.3 |
| 1024 | 1 | 119.8 / 130.7 | 66.7 / 68.6 | 56.3 / 56.6 |
| 1 | 12 | 51.4 / 258.8 | 73.2 / 444.5 | 77.1 / 101.1 |
| 128 | 12 | 112.5 / 1459.4 | 54.4 / 921.1 | 207.8 / 289.8 |
| 1024 | 12 | 736.3 / 3998.8 | 230.5 / 2093.2 | 1067.4 / 1165.0 |

The PRD budget is P99 under 100 ms at 128 partitions per commit. With one committer, set-based P99 was 36.2 ms.

## Decision

Production commits call set-based `sl_commit_object`. `LAKEWIRE_GROUP_COMMIT` defaults to false.

Set-based is the function the broker will call. At one committer it matches the row loop up to 128 partitions and is clearly faster at 512 and 1024 (P50 35.8 ms and 66.7 ms, against 52.8 ms and 119.8 ms). Group commit's 5 ms wait makes a single small commit slower (28 ms against 11 ms at one partition). With 12 committers and 1024 partitions, putting every object in one transaction raised the median from 230 ms to 1067 ms, because each caller waited for the whole batch. The tail was tighter. That is not a reason to turn the wait on for every broker.

## Consequences

- `sl_commit_object_rowloop` stays in the database for this lab. Production code does not call it.
- `GroupCommitter` stays. A later lab can turn the flag on if fsync is left on and committers are not all locking the same partitions.
- Multi-committer P99 in this CSV is mostly lock queueing on a shared partition set. It is not the cost of one object on an idle coordinator.
