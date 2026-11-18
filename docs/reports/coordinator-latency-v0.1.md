# Coordinator commit latency, v0.1

Source: `bench/coordinator/results-2026-11-18.csv`, measured by `CoordinatorLatencyLab` against `postgres:18.7-alpine` (server 18.7, `fsync=off` inside Testcontainers). The client-visible time starts at the commit call. The upload-intent insert is not included. Every commit updates partitions `0..N-1`, so the committers contend on the same rows. Group commit waits up to 5 ms.

The lab machine was Windows 11 amd64, 12 processors, Java 26.0.2.1. `fsync=off` means these numbers are not an fsync-amortization result.

P50 milliseconds:

| Partitions per commit | Committers | Row loop | Set-based | Group commit |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1 | 9.5 | 11.4 | 28.2 |
| 1 | 12 | 51.4 | 73.2 | 77.1 |
| 16 | 1 | 10.7 | 16.2 | 18.8 |
| 128 | 1 | 25.4 | 23.3 | 27.7 |
| 128 | 12 | 112.5 | 54.4 | 207.8 |
| 1024 | 1 | 119.8 | 66.7 | 56.3 |
| 1024 | 12 | 736.3 | 230.5 | 1067.4 |

P99 at 1024 partitions and 12 committers is 3999 ms for the row loop, 2093 ms for set-based, and 1165 ms for group commit. Set-based is the production function (`sl_commit_object`). Group commit stays off by default (ADR-001): it helps the many-committers tail and adds its window to the single-committer case.

The chart is `bench/coordinator/commit-latency-2026-11-18.svg`.
