# Contributing

## Dev setup (2 commands)

```text
git clone https://github.com/SunilMaurya-18/lakewire.git
cd lakewire && copy deploy\compose\.env.example deploy\compose\.env && docker compose -f deploy/compose/compose.yaml --env-file deploy/compose/.env up -d
```

Needs Docker Desktop, Java 21, Git. Unix: `cp` instead of `copy`.

No Docker: `.\mvnw.cmd -pl lakewire-cli -am package -DskipTests` then `bin\lakewire.cmd dev`.

## Module map

| Module | Role |
| --- | --- |
| `lakewire-protocol` | Kafka codec, `SupportedApi` |
| `lakewire-storage` | ObjectStore, LKW1 |
| `lakewire-coordinator` | Flyway, `sl_*` |
| `lakewire-workers` | GC, retention, audit |
| `lakewire-broker` | Netty, produce/fetch |
| `lakewire-cost` | §12.1 model |
| `lakewire-admin` | Spring Boot REST |
| `lakewire-cli` | picocli |
| `lakewire-testkit` | Testcontainers cluster |
| `verify` | nemeses + checkers |
| `bench` | CSVs |

Maven wrapper only. Spring Boot only in `lakewire-admin`.

## Run verify before PR

```text
.\mvnw.cmd -B verify
```

Docker required for `*IT.java`. Do not skip failsafe.

## DCO

Every commit must include:

```text
Signed-off-by: Your Name <you@example.com>
```

(`git commit -s`.) You certify the [Developer Certificate of Origin](https://developercertificate.org/).

## Commit style

[Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`, `test:`, `build:`. One milestone, one commit message from the phase prompts when following that process.

## Add a nemesis

1. Add a `NemesisKind` flag and implement it in `ClusterFaults`.
2. Map which invariants it targets in `GUARANTEES.md`.
3. Cover it in `VerifySmokeIT` or a focused `*IT`.
4. Document the seed on failure.

## Add a checker

1. New class next to `I1AckedReadableChecker`.
2. Call it from `LakeWireVerify` after the workload.
3. Add a unit test in `CheckerTest`.
4. Link it from `GUARANTEES.md`.
