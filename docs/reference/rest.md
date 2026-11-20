# Admin REST

Base `http://localhost:8080/api/v1`. Writes go through `sl_*`. Admin **validates** Flyway only.

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/cluster` | cluster id + brokers |
| GET | `/topics` | |
| POST | `/topics` | body `{ "name", "partitions", "config": { "retention.ms" } }` |
| DELETE | `/topics/{name}` | marks deleted (A2) |
| GET | `/topics/{name}/partitions/{p}/segments` | |
| GET | `/objects/{id}` | |
| GET | `/cost?gib_per_day=&zones=3&retention_days=7` | §12.1 |
| POST | `/verify/audit` | I7/I8 inspect once |
| GET | `/actuator/prometheus` | |

`/groups` is v0.2.
