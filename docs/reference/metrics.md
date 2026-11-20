# Metrics

Broker: JDK `HttpServer` at `http://<host>:<LAKEWIRE_METRICS_PORT>/metrics` (Prometheus text).

Admin: `GET /actuator/prometheus`.

| Name | Type | Notes |
| --- | --- | --- |
| `lakewire_commit_latency_seconds` | histogram | coordinator commit |
| `lakewire_produce_latency_seconds` | histogram | produce path |
| `lakewire_fetch_latency_seconds` | histogram | fetch path |
| `lakewire_s3_put_total` | counter | `outcome` tag |
| `lakewire_s3_get_total` | counter | `outcome` tag |
| `lakewire_cache_hits_total` / `lakewire_cache_misses_total` | counter | dashboard hit ratio |
| `lakewire_object_bytes` | gauge | last object |
| `lakewire_buffer_bytes` | gauge | write buffer |
| `lakewire_audit_violations_total` | counter | `invariant` tag (I7/I8) |

Grafana: `deploy/grafana/lakewire-dashboard.json`. Prometheus scrape: `deploy/prometheus/prometheus.yml`.
