# Compatibility matrix

Pinned by `SupportedApi` / `SupportedApiTest`. Unknown API or version → `UNSUPPORTED_VERSION`.

| API | Versions | Java client 4.4.0 | console tools (`apache/kafka:4.4.0`) | kcat 1.7.1 |
| --- | --- | --- | --- | --- |
| ApiVersions | 0–3 | yes | yes | yes |
| Metadata | 1–9 | yes (zone via `client.id=…,az=`) | yes | yes (`KcatMetadataIT`) |
| Produce | 3–12 | yes, idempotence on | yes | produce |
| Fetch | 4–12 | yes | yes | consume |
| ListOffsets | 1–6 | yes | yes | |
| InitProducerId | 0–4 | yes; transactional id → `INVALID_REQUEST` | | |
| CreateTopics | 2–6 | yes | yes | |
| DeleteTopics | 1–5 | yes | yes | |

Not advertised: consumer groups, txns, topic UUIDs (Metadata ≥ 10, Produce/Fetch ≥ 13).
