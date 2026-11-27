# Cost model

`lakewire cost` implements PRD §12.1. Prices: `lakewire-cost/src/main/resources/prices/aws-us-east-1.properties` (us-east-1, dated in comments). Override with `--prices`; `prices/gcp-us-central1.properties` holds GCP us-central1 list prices (GCS Standard, pd-balanced, inter-zone $0.01/GiB billed once).

Per GiB written, 3 zones, 8 MiB objects (reproduced in `CostModelTest`):

| Path | Transfer ≈ |
| --- | --- |
| Classic Kafka RF=3 | $0.0533 / GiB |
| LakeWire | $0.0008 / GiB |

Classic: producer→leader cross-AZ `(Z-1)/Z × $0.02` plus replication `2 × $0.02`. LakeWire: zone-local produce ≈ $0; PUTs = GiB/object × PUT price; GETs = zones × objects × GET price. Storage (EBS vs S3) is printed separately (A51).

The CLI always prints the §12.1 caveats (list prices, no request tax, laptop ≠ cloud).
