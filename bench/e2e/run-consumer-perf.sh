#!/usr/bin/env bash
set -euo pipefail
BOOTSTRAP="${BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${TOPIC:-bench}"
MESSAGES="${MESSAGES:-20000}"
OUT="${OUT:-bench/e2e/laptop-consumer-perf.csv}"
IMAGE="${KAFKA_IMAGE:-apache/kafka:4.4.0}"

mkdir -p "$(dirname "$OUT")"
cat > "$OUT" <<EOF
# label=laptop
# tool=kafka-consumer-perf-test.sh
# image=${IMAGE}
# bootstrap=${BOOTSTRAP}
# topic=${TOPIC}
# messages=${MESSAGES}
# hardware=$(uname -s) $(uname -m)
# timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
mb_sec,records_per_sec
EOF
docker run --rm --network host "$IMAGE" \
  /opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --messages "$MESSAGES" \
  --timeout 60000 \
  | tee -a "$OUT"
