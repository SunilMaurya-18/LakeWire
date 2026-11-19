#!/usr/bin/env bash
# Laptop/compose wrapper for kafka-producer-perf-test.sh (apache/kafka:4.4.0).
set -euo pipefail
BOOTSTRAP="${BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${TOPIC:-bench}"
RECORDS="${RECORDS:-20000}"
RECORD_SIZE="${RECORD_SIZE:-1024}"
THROUGHPUT="${THROUGHPUT:--1}"
OUT="${OUT:-bench/e2e/laptop-producer-perf.csv}"
IMAGE="${KAFKA_IMAGE:-apache/kafka:4.4.0}"

header() {
  cat <<EOF
# label=laptop
# tool=kafka-producer-perf-test.sh
# image=${IMAGE}
# bootstrap=${BOOTSTRAP}
# topic=${TOPIC}
# records=${RECORDS}
# record_size=${RECORD_SIZE}
# throughput=${THROUGHPUT}
# acks=all
# hardware=$(uname -s) $(uname -m)
# java=container
# timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
records_sent,records_per_sec,avg_latency_ms,max_latency_ms
EOF
}

mkdir -p "$(dirname "$OUT")"
header > "$OUT"
docker run --rm --network host "$IMAGE" \
  /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic "$TOPIC" \
  --num-records "$RECORDS" \
  --record-size "$RECORD_SIZE" \
  --throughput "$THROUGHPUT" \
  --producer-props bootstrap.servers="$BOOTSTRAP" acks=all linger.ms=200 \
  | tee -a "$OUT"
