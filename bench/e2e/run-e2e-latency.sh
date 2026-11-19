#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BOOTSTRAP="${BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${TOPIC:-bench-e2e}"
RECORDS="${RECORDS:-2000}"
OUT="${OUT:-$ROOT/bench/e2e/laptop-e2e-latency.csv}"
cd "$ROOT"
./mvnw -q -pl bench -am exec:java \
  -Dexec.mainClass=lakewire.bench.e2e.E2eLatencyProbe \
  -Dexec.args="--bootstrap ${BOOTSTRAP} --topic ${TOPIC} --records ${RECORDS} --output ${OUT}"
