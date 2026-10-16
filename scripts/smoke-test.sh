#!/usr/bin/env bash
# Smoke test against a running instance: health, then a round trip through Kafka.
#   scripts/smoke-test.sh http://localhost:8080
set -euo pipefail
BASE="${1:-http://localhost:8080}"
STREAM="smoke-$(date +%s)"

gql() {
  curl -sS -f -X POST "$BASE/graphql" -H 'Content-Type: application/json' -d "$1"
}

echo "health..."
curl -sS -f "$BASE/actuator/health" | grep -q '"UP"'

echo "create stream $STREAM..."
gql "{\"query\":\"mutation { createStream(input: { name: \\\"$STREAM\\\", partitions: 1 }) { topic } }\"}" | grep -q "events.$STREAM"

echo "publish event..."
gql "{\"query\":\"mutation { publishEvent(input: { stream: \\\"$STREAM\\\", type: \\\"Ping\\\", payload: { ok: true } }) { eventId } }\"}" | grep -q eventId

echo "wait for consumer..."
for _ in $(seq 1 30); do
  if gql "{\"query\":\"{ streamStats(stream: \\\"$STREAM\\\") { totalEvents } }\"}" | grep -q '"totalEvents":1'; then
    echo "OK: event round-tripped through Kafka into MongoDB"
    exit 0
  fi
  sleep 2
done
echo "FAIL: event never arrived" >&2
exit 1
