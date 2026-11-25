#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PG_DSN="${FERRY_PG_DSN:-postgres://ferry:ferry@localhost:5432/ferry?sslmode=disable}"
KAFKA_BROKERS="${FERRY_KAFKA_BROKERS:-localhost:9092}"
REDIS_ADDR="${FERRY_REDIS_ADDR:-localhost:6379}"
SERVER_URL="${FERRY_SERVER_URL:-http://127.0.0.1:8080}"
RECEIVER_URL="${FERRY_RECEIVER_URL:-http://127.0.0.1:8081}"
WORKER_URL="${FERRY_WORKER_URL:-http://127.0.0.1:8082}"
RUN_ID="$(date +%s)-$$"
KAFKA_READY_TOPIC="${FERRY_KAFKA_READY_TOPIC:-ferry.verify.m1.ready.${RUN_ID}}"
KAFKA_GROUP_ID="${FERRY_KAFKA_GROUP_ID:-ferry-verify-m1-${RUN_ID}}"
SKIP_TESTS="${SKIP_TESTS:-0}"

PIDS=()
LOG_DIR="$(mktemp -d)"

cleanup() {
  local code=$?
  for pid in "${PIDS[@]:-}"; do
    if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
      kill -TERM "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  if [ "$code" -ne 0 ]; then
    echo "verify_m1 failed; recent logs in $LOG_DIR:" >&2
    for f in "$LOG_DIR"/*.log; do
      [ -f "$f" ] || continue
      echo "=== $f ===" >&2
      tail -n 40 "$f" >&2 || true
    done
  else
    rm -rf "$LOG_DIR"
  fi
  exit "$code"
}
trap cleanup EXIT

wait_http() {
  local url=$1
  local name=$2
  for _ in $(seq 1 40); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "timed out waiting for $name at $url" >&2
  return 1
}

echo "[1/6] Starting Docker Compose dependencies..."
docker compose -f deploy/docker-compose.yml up -d --wait

if [ "$SKIP_TESTS" = "1" ]; then
  echo "[2/6] Skipping go test (SKIP_TESTS=1) and building binaries..."
else
  echo "[2/6] Running race unit tests and building binaries..."
  go test -race -count=1 ./...
fi
make build

echo "[3/6] Applying Goose database migrations..."
FERRY_PG_DSN="$PG_DSN" ./.build/ferry-server -migrate-only

echo "[4/6] Launching chaos-receiver (:8081), ferry-server (:8080), and ferry-worker (:8082) [topic=$KAFKA_READY_TOPIC, group=$KAFKA_GROUP_ID]..."
FERRY_RECEIVER_ADDR=":8081" \
  ./.build/chaos-receiver >"$LOG_DIR/chaos-receiver.log" 2>&1 &
PIDS+=($!)

FERRY_HTTP_ADDR=":8080" \
FERRY_METRICS_ADDR=":2112" \
FERRY_PG_DSN="$PG_DSN" \
FERRY_KAFKA_BROKERS="$KAFKA_BROKERS" \
FERRY_KAFKA_READY_TOPIC="$KAFKA_READY_TOPIC" \
FERRY_REDIS_ADDR="$REDIS_ADDR" \
  ./.build/ferry-server >"$LOG_DIR/ferry-server.log" 2>&1 &
PIDS+=($!)

wait_http "$RECEIVER_URL/healthz" "chaos-receiver"
wait_http "$SERVER_URL/readyz" "ferry-server"

FERRY_WORKER_PROBE_ADDR=":8082" \
FERRY_WORKER_METRICS_ADDR=":2113" \
FERRY_PG_DSN="$PG_DSN" \
FERRY_KAFKA_BROKERS="$KAFKA_BROKERS" \
FERRY_KAFKA_READY_TOPIC="$KAFKA_READY_TOPIC" \
FERRY_KAFKA_GROUP_ID="$KAFKA_GROUP_ID" \
FERRY_REDIS_ADDR="$REDIS_ADDR" \
  ./.build/ferry-worker >"$LOG_DIR/ferry-worker.log" 2>&1 &
WORKER_PID=$!
PIDS+=("$WORKER_PID")

wait_http "$WORKER_URL/readyz" "ferry-worker"

echo "[5/6] Running Milestone 1 end-to-end checks..."
SECRET="whsec_m1_verify"
EP_JSON="$(curl -fsS -X POST "$SERVER_URL/v1/endpoints" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"m1-ok\",\"target_url\":\"$RECEIVER_URL/webhook?secret=$SECRET\",\"signing_secret\":\"$SECRET\"}")"
EP_ID="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$EP_JSON")"
echo "Created Endpoint: $EP_ID"

RAW_PAYLOAD='{"z": 1,   "a": "ferry-m1"}'
IDEM_KEY="m1-idem-${RUN_ID}"
TMP_CODES="$LOG_DIR/codes.txt"
TMP_IDS="$LOG_DIR/ids.txt"
: >"$TMP_CODES"
: >"$TMP_IDS"

REQ_PIDS=()
for _ in $(seq 1 10); do
  (
    resp="$(curl -sS -w '\n%{http_code}' -X POST "$SERVER_URL/v1/tasks" \
      -H "Content-Type: application/json" \
      -H "Idempotency-Key: $IDEM_KEY" \
      -d "{\"endpoint_id\":\"$EP_ID\",\"payload\":$RAW_PAYLOAD}")"
    code="$(tail -n1 <<<"$resp")"
    body="$(sed '$d' <<<"$resp")"
    printf '%s\n' "$code" >>"$TMP_CODES"
    python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$body" >>"$TMP_IDS"
  ) &
  REQ_PIDS+=($!)
done
for pid in "${REQ_PIDS[@]}"; do
  wait "$pid"
done

UNIQUE_IDS="$(sort -u "$TMP_IDS" | wc -l | tr -d ' ')"
if [ "$UNIQUE_IDS" != "1" ]; then
  echo "expected exactly 1 unique task id across 10 concurrent submissions, got $UNIQUE_IDS" >&2
  exit 1
fi
TASK_ID="$(head -n1 "$TMP_IDS")"
echo "Idempotent Task ID: $TASK_ID (codes: $(tr '\n' ' ' <"$TMP_CODES"))"

CONFLICT_CODE="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$SERVER_URL/v1/tasks" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $IDEM_KEY" \
  -d "{\"endpoint_id\":\"$EP_ID\",\"payload\":{\"different\":true}}")"
if [ "$CONFLICT_CODE" != "409" ]; then
  echo "expected 409 Conflict for mismatched idempotency payload, got $CONFLICT_CODE" >&2
  exit 1
fi

for _ in $(seq 1 40); do
  STATUS="$(curl -fsS "$SERVER_URL/v1/tasks/$TASK_ID" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  if [ "$STATUS" = "SUCCEEDED" ]; then
    break
  fi
  sleep 0.25
done
if [ "${STATUS:-}" != "SUCCEEDED" ]; then
  echo "expected task $TASK_ID to reach SUCCEEDED, got ${STATUS:-unknown}" >&2
  exit 1
fi

STAT_JSON="$(curl -fsS "$RECEIVER_URL/stats/$TASK_ID")"
python3 - "$STAT_JSON" <<'PY'
import json, sys
stat = json.loads(sys.argv[1])
assert stat["count"] == 1, f"expected count=1, got {stat}"
assert stat["signature_valid"] is True, f"expected signature_valid=True, got {stat}"
PY

BAD_EP_ID="$(curl -fsS -X POST "$SERVER_URL/v1/endpoints" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"m1-400\",\"target_url\":\"$RECEIVER_URL/webhook/400\"}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"

BAD_TASK_ID="$(curl -fsS -X POST "$SERVER_URL/v1/tasks" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: m1-400-${RUN_ID}" \
  -d "{\"endpoint_id\":\"$BAD_EP_ID\",\"payload\":{\"kind\":\"bad\"}}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"

for _ in $(seq 1 40); do
  BAD_JSON="$(curl -fsS "$SERVER_URL/v1/tasks/$BAD_TASK_ID")"
  BAD_STATUS="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"$BAD_JSON")"
  if [ "$BAD_STATUS" = "DEAD_LETTER" ]; then
    break
  fi
  sleep 0.25
done
python3 - "$BAD_JSON" <<'PY'
import json, sys
task = json.loads(sys.argv[1])
assert task["status"] == "DEAD_LETTER", task
assert task["attempt"] == 1, task
assert task["last_error"] == "NON_RETRYABLE_4XX", task
PY

echo "[6/6] Verifying in-flight cancel lock conflict (409) and SIGTERM graceful drain..."
SLOW_EP_ID="$(curl -fsS -X POST "$SERVER_URL/v1/endpoints" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"m1-slow\",\"target_url\":\"$RECEIVER_URL/webhook/200?delay_ms=2500\"}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"

SLOW_TASK_ID="$(curl -fsS -X POST "$SERVER_URL/v1/tasks" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: m1-slow-${RUN_ID}" \
  -d "{\"endpoint_id\":\"$SLOW_EP_ID\",\"payload\":{\"slow\":true}}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"

sleep 0.6
CANCEL_CODE="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$SERVER_URL/v1/tasks/$SLOW_TASK_ID/cancel")"
if [ "$CANCEL_CODE" != "409" ]; then
  echo "expected 409 Conflict cancelling in-flight task, got $CANCEL_CODE" >&2
  exit 1
fi

kill -TERM "$WORKER_PID"
wait "$WORKER_PID"

SLOW_STATUS="$(curl -fsS "$SERVER_URL/v1/tasks/$SLOW_TASK_ID" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
if [ "$SLOW_STATUS" != "SUCCEEDED" ]; then
  echo "expected in-flight task $SLOW_TASK_ID to finish SUCCEEDED before SIGTERM exit, got $SLOW_STATUS" >&2
  exit 1
fi

echo "Milestone 1 verification passed!"
