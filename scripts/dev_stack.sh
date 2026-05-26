#!/usr/bin/env bash
# Bring up the full local dev stack: Kafka + ClickHouse + Streamlit
# dashboard + Kafka consumer + RSS producer. Tails all three component
# logs into one terminal with per-component prefixes.
#
# Layout:
#   * docker compose up -d           broker + DWH (persistent volumes)
#   * streamlit (background)         dashboard at http://localhost:8501
#   * consumer  (background)         drains topic -> ClickHouse in batches
#   * producer  (background)         publishes RSS + Google News
#   * tail -F   (foreground)         live merged log view
#
# Designed to run indefinitely (overnight). ClickHouse data lives in the
# `infra_clickhouse_data` named volume, Kafka in `infra_kafka_data`.
# Both survive Ctrl+C and `docker compose down`. They only go away on
# `docker volume rm`.
#
# Shutdown is ordered for clean drain:
#   1. SIGTERM the producer  (it stops mid-sweep after its in-flight flush)
#   2. wait $DRAIN_GRACE_S    (consumer keeps eating the Kafka backlog)
#   3. SIGTERM the consumer  (it flushes its final batch + commits offsets)
#   4. SIGTERM streamlit + tails
#
# Use scripts/dev_stack.sh --down to also stop the docker stack
# (volumes are preserved).
#
# Run from the repo root: ./scripts/dev_stack.sh

set -u

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--down" ]]; then
  docker compose -f infra/docker-compose.yml down
  exit 0
fi

LOG_DIR=${LOG_DIR:-/tmp}
CONSUMER_LOG="$LOG_DIR/news-consumer.log"
STREAMLIT_LOG="$LOG_DIR/news-streamlit.log"

# Number of producer processes. Each takes a 1/N shard of the feed
# catalogue (sources.yaml + gnews_queries.yaml) so combined throughput
# scales near-linearly with N until upstream publishers rate-limit.
# Each producer runs its own RSS_BODY_WORKERS pool so total in-flight
# HTTP body requests is N * RSS_BODY_WORKERS.
PRODUCER_SHARDS=${PRODUCER_SHARDS:-4}

# Seconds to wait after killing producer before killing consumer,
# so the consumer drains whatever the producer left on the topic.
DRAIN_GRACE_S=${DRAIN_GRACE_S:-25}

# Truncate log files on each fresh run so the live tail starts clean.
: > "$CONSUMER_LOG"
: > "$STREAMLIT_LOG"
producer_logs=()
for ((i = 0; i < PRODUCER_SHARDS; i++)); do
  producer_logs+=("$LOG_DIR/news-producer-$i.log")
  : > "${producer_logs[$i]}"
done

producer_pids=()
consumer_pid=""
streamlit_pid=""
tail_pids=()
cleaned_up=0

cleanup() {
  if [[ "$cleaned_up" -eq 1 ]]; then
    return
  fi
  cleaned_up=1
  echo
  echo "[dev-stack] ordered shutdown..."

  if [[ ${#producer_pids[@]} -gt 0 ]]; then
    echo "[dev-stack]   1/3 stopping ${#producer_pids[@]} producer(s): ${producer_pids[*]}"
    for pid in "${producer_pids[@]}"; do
      kill -TERM "$pid" 2>/dev/null || true
    done
    for pid in "${producer_pids[@]}"; do
      wait "$pid" 2>/dev/null || true
    done
  fi

  if [[ -n "$consumer_pid" ]] && kill -0 "$consumer_pid" 2>/dev/null; then
    echo "[dev-stack]   2/3 draining consumer for up to ${DRAIN_GRACE_S}s..."
    kill -TERM "$consumer_pid" 2>/dev/null || true
    drained=0
    while [[ $drained -lt $DRAIN_GRACE_S ]] && kill -0 "$consumer_pid" 2>/dev/null; do
      sleep 1
      drained=$((drained + 1))
    done
    if kill -0 "$consumer_pid" 2>/dev/null; then
      echo "[dev-stack]   consumer still running after ${DRAIN_GRACE_S}s, forcing"
      kill -KILL "$consumer_pid" 2>/dev/null || true
    fi
    wait "$consumer_pid" 2>/dev/null || true
  fi

  echo "[dev-stack]   3/3 stopping streamlit + tails"
  if [[ -n "$streamlit_pid" ]]; then
    kill -TERM "$streamlit_pid" 2>/dev/null || true
  fi
  for pid in "${tail_pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true

  echo "[dev-stack] done. data preserved in docker volumes:"
  echo "[dev-stack]   - infra_clickhouse_data (rows in news.articles)"
  echo "[dev-stack]   - infra_kafka_data      (broker offsets + unread msgs)"
  echo "[dev-stack] containers are still up. run './scripts/dev_stack.sh --down' to stop them."
}
trap cleanup EXIT INT TERM

echo "[dev-stack] 1/5 starting docker stack..."
docker compose -f infra/docker-compose.yml up -d

# Wait for a docker service to become healthy. Bails fast if the
# container has exited instead of polling forever (common when kafka
# crashes on a stale ZK ephemeral and the broker never comes up).
wait_for_service() {
  local container="$1" label="$2"
  shift 2
  local elapsed=0
  local timeout=${SERVICE_WAIT_TIMEOUT_S:-120}
  while ! docker exec "$container" "$@" >/dev/null 2>&1; do
    local state
    state=$(docker inspect -f '{{.State.Status}}' "$container" 2>/dev/null || echo "missing")
    if [[ "$state" != "running" ]]; then
      echo "[dev-stack] $label container not running (state=$state)."
      echo "[dev-stack]   inspect: docker logs $container"
      exit 1
    fi
    if [[ $elapsed -ge $timeout ]]; then
      echo "[dev-stack] $label still not responding after ${timeout}s, aborting."
      echo "[dev-stack]   inspect: docker logs $container"
      exit 1
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
}

echo "[dev-stack] 2/5 waiting for kafka..."
wait_for_service infra-kafka-1 kafka \
  kafka-topics --bootstrap-server localhost:9092 --list

echo "[dev-stack] 3/5 waiting for clickhouse..."
wait_for_service infra-clickhouse-1 clickhouse \
  clickhouse-client --user news --password news -q "SELECT 1"

echo "[dev-stack] 4/5 launching streamlit / consumer / producer in background..."

PYTHONPATH=. uv run streamlit run dashboard/streamlit_app.py \
  --server.headless true --server.port 8501 >"$STREAMLIT_LOG" 2>&1 &
streamlit_pid=$!

# Consumer in long-lived mode so it keeps draining as the producer
# publishes. Tight batch flush => dashboard sparkline updates feel live.
CONSUMER_IDLE_TIMEOUT_S=0 \
  CONSUMER_BATCH_FLUSH_S=${CONSUMER_BATCH_FLUSH_S:-0.5} \
  CONSUMER_BATCH_MAX=${CONSUMER_BATCH_MAX:-200} \
  CONSUMER_SHUTDOWN_DRAIN_S=${CONSUMER_SHUTDOWN_DRAIN_S:-20} \
  PYTHONPATH=. uv run python pipelines/kafka/consumer_to_clickhouse.py \
  >"$CONSUMER_LOG" 2>&1 &
consumer_pid=$!

# Producers loop on this interval and pace messages so each sweep
# trickles across the window instead of bursting. PRODUCER_SHARDS
# parallel processes each take a 1/N slice of the feed catalogue.
# Throughput scales near-linearly until upstream rate-limits kick in.
for ((i = 0; i < PRODUCER_SHARDS; i++)); do
  PRODUCER_INTERVAL_S=${PRODUCER_INTERVAL_S:-300} \
    PRODUCER_MSG_DELAY_MS=${PRODUCER_MSG_DELAY_MS:-7} \
    PRODUCER_SHARD="$i/$PRODUCER_SHARDS" \
    RSS_FETCH_BODY=${RSS_FETCH_BODY:-1} \
    PYTHONPATH=. uv run python pipelines/kafka/producer_rss.py \
    >"${producer_logs[$i]}" 2>&1 &
  producer_pids+=($!)
done

# ANSI colours per component so the merged view stays readable.
CYAN="\033[0;36m"; YEL="\033[0;33m"; MAG="\033[0;35m"; RST="\033[0m"

prefix() {
  local label="$1" colour="$2" file="$3"
  tail -F "$file" 2>/dev/null \
    | awk -v p="$(printf '%b[%-9s]%b ' "$colour" "$label" "$RST")" \
          '{ print p $0; fflush(); }' &
  tail_pids+=($!)
}

for ((i = 0; i < PRODUCER_SHARDS; i++)); do
  prefix "producer$i" "$CYAN" "${producer_logs[$i]}"
done
prefix "consumer"  "$YEL"  "$CONSUMER_LOG"
prefix "streamlit" "$MAG"  "$STREAMLIT_LOG"

echo "[dev-stack] 5/5 live. dashboard: http://localhost:8501"
echo "[dev-stack]   logs:   ${producer_logs[*]} / $CONSUMER_LOG / $STREAMLIT_LOG"
echo "[dev-stack]   stop:   Ctrl+C   -> ordered drain, data persists in volumes"
echo "[dev-stack]           --down   -> also stops docker (volumes preserved)"
echo

# Block on the tails. Ctrl+C trips the trap and runs ordered shutdown.
wait "${tail_pids[@]}"
