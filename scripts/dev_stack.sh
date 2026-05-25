#!/usr/bin/env bash
# Bring up the full local dev stack: Kafka + ClickHouse + Streamlit
# dashboard + Kafka consumer + RSS producer. Tails all three component
# logs into one terminal with per-component prefixes.
#
# Layout:
#   * docker compose up -d           broker + DWH
#   * streamlit (background)         dashboard at http://localhost:8501
#   * consumer  (background)         drains topic -> ClickHouse in batches
#   * producer  (background)         publishes RSS + Google News
#   * tail -F   (foreground)         live merged log view
#
# Ctrl+C tears everything down (streamlit, consumer, producer, tails).
# Use scripts/dev_stack.sh --down to also stop the docker stack.
#
# Run from the repo root: ./scripts/dev_stack.sh

set -u

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--down" ]]; then
  docker compose -f infra/docker-compose.yml down
  exit 0
fi

LOG_DIR=${LOG_DIR:-/tmp}
PRODUCER_LOG="$LOG_DIR/news-producer.log"
CONSUMER_LOG="$LOG_DIR/news-consumer.log"
STREAMLIT_LOG="$LOG_DIR/news-streamlit.log"

# Truncate log files on each fresh run so the live tail starts clean.
: > "$PRODUCER_LOG"
: > "$CONSUMER_LOG"
: > "$STREAMLIT_LOG"

bg_pids=()
cleanup() {
  echo
  echo "[dev-stack] shutting down..."
  for pid in "${bg_pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "[dev-stack] 1/5 starting docker stack..."
docker compose -f infra/docker-compose.yml up -d

echo "[dev-stack] 2/5 waiting for kafka..."
until docker exec infra-kafka-1 \
    kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
done

echo "[dev-stack] 3/5 waiting for clickhouse..."
until docker exec infra-clickhouse-1 \
    clickhouse-client --user news --password news -q "SELECT 1" >/dev/null 2>&1; do
  sleep 2
done

echo "[dev-stack] 4/5 launching streamlit / consumer / producer in background..."

PYTHONPATH=. uv run streamlit run dashboard/streamlit_app.py \
  --server.headless true --server.port 8501 >"$STREAMLIT_LOG" 2>&1 &
bg_pids+=($!)

# Consumer in long-lived mode so it keeps draining as the producer
# publishes. Smaller batch flush => more frequent dashboard updates.
CONSUMER_IDLE_TIMEOUT_S=0 CONSUMER_BATCH_FLUSH_S=2 \
  PYTHONPATH=. uv run python pipelines/kafka/consumer_to_clickhouse.py \
  >"$CONSUMER_LOG" 2>&1 &
bg_pids+=($!)

# Producer loops on this interval by default so the dashboard keeps
# seeing fresh items. Override with PRODUCER_INTERVAL_S=0 for one-shot.
PRODUCER_INTERVAL_S=${PRODUCER_INTERVAL_S:-300} \
  PYTHONPATH=. uv run python pipelines/kafka/producer_rss.py \
  >"$PRODUCER_LOG" 2>&1 &
bg_pids+=($!)

# ANSI colours per component so the merged view stays readable.
CYAN="\033[0;36m"; YEL="\033[0;33m"; MAG="\033[0;35m"; RST="\033[0m"

prefix() {
  local label="$1" colour="$2" file="$3"
  tail -F "$file" 2>/dev/null \
    | awk -v p="$(printf '%b[%-9s]%b ' "$colour" "$label" "$RST")" \
          '{ print p $0; fflush(); }' &
  bg_pids+=($!)
}

prefix "producer"  "$CYAN" "$PRODUCER_LOG"
prefix "consumer"  "$YEL"  "$CONSUMER_LOG"
prefix "streamlit" "$MAG"  "$STREAMLIT_LOG"

echo "[dev-stack] 5/5 live. dashboard: http://localhost:8501"
echo "[dev-stack]   logs:   $PRODUCER_LOG / $CONSUMER_LOG / $STREAMLIT_LOG"
echo "[dev-stack]   stop:   Ctrl+C (or scripts/dev_stack.sh --down to also stop docker)"
echo

# Block on the tails. Ctrl+C trips the trap and kills everything.
wait
