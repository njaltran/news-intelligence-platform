#!/usr/bin/env bash
# Bring up the full local dev stack: Kafka + ClickHouse + Streamlit
# dashboard + consumer + producer, all wired together.
#
# Layout:
#   * docker compose up -d         broker + DWH
#   * streamlit (background)       dashboard at http://localhost:8501
#   * consumer  (background)       drains topic -> ClickHouse in batches
#   * producer  (foreground)       publishes RSS + Google News
#
# Ctrl+C tears down the foreground producer and shuts down streamlit +
# consumer. Use scripts/dev_stack.sh --down to also stop the docker
# stack.
#
# Run from the repo root: ./scripts/dev_stack.sh

set -u

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--down" ]]; then
  docker compose -f infra/docker-compose.yml down
  exit 0
fi

bg_pids=()
cleanup() {
  echo
  echo "shutting down background tasks..."
  for pid in "${bg_pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "[1/5] starting docker stack..."
docker compose -f infra/docker-compose.yml up -d

echo "[2/5] waiting for kafka..."
until docker exec infra-kafka-1 \
    kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
done

echo "[3/5] waiting for clickhouse..."
until docker exec infra-clickhouse-1 \
    clickhouse-client --user news --password news -q "SELECT 1" >/dev/null 2>&1; do
  sleep 2
done

echo "[4/5] launching streamlit (http://localhost:8501) and consumer..."
PYTHONPATH=. uv run streamlit run dashboard/streamlit_app.py \
  --server.headless true --server.port 8501 >/tmp/news-streamlit.log 2>&1 &
bg_pids+=($!)

# Consumer in long-lived mode so it keeps draining as the producer publishes.
CONSUMER_IDLE_TIMEOUT_S=0 PYTHONPATH=. uv run python \
  pipelines/kafka/consumer_to_clickhouse.py >/tmp/news-consumer.log 2>&1 &
bg_pids+=($!)

echo "[5/5] running producer (Ctrl+C to stop)..."
echo "tail logs: tail -f /tmp/news-streamlit.log /tmp/news-consumer.log"
echo
PYTHONPATH=. uv run python pipelines/kafka/producer_rss.py
