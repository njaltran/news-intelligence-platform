"""Kafka consumer: reads `unified_news_topic` and lands it in
ClickHouse via dlt.

EA framing. This is the consumer side of the Kappa path that the
project uses for stream processing. dlt does schema inference on
incoming JSON messages (Variety V of the 5 Vs) and writes one row
per article into a columnar / MPP analytical DWH (Watson 2014
data-warehouse generation). The broker decouples producer cadence
from consumer cadence, so any producer outage does not block
ingestion and old offsets stay replayable.

Run from the repo root (broker + ClickHouse must be up first):

    docker compose -f infra/docker-compose.yml up -d
    uv run python pipelines/kafka/consumer_to_clickhouse.py

Consumes in micro-batches so the dashboard sees rows arrive
incrementally rather than all at once at the end. Tunables:

  CONSUMER_BATCH_MAX        max msgs per dlt load (default 500)
  CONSUMER_BATCH_FLUSH_S    seconds before flushing a partial batch (default 5)
  CONSUMER_IDLE_TIMEOUT_S   exit after this many idle seconds (default 15, 0 = never)

The dataset name `news` matches the database created by the
clickhouse service in infra/docker-compose.yml.
"""

from __future__ import annotations

import json
import os
import signal
import time
from collections import Counter

import dlt
from confluent_kafka import Consumer

from pipelines.kafka._log import get_logger

TOPIC = "unified_news_topic"
DATASET = "news"
TABLE = "articles"

BOOTSTRAP_SERVERS = "localhost:9092"
CONSUMER_GROUP = "news_loaders_clickhouse"

BATCH_MAX = int(os.environ.get("CONSUMER_BATCH_MAX", "500"))
BATCH_FLUSH_S = float(os.environ.get("CONSUMER_BATCH_FLUSH_S", "5"))
IDLE_TIMEOUT_S = int(os.environ.get("CONSUMER_IDLE_TIMEOUT_S", "15"))
# Grace period after shutdown signal to drain remaining Kafka backlog.
SHUTDOWN_DRAIN_S = float(os.environ.get("CONSUMER_SHUTDOWN_DRAIN_S", "20"))
POLL_TIMEOUT_S = 0.5

log = get_logger("consumer")

_shutdown_at: float | None = None


def _request_shutdown(signum, _frame) -> None:
    global _shutdown_at
    if _shutdown_at is None:
        _shutdown_at = time.time()
        log.info(
            "received signal %d, draining for %.0fs then exiting",
            signum,
            SHUTDOWN_DRAIN_S,
        )


def _drain_batch(consumer: Consumer) -> list[dict]:
    """Collect up to BATCH_MAX msgs or until BATCH_FLUSH_S elapsed."""
    batch: list[dict] = []
    deadline = time.time() + BATCH_FLUSH_S
    while time.time() < deadline and len(batch) < BATCH_MAX:
        msg = consumer.poll(POLL_TIMEOUT_S)
        if msg is None:
            continue
        if msg.error():
            log.error("kafka error: %s", msg.error())
            continue
        batch.append(json.loads(msg.value().decode("utf-8")))
    return batch


def run() -> None:
    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([TOPIC])
    log.info(
        "subscribed topic=%s group=%s sink=clickhouse.%s.%s batch_max=%d flush_s=%.1f idle_timeout_s=%d",
        TOPIC,
        CONSUMER_GROUP,
        DATASET,
        TABLE,
        BATCH_MAX,
        BATCH_FLUSH_S,
        IDLE_TIMEOUT_S,
    )

    pipeline = dlt.pipeline(
        pipeline_name="kafka_to_clickhouse",
        destination="clickhouse",
        dataset_name=DATASET,
    )

    last_msg_at = time.time()
    received_any = False
    total = 0
    by_country: Counter[str] = Counter()
    started = time.monotonic()

    try:
        while True:
            batch_started = time.monotonic()
            batch = _drain_batch(consumer)
            if batch:
                received_any = True
                last_msg_at = time.time()
                load_started = time.monotonic()
                pipeline.run(
                    dlt.resource(
                        iter(batch),
                        name=TABLE,
                        primary_key="url",
                        write_disposition="merge",
                    )
                )
                for row in batch:
                    by_country[row.get("country_target") or "?"] += 1
                total += len(batch)
                load_s = time.monotonic() - load_started
                elapsed = time.monotonic() - started
                rate = total / max(elapsed, 1e-3)
                log.info(
                    "batch=%d load=%.2fs total=%d rate=%.0f msg/s by_country=%s",
                    len(batch),
                    load_s,
                    total,
                    rate,
                    dict(by_country),
                )
                continue
            if (
                IDLE_TIMEOUT_S > 0
                and received_any
                and (time.time() - last_msg_at) > IDLE_TIMEOUT_S
            ):
                log.info(
                    "no messages for %ds, exiting. total=%d elapsed=%.1fs",
                    IDLE_TIMEOUT_S,
                    total,
                    time.monotonic() - started,
                )
                return
            if _shutdown_at is not None:
                drained_for = time.time() - _shutdown_at
                idle_for = time.time() - last_msg_at
                # Exit as soon as backlog is drained (idle > 2s) or the
                # hard drain budget is up, so Ctrl+C is responsive.
                if drained_for >= SHUTDOWN_DRAIN_S or (received_any and idle_for >= 2.0):
                    log.info(
                        "shutdown drain complete. total=%d drained_for=%.1fs",
                        total,
                        drained_for,
                    )
                    return
    finally:
        log.info("closing consumer (commits final offsets). total=%d", total)
        consumer.close()


if __name__ == "__main__":
    run()
