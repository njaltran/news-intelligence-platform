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
import time

import dlt
from confluent_kafka import Consumer

TOPIC = "unified_news_topic"
DATASET = "news"
TABLE = "articles"

BOOTSTRAP_SERVERS = "localhost:9092"
CONSUMER_GROUP = "news_loaders_clickhouse"

BATCH_MAX = int(os.environ.get("CONSUMER_BATCH_MAX", "500"))
BATCH_FLUSH_S = float(os.environ.get("CONSUMER_BATCH_FLUSH_S", "5"))
IDLE_TIMEOUT_S = int(os.environ.get("CONSUMER_IDLE_TIMEOUT_S", "15"))
POLL_TIMEOUT_S = 0.5


def _drain_batch(consumer: Consumer) -> list[dict]:
    """Collect up to BATCH_MAX msgs or until BATCH_FLUSH_S elapsed."""
    batch: list[dict] = []
    deadline = time.time() + BATCH_FLUSH_S
    while time.time() < deadline and len(batch) < BATCH_MAX:
        msg = consumer.poll(POLL_TIMEOUT_S)
        if msg is None:
            continue
        if msg.error():
            print(f"consumer error: {msg.error()}")  # noqa: T201
            continue
        try:
            batch.append(json.loads(msg.value().decode("utf-8")))
        except json.JSONDecodeError as exc:
            print(f"skip bad message: {exc}")  # noqa: T201
    return batch


def run() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([TOPIC])
    print(f"consuming {TOPIC} -> clickhouse.{DATASET}.{TABLE}")  # noqa: T201

    pipeline = dlt.pipeline(
        pipeline_name="kafka_to_clickhouse",
        destination="clickhouse",
        dataset_name=DATASET,
    )

    last_msg_at = time.time()
    received_any = False
    total = 0
    try:
        while True:
            batch = _drain_batch(consumer)
            if batch:
                received_any = True
                last_msg_at = time.time()
                pipeline.run(
                    dlt.resource(
                        iter(batch),
                        name=TABLE,
                        primary_key="url",
                        write_disposition="merge",
                    )
                )
                total += len(batch)
                print(f"loaded batch={len(batch)} total={total}")  # noqa: T201
                continue
            if (
                IDLE_TIMEOUT_S > 0
                and received_any
                and (time.time() - last_msg_at) > IDLE_TIMEOUT_S
            ):
                print(f"no messages for {IDLE_TIMEOUT_S}s, exiting.")  # noqa: T201
                return
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
