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

The dataset name `news` matches the database created by the
clickhouse service in infra/docker-compose.yml.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator

import dlt
from confluent_kafka import Consumer

TOPIC = "unified_news_topic"
DATASET = "news"
TABLE = "articles"

BOOTSTRAP_SERVERS = "localhost:9092"
CONSUMER_GROUP = "news_loaders_clickhouse"

# Stop after this many seconds with no new messages. Keeps the local
# dev loop bounded; a real deployment would run as a long-lived daemon.
IDLE_TIMEOUT_S = 15
POLL_TIMEOUT_S = 1.0


def _messages(consumer: Consumer) -> Iterator[dict]:
    last_msg_at = time.time()
    received_any = False
    while True:
        msg = consumer.poll(POLL_TIMEOUT_S)
        if msg is None:
            if received_any and (time.time() - last_msg_at > IDLE_TIMEOUT_S):
                print(f"no messages for {IDLE_TIMEOUT_S}s, draining.")  # noqa: T201
                return
            continue
        if msg.error():
            print(f"consumer error: {msg.error()}")  # noqa: T201
            continue
        received_any = True
        last_msg_at = time.time()
        try:
            yield json.loads(msg.value().decode("utf-8"))
        except json.JSONDecodeError as exc:
            print(f"skip bad message: {exc}")  # noqa: T201
            continue


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

    try:
        load_info = pipeline.run(
            dlt.resource(
                _messages(consumer),
                name=TABLE,
                primary_key="url",
                write_disposition="merge",
            )
        )
        print(load_info)  # noqa: T201
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
