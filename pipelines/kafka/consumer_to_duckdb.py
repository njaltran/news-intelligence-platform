"""Kafka consumer: reads the unified news topic and lands it in DuckDB via dlt.

EA framing. This is the consumer side of the Kappa path. dlt does schema
inference on the messages (Variety axis of the 5 Vs) and writes one row
per article into duckdb. The broker decouples producer cadence from
consumer cadence, so any producer outage does not block ingestion and
old offsets stay replayable.

Run (from repo root, after `docker compose -f infra/docker-compose.yml up -d`):

    uv run python pipelines/kafka/consumer_to_duckdb.py
"""

import json
import time

import dlt
from confluent_kafka import Consumer


def consume_and_load(topic: str, dataset_name: str) -> None:
    conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "news_loaders",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    print(f"Starting consumer for topic: {topic}...")

    def message_generator():
        start_time = time.time()
        timeout = 10  # stop if no messages for 10 seconds
        received_any = False

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    if received_any and (time.time() - start_time > timeout):
                        print("Timeout reached, closing generator.")
                        break
                    continue

                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                received_any = True
                start_time = time.time()
                yield json.loads(msg.value().decode("utf-8"))
        finally:
            consumer.close()

    pipeline = dlt.pipeline(
        pipeline_name="kafka_to_duckdb",
        destination="duckdb",
        dataset_name=dataset_name,
    )
    load_info = pipeline.run(message_generator(), table_name="news_articles")
    print(load_info)


if __name__ == "__main__":
    consume_and_load("unified_news_topic", "unified_news_data")
