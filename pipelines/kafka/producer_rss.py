"""Kafka producer: RSS + Google News articles.

Reuses the iterators from sources/rss.py and sources/gnews.py so this
producer publishes the same ~120 curated section feeds and the
country-sliced Google News topic + query feeds the batch arm reads.

EA framing. This is the streaming-side counterpart of
pipelines/ingest_rss.py. Same Variety + Long Tail input set, different
delivery: instead of writing one duckdb snapshot, every article lands
as a message on Kafka topic `unified_news_topic`. The consumer
(consumer_to_clickhouse.py) drains it to ClickHouse via dlt.

Message schema matches the project canon (source, country_target,
title, summary, url, published_at, extracted_at) so the consumer
sees a uniform stream regardless of upstream feed.

Run from the repo root after bringing the broker up:

    docker compose -f infra/docker-compose.yml up -d
    PYTHONPATH=. uv run python pipelines/kafka/producer_rss.py
"""

from __future__ import annotations

import json
import socket

from confluent_kafka import Producer

from sources.gnews import iter_gnews_articles
from sources.rss import iter_rss_articles

TOPIC = "unified_news_topic"
BOOTSTRAP_SERVERS = "localhost:9092"

# Bound feedparser network waits so one stuck feed cannot stall the producer.
socket.setdefaulttimeout(20)


def _delivery_report(err, _msg) -> None:
    if err is not None:
        print(f"delivery failed: {err}")  # noqa: T201


def run() -> None:
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "rss-producer",
            "socket.timeout.ms": 10000,
            "queue.buffering.max.messages": 200000,
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    )

    count = 0
    for article in _chained_articles():
        producer.produce(
            TOPIC,
            json.dumps(article, ensure_ascii=False).encode("utf-8"),
            callback=_delivery_report,
        )
        count += 1
        # Poll periodically so delivery callbacks fire and the
        # internal queue does not back up.
        if count % 500 == 0:
            producer.poll(0)
            print(f"produced {count} messages")  # noqa: T201

    producer.flush()
    print(f"done. produced {count} messages to {TOPIC}")  # noqa: T201


def _chained_articles():
    """Curated per-outlet feeds first, then the Google News amplifier."""
    yield from iter_rss_articles()
    yield from iter_gnews_articles()


if __name__ == "__main__":
    run()
