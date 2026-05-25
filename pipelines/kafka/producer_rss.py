"""Kafka producer: RSS + Google News articles.

Reuses the iterators from sources/rss.py and sources/gnews.py so this
producer publishes the same ~120 curated section feeds and the
country-sliced Google News topic + query feeds the batch arm reads.

EA framing. This is the streaming-side counterpart of
pipelines/ingest_rss.py. Same Variety + Long Tail input set, different
delivery: instead of writing one duckdb snapshot, every article lands
as a message on Kafka topic `unified_news_topic`. The consumer
(consumer_to_clickhouse.py) drains it to ClickHouse via dlt.

Single-shot by default. Set PRODUCER_INTERVAL_S=300 (or any positive
value in seconds) to keep polling all feeds on that interval. Useful
when you want the dashboard to keep refreshing with newly-published
articles instead of stopping after the first sweep.

Message schema matches the project canon (source, country_target,
title, summary, url, published_at, extracted_at) so the consumer
sees a uniform stream regardless of upstream feed.

Run from the repo root after bringing the broker up:

    docker compose -f infra/docker-compose.yml up -d
    PYTHONPATH=. uv run python pipelines/kafka/producer_rss.py
"""

from __future__ import annotations

import json
import os
import socket
import time
from collections import Counter

from confluent_kafka import Producer

from pipelines.kafka._log import get_logger
from sources.gnews import iter_gnews_articles
from sources.rss import iter_rss_articles

TOPIC = "unified_news_topic"
BOOTSTRAP_SERVERS = "localhost:9092"
PROGRESS_EVERY = 500
INTERVAL_S = int(os.environ.get("PRODUCER_INTERVAL_S", "0"))
# Sleep this many ms between messages so a sweep trickles out instead
# of bursting. 0 = unpaced (legacy behaviour, useful when bulk-loading).
MSG_DELAY_MS = float(os.environ.get("PRODUCER_MSG_DELAY_MS", "0"))

log = get_logger("producer")
# Hook the source loggers into the same handler so feed-level lines
# stream alongside the producer's own.
get_logger("rss")
get_logger("gnews")

# Bound feedparser network waits so one stuck feed cannot stall the producer.
socket.setdefaulttimeout(20)


def _delivery_report(err, _msg) -> None:
    if err is not None:
        log.error("delivery failed: %s", err)


def _one_sweep(producer: Producer) -> int:
    by_country: Counter[str] = Counter()
    started = time.monotonic()
    count = 0
    delay = MSG_DELAY_MS / 1000.0
    for article in _chained_articles():
        producer.produce(
            TOPIC,
            json.dumps(article, ensure_ascii=False).encode("utf-8"),
            callback=_delivery_report,
        )
        by_country[article.get("country_target") or "?"] += 1
        count += 1
        if delay > 0:
            time.sleep(delay)
        if count % PROGRESS_EVERY == 0:
            producer.poll(0)
            rate = count / max(time.monotonic() - started, 1e-3)
            log.info(
                "produced %d msgs (%.0f msg/s) by_country=%s",
                count,
                rate,
                dict(by_country),
            )

    producer.flush()
    elapsed = time.monotonic() - started
    log.info(
        "sweep done. produced=%d elapsed=%.1fs rate=%.0f msg/s by_country=%s",
        count,
        elapsed,
        count / max(elapsed, 1e-3),
        dict(by_country),
    )
    return count


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

    log.info(
        "producing to topic=%s bootstrap=%s interval_s=%d msg_delay_ms=%.2f",
        TOPIC,
        BOOTSTRAP_SERVERS,
        INTERVAL_S,
        MSG_DELAY_MS,
    )

    sweep = 0
    total = 0
    while True:
        sweep += 1
        log.info("starting sweep %d", sweep)
        total += _one_sweep(producer)
        if INTERVAL_S <= 0:
            log.info("done. total_produced=%d sweeps=%d", total, sweep)
            return
        log.info(
            "sweep %d complete. total=%d. sleeping %ds before next sweep.",
            sweep,
            total,
            INTERVAL_S,
        )
        time.sleep(INTERVAL_S)


def _chained_articles():
    """Curated per-outlet feeds first, then the Google News amplifier."""
    yield from iter_rss_articles()
    yield from iter_gnews_articles()


if __name__ == "__main__":
    run()
