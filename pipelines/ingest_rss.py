"""Entry point for RSS-based ingestion (curated outlets + Google News).

Run from the repo root so dlt resolves .dlt/secrets.toml from cwd:

    uv run python pipelines/ingest_rss.py

Behaviour. Two sources land in the same `articles` table:

  * sources/rss.py: curated per-outlet section feeds in
    data/config/sources.yaml (Variety V).
  * sources/gnews.py: Google News topic + search feeds in
    data/config/gnews_queries.yaml (Long Tail amplifier).

Both write with merge disposition on `url`, so re-runs accumulate
without duplicates. dev_mode is OFF so the dataset persists across
runs (the project target is many thousands of unique URLs over the
6-week window, not a per-run snapshot).
"""

from __future__ import annotations

import socket
from typing import Any, Iterator

import dlt

from sources.gnews import iter_gnews_articles
from sources.rss import iter_rss_articles

FEED_SOCKET_TIMEOUT_S = 20


@dlt.resource(name="articles", primary_key="url", write_disposition="merge")
def articles() -> Iterator[dict[str, Any]]:
    """Single resource over both inputs so they land in one table.
    Two separate @dlt.resource(name='articles') functions would
    collide; chaining their iterators avoids that and preserves the
    merge-on-url dedup."""
    yield from iter_rss_articles()
    yield from iter_gnews_articles()


@dlt.source(name="rss")
def rss_source() -> Any:
    yield articles()


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rss",
        destination="duckdb",
        dataset_name="rss_raw",
    )
    # Some publisher feeds hang. Bound the socket wait only for the
    # duration of this run so we don't change the default for dlt
    # internals or anything that imports this module.
    prev_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(FEED_SOCKET_TIMEOUT_S)
    try:
        load_info = pipeline.run(rss_source())
    finally:
        socket.setdefaulttimeout(prev_timeout)
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    run()
