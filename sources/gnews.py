"""Google News RSS source. Acts as a force multiplier on top of the
per-outlet feeds in sources/rss.py.

Each (country, topic|query) pair from data/config/gnews_queries.yaml
becomes one Google News RSS feed. Each feed returns ~100 entries, so
the catalogue is the main lever for hitting the project's target
article count.

EA framing. This is the [[Variety]] amplifier. Topic-sliced Google
News surfaces Long Tail outlets the curated per-outlet list cannot
realistically enumerate, while still anchoring on country and
language. Velocity is bounded by Google News update cadence (low) so
periodic polling is sufficient. No Kafka.

Schema matches sources/rss.py: source, country_target, title,
summary, url, published_at, extracted_at. Same dlt resource name
(`articles`) so it lands in the same table as the curated RSS pull
and dedups on URL via merge disposition.
"""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote_plus

import dlt
import feedparser
import yaml
from dlt.common.pendulum import pendulum

_log = logging.getLogger("gnews")

# Google News is more aggressive about throttling than per-outlet RSS,
# so default to fewer workers. Override via env if a run gets 429s.
WORKERS = int(os.environ.get("GNEWS_WORKERS", "12"))

USER_AGENT = "Mozilla/5.0 (compatible; NewsIntelBot/0.1)"

GNEWS_YAML = (
    Path(__file__).resolve().parents[1] / "data" / "config" / "gnews_queries.yaml"
)

TOPIC_URL = (
    "https://news.google.com/rss/headlines/section/topic/{topic}"
    "?hl={lang}&gl={gl}&ceid={ceid}"
)
SEARCH_URL = "https://news.google.com/rss/search?q={q}&hl={lang}&gl={gl}&ceid={ceid}"

REQUEST_DELAY_S = 0.2  # be polite to Google News


def _load_query_catalogue() -> list[dict[str, str]]:
    """Flatten gnews_queries.yaml to one entry per Google News feed."""
    with GNEWS_YAML.open() as fh:
        config = yaml.safe_load(fh)
    feeds: list[dict[str, str]] = []
    for country_code, spec in (config.get("countries") or {}).items():
        lang = spec.get("lang", "en")
        gl = spec.get("gl", country_code)
        ceid = spec.get("ceid", f"{gl}:{lang}")
        for topic in spec.get("topics") or []:
            feeds.append(
                {
                    "country_target": country_code,
                    "source": f"Google News {country_code} / {topic}",
                    "rss": TOPIC_URL.format(topic=topic, lang=lang, gl=gl, ceid=ceid),
                    "language": lang,
                }
            )
        for query in spec.get("queries") or []:
            feeds.append(
                {
                    "country_target": country_code,
                    "source": f"Google News {country_code} / q:{query}",
                    "rss": SEARCH_URL.format(
                        q=quote_plus(query), lang=lang, gl=gl, ceid=ceid
                    ),
                    "language": lang,
                }
            )
    return feeds


def _parse_published(entry: feedparser.FeedParserDict) -> str | None:
    """Map RSS published/updated to ISO8601 UTC. None if unparseable."""
    for key in ("published_parsed", "updated_parsed"):
        val = entry.get(key)
        if val:
            return pendulum.from_timestamp(
                pendulum.datetime(*val[:6]).timestamp(), tz="UTC"
            ).to_iso8601_string()
    return None


def _summary(entry: feedparser.FeedParserDict) -> str | None:
    """First non-empty summary-ish field. HTML cleaning is downstream."""
    for key in ("summary", "description"):
        val = entry.get(key)
        if val:
            return val
    return None


def _fetch(feed: dict[str, str]):
    """feedparser.parse with the bot UA. Runs in a worker thread."""
    return feedparser.parse(feed["rss"], agent=USER_AGENT)


def iter_gnews_articles() -> Iterator[dict[str, Any]]:
    """One row per Google News RSS entry across the query catalogue.
    Feeds fan out across GNEWS_WORKERS threads (default 12). A single
    failing feed does not abort the run. Plain generator so the
    combined pipeline can chain it with the per-outlet iterator into
    one dlt resource."""
    extracted_at = pendulum.now("UTC").to_iso8601_string()
    feeds = _load_query_catalogue()
    _log.info("gnews: %d feeds, %d workers", len(feeds), WORKERS)
    ok = bad = empty = 0
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_fetch, f): f for f in feeds}
        for fut in as_completed(futures):
            feed = futures[fut]
            label = feed["source"]
            try:
                parsed = fut.result()
            except Exception as exc:  # noqa: BLE001
                _log.warning("gnews: %s -> error: %s", label, exc)
                bad += 1
                continue
            if parsed.bozo and not parsed.entries:
                _log.warning("gnews: %s -> empty/malformed", label)
                empty += 1
                continue
            emitted = 0
            for entry in parsed.entries:
                url = entry.get("link")
                if not url:
                    continue
                emitted += 1
                yield {
                    "source": feed["source"],
                    "country_target": feed["country_target"],
                    "title": entry.get("title"),
                    "summary": _summary(entry),
                    "url": url,
                    "published_at": _parse_published(entry),
                    "extracted_at": extracted_at,
                }
            ok += 1
            _log.info("gnews: %s -> %d entries", label, emitted)
    _log.info(
        "gnews: done. ok=%d empty=%d bad=%d elapsed=%.1fs",
        ok,
        empty,
        bad,
        time.monotonic() - started,
    )


@dlt.resource(name="articles", primary_key="url", write_disposition="merge")
def gnews_articles() -> Iterator[dict[str, Any]]:
    """dlt resource wrapper. Used when this source is run standalone."""
    yield from iter_gnews_articles()


@dlt.source(name="gnews")
def gnews_source() -> Any:
    """Google News RSS source. See sources/rss.py for the per-outlet variant."""
    yield gnews_articles()
