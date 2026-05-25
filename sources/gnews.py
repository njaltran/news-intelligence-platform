"""Google News RSS source. Acts as a force multiplier on top of the
per-outlet feeds in sources/rss.py.

Each (country, topic|query) pair from data/config/gnews_queries.yaml
becomes a Google News RSS feed. Each feed is hard-capped at ~100
entries server-side, so to scale we fan every search query across
multiple `when:` time windows (and optionally multiple UI languages
per country) -- each combo is its own 100-cap feed.

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
import random
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

# Google News RSS hard-caps each feed at ~100 entries. To break the cap
# we fan each query out across `when:` time windows. An empty string
# means "no when: suffix" (Google's default relevance window). Each
# window is its own 100-cap feed; URL dedup in the dlt merge disposition
# absorbs overlap. Override via GNEWS_WINDOWS (comma-separated, "" = no
# operator). Order matters only for log readability.
SEARCH_WINDOWS: tuple[str, ...] = tuple(
    w.strip() for w in os.environ.get("GNEWS_WINDOWS", ",1h,1d,7d").split(",")
)


def _load_query_catalogue() -> list[dict[str, str]]:
    """Flatten gnews_queries.yaml to one entry per Google News feed.

    For each country, optional `langs:` list fans search + topic feeds
    across multiple UI languages (e.g. KZ in ru + kk). Default is the
    single `lang:` value. Each search query then fans out across
    SEARCH_WINDOWS to bypass the per-feed 100-entry cap.
    """
    with GNEWS_YAML.open() as fh:
        config = yaml.safe_load(fh)
    feeds: list[dict[str, str]] = []
    for country_code, spec in (config.get("countries") or {}).items():
        default_lang = spec.get("lang", "en")
        gl = spec.get("gl", country_code)
        langs = spec.get("langs") or [default_lang]
        for lang in langs:
            # ceid: per-lang derived unless single-lang and explicit.
            if len(langs) == 1 and spec.get("ceid"):
                ceid = spec["ceid"]
            else:
                ceid = f"{gl}:{lang}"
            for topic in spec.get("topics") or []:
                feeds.append(
                    {
                        "country_target": country_code,
                        "source": f"Google News {country_code}/{lang} / {topic}",
                        "rss": TOPIC_URL.format(
                            topic=topic, lang=lang, gl=gl, ceid=ceid
                        ),
                        "language": lang,
                    }
                )
            for query in spec.get("queries") or []:
                for window in SEARCH_WINDOWS:
                    q_text = f"{query} when:{window}" if window else query
                    win_tag = window or "all"
                    feeds.append(
                        {
                            "country_target": country_code,
                            "source": (
                                f"Google News {country_code}/{lang} "
                                f"/ q:{query} ({win_tag})"
                            ),
                            "rss": SEARCH_URL.format(
                                q=quote_plus(q_text), lang=lang, gl=gl, ceid=ceid
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


MAX_RETRIES = int(os.environ.get("GNEWS_MAX_RETRIES", "5"))
BACKOFF_BASE_S = float(os.environ.get("GNEWS_BACKOFF_BASE_S", "1.0"))
BACKOFF_CAP_S = float(os.environ.get("GNEWS_BACKOFF_CAP_S", "60.0"))


def _fetch(feed: dict[str, str]):
    """feedparser.parse with the bot UA, exponential backoff on 429.

    feedparser swallows HTTP errors and surfaces the status code on
    `parsed.status`. On 429 (Google throttling) we sleep base*2^attempt
    + jitter, capped at BACKOFF_CAP_S, up to GNEWS_MAX_RETRIES retries.
    Other errors fall through to the caller's try/except.
    """
    for attempt in range(MAX_RETRIES + 1):
        parsed = feedparser.parse(feed["rss"], agent=USER_AGENT)
        if getattr(parsed, "status", None) != 429:
            return parsed
        if attempt == MAX_RETRIES:
            _log.warning(
                "gnews: %s -> 429 after %d retries, giving up",
                feed["source"],
                MAX_RETRIES,
            )
            return parsed
        delay = min(BACKOFF_BASE_S * (2**attempt), BACKOFF_CAP_S)
        delay += random.uniform(0, delay * 0.25)
        _log.info(
            "gnews: %s -> 429, retry %d/%d in %.1fs",
            feed["source"],
            attempt + 1,
            MAX_RETRIES,
            delay,
        )
        time.sleep(delay)
    return parsed


def iter_gnews_articles() -> Iterator[dict[str, Any]]:
    """One row per Google News RSS entry across the query catalogue.
    Feeds fan out across GNEWS_WORKERS threads (default 12). A single
    failing feed does not abort the run. Plain generator so the
    combined pipeline can chain it with the per-outlet iterator into
    one dlt resource."""
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
                    "extracted_at": pendulum.now("UTC").to_iso8601_string(),
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
