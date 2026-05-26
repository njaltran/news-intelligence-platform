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
from sources.rss import (
    BODY_WORKERS,
    FETCH_BODY,
    _fetch_body,
    _should_skip,
)

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
#
# The default now spans short-burst freshness (1h, 1d, 7d) plus
# historical backfill (30d, 180d, 1y) so the first sweep populates
# months of context rather than days. Each extra window adds ~100
# entries per query × lang at the cost of one more HTTP fetch.
SEARCH_WINDOWS: tuple[str, ...] = tuple(
    w.strip() for w in os.environ.get(
        "GNEWS_WINDOWS", ",1h,1d,7d,30d,180d,1y"
    ).split(",")
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

# Circuit breaker. Google soft-throttles by returning HTTP 200 with an
# empty/malformed RSS payload instead of 429 once it decides the client
# is hitting too hard. The bozo+empty branch below cannot distinguish
# that from a genuinely empty feed, so we watch the global ok-rate over
# the first GNEWS_EARLY_CHECK feeds and abort the sweep if it stays
# below GNEWS_MIN_OK_RATE. Caller (producer) loops on its own interval,
# so abort just defers the work to the next sweep instead of burning
# 30 minutes on a soft-block.
GNEWS_EARLY_CHECK = int(os.environ.get("GNEWS_EARLY_CHECK", "60"))
GNEWS_MIN_OK_RATE = float(os.environ.get("GNEWS_MIN_OK_RATE", "0.05"))


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


def _interleave_by_country(feeds: list[dict[str, str]]) -> list[dict[str, str]]:
    """Round-robin across country_target so we don't submit every DE
    feed back-to-back. Same set, different order. Spreads load across
    Google's per-country endpoints and avoids the thundering-herd that
    trips Google's soft-throttle on a single locale."""
    by_country: dict[str, list[dict[str, str]]] = {}
    for f in feeds:
        by_country.setdefault(f["country_target"], []).append(f)
    out: list[dict[str, str]] = []
    while any(by_country.values()):
        for country in list(by_country.keys()):
            bucket = by_country[country]
            if bucket:
                out.append(bucket.pop(0))
    return out


def iter_gnews_articles(
    countries: list[str] | None = None,
    shard: tuple[int, int] | None = None,
) -> Iterator[dict[str, Any]]:
    """One row per Google News RSS entry across the query catalogue.
    Feeds fan out across GNEWS_WORKERS threads (default 12). A single
    failing feed does not abort the run. Plain generator so the
    combined pipeline can chain it with the per-outlet iterator into
    one dlt resource.

    `countries` (optional) restricts feeds to those country codes.
    Lets multiple producer processes shard the catalogue.

    Includes a soft-throttle circuit breaker: if the ok-rate stays
    below GNEWS_MIN_OK_RATE after GNEWS_EARLY_CHECK feeds, abort the
    sweep so the caller can move on instead of burning ~30 minutes on
    a Google IP-block."""
    feeds = _load_query_catalogue()
    if countries:
        wanted = {c.strip() for c in countries}
        feeds = [f for f in feeds if f["country_target"] in wanted]
    feeds = _interleave_by_country(feeds)
    if shard is not None:
        n, m = shard
        feeds = [f for i, f in enumerate(feeds) if i % m == n]
    _log.info("gnews: %d feeds, %d workers", len(feeds), WORKERS)
    ok = bad = empty = 0
    started = time.monotonic()
    aborted = False
    body_pool = (
        ThreadPoolExecutor(max_workers=BODY_WORKERS, thread_name_prefix="gnews-body")
        if FETCH_BODY
        else None
    )
    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(_fetch, f): f for f in feeds}
            try:
                for fut in as_completed(futures):
                    if aborted:
                        continue
                    feed = futures[fut]
                    label = feed["source"]
                    try:
                        parsed = fut.result()
                    except Exception as exc:  # noqa: BLE001
                        _log.warning("gnews: %s -> error: %s", label, exc)
                        bad += 1
                    else:
                        if parsed.bozo and not parsed.entries:
                            _log.warning("gnews: %s -> empty/malformed", label)
                            empty += 1
                        else:
                            rows: list[dict[str, Any]] = []
                            for entry in parsed.entries:
                                url = entry.get("link")
                                if not url:
                                    continue
                                rows.append(
                                    {
                                        "source": feed["source"],
                                        "country_target": feed["country_target"],
                                        "title": entry.get("title"),
                                        "summary": _summary(entry),
                                        "url": url,
                                        "published_at": _parse_published(entry),
                                        "extracted_at": pendulum.now("UTC").to_iso8601_string(),
                                    }
                                )
                            if body_pool is not None and rows:
                                body_futures: dict[Any, int] = {}
                                skipped = 0
                                for i, r in enumerate(rows):
                                    if _should_skip(r["url"]):
                                        skipped += 1
                                        yield r
                                    else:
                                        body_futures[
                                            body_pool.submit(_fetch_body, r["url"])
                                        ] = i
                                filled = 0
                                for bfut in as_completed(body_futures):
                                    idx = body_futures[bfut]
                                    try:
                                        body = bfut.result()
                                    except Exception as exc:  # noqa: BLE001
                                        _log.debug("body: %s -> %s", rows[idx]["url"], exc)
                                        body = None
                                    if body:
                                        rows[idx]["body"] = body
                                        filled += 1
                                    yield rows[idx]
                                _log.info(
                                    "gnews: %s -> %d entries, %d bodies (%d skipped)",
                                    label, len(rows), filled, skipped,
                                )
                            else:
                                _log.info("gnews: %s -> %d entries", label, len(rows))
                                for row in rows:
                                    yield row
                            ok += 1

                    processed = ok + empty + bad
                    if (
                        not aborted
                        and processed >= GNEWS_EARLY_CHECK
                        and ok / processed < GNEWS_MIN_OK_RATE
                    ):
                        _log.warning(
                            "gnews: ok=%d empty=%d bad=%d after %d feeds "
                            "(ok-rate %.1f%% < %.1f%%). Looks like Google is "
                            "soft-throttling. Aborting sweep, next sweep retries.",
                            ok,
                            empty,
                            bad,
                            processed,
                            100 * ok / processed,
                            100 * GNEWS_MIN_OK_RATE,
                        )
                        aborted = True
                        for pending in futures:
                            if not pending.done():
                                pending.cancel()
            finally:
                # Drop still-running fetches as soon as the with-block exits.
                # Without cancel_futures, ThreadPoolExecutor.shutdown blocks
                # waiting on every queued future to be picked up first.
                pool.shutdown(wait=False, cancel_futures=True)
    finally:
        if body_pool is not None:
            body_pool.shutdown(wait=True)
    _log.info(
        "gnews: done. ok=%d empty=%d bad=%d aborted=%s elapsed=%.1fs",
        ok,
        empty,
        bad,
        aborted,
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
