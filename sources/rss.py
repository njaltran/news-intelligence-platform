"""RSS source. Pulls one row per RSS entry across all outlets in
`data/config/sources.yaml` with `scrape: rss`. Normalises to the
project schema (source, country_target, title, summary, url,
published_at, extracted_at).

EA framing. Variety (multi-country, multi-language viewpoints) and
Long Tail (MM, KZ outlets that no aggregator covers) are the two Vs
this resource defends. Velocity is bounded by publisher cadence, so
periodic polling is sufficient. No Kafka.
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterator

import dlt
import feedparser
import requests
import trafilatura
import yaml
from dlt.common.pendulum import pendulum

_log = logging.getLogger("rss")

WORKERS = int(os.environ.get("RSS_WORKERS", "24"))

# Opt-in full-article body fetch. RSS summaries are often truncated or
# pure HTML snippets, so for downstream NLP we want the real body. Off
# by default to keep the batch arm fast; turn on for richer Volume.
FETCH_BODY = os.environ.get("RSS_FETCH_BODY", "0") not in ("", "0", "false", "False")
BODY_WORKERS = int(os.environ.get("RSS_BODY_WORKERS", "16"))
BODY_TIMEOUT_S = float(os.environ.get("RSS_BODY_TIMEOUT_S", "15"))
BODY_MAX_CHARS = int(os.environ.get("RSS_BODY_MAX_CHARS", "20000"))

# UA for feedparser (RSS feeds). Bot-flavoured so MM/KZ outlets behind
# light WAFs let us through.
USER_AGENT = "Mozilla/5.0 (compatible; NewsIntelBot/0.1)"

# UA for article body fetches. A real-looking Chrome string gets us
# past more publisher WAFs than the bot UA. Smoke test in
# scripts/sample_body_extraction.py compared both; see spec for detail.
BODY_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Hosts whose article pages cannot be extracted by a simple GET:
# - news.google.com: every link is a JS-redirect to the publisher;
#   resolving needs Google's batchexecute endpoint which IP-throttles us.
# - reddit.com: needs the Reddit API.
_SKIP_HOST_PREFIXES = (
    "https://news.google.com/",
    "https://www.reddit.com/",
    "https://reddit.com/",
)

SOURCES_YAML = Path(__file__).resolve().parents[1] / "data" / "config" / "sources.yaml"


def _load_rss_outlets() -> list[dict[str, str]]:
    """Read sources.yaml and flatten to one entry per RSS outlet."""
    with SOURCES_YAML.open() as fh:
        config = yaml.safe_load(fh)
    outlets: list[dict[str, str]] = []
    for country_code, entries in (config.get("countries") or {}).items():
        for entry in entries or []:
            if entry.get("scrape") != "rss" or not entry.get("rss"):
                continue
            outlets.append(
                {
                    "country_target": country_code,
                    "source": entry["name"],
                    "rss": entry["rss"],
                    "language": entry.get("language", ""),
                }
            )
    return outlets


def _parse_published(entry: feedparser.FeedParserDict) -> str | None:
    """Map RSS published/updated → ISO8601 UTC. Returns None if unparseable."""
    for key in ("published_parsed", "updated_parsed"):
        val = entry.get(key)
        if val:
            return pendulum.from_timestamp(
                pendulum.datetime(*val[:6]).timestamp(), tz="UTC"
            ).to_iso8601_string()
    return None


def _summary(entry: feedparser.FeedParserDict) -> str | None:
    """First non-empty summary-ish field. HTML left in place. Cleaning
    happens downstream (processing/)."""
    for key in ("summary", "description"):
        val = entry.get(key)
        if val:
            return val
    return None


def _fetch(outlet: dict[str, str]):
    """feedparser.parse with the bot UA. Runs in a worker thread."""
    return feedparser.parse(outlet["rss"], agent=USER_AGENT)


def _extract_body(html: bytes) -> str | None:
    """trafilatura main-text extraction. Input is raw response bytes so
    trafilatura can sniff <meta charset> (avoids the requests
    ISO-8859-1 default that mangled Cyrillic on NUR.KZ etc.). Cap at
    BODY_MAX_CHARS so a giant page cannot inflate one Kafka message."""
    if not html:
        return None
    text = trafilatura.extract(
        html, include_comments=False, include_tables=False, favor_recall=False
    )
    if not text:
        return None
    return text[:BODY_MAX_CHARS]


def _fetch_body(url: str) -> str | None:
    """GET article URL with a browser UA and run _extract_body on the
    raw bytes. Returns None on any network/HTTP error so a single dead
    page does not block the sweep. Callers short-circuit known-
    non-extractable hosts via _should_skip before submitting."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": BODY_UA},
            timeout=BODY_TIMEOUT_S,
            allow_redirects=True,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        _log.debug("body: %s -> %s", url, exc)
        return None
    return _extract_body(resp.content)


def _should_skip(url: str) -> bool:
    """True if we know this URL will not yield a body via plain HTTP GET."""
    return url.startswith(_SKIP_HOST_PREFIXES)


def iter_rss_articles() -> Iterator[dict[str, Any]]:
    """One row per RSS entry across all outlets with `scrape: rss` in
    sources.yaml. Feeds are fetched in parallel via a ThreadPool
    (RSS_WORKERS, default 24). A single dead feed does not abort the
    run. Plain generator so the combined pipeline can chain it with
    the Google News iterator into one dlt resource. `extracted_at` is
    stamped at yield time so downstream dashboards can show a real
    arrivals timeline rather than a single sweep-start spike.

    When RSS_FETCH_BODY=1 each entry's URL is GET'd in a separate
    ThreadPool (RSS_BODY_WORKERS, default 16) and the extracted main
    text lands in `body`. Volume V goes up by ~10-50x per row;
    dlt schema-infers the new column so the consumer auto-evolves."""
    outlets = _load_rss_outlets()
    _log.info(
        "rss: %d outlets, %d workers, fetch_body=%s",
        len(outlets),
        WORKERS,
        FETCH_BODY,
    )
    ok = bad = empty = 0
    body_pool = (
        ThreadPoolExecutor(max_workers=BODY_WORKERS, thread_name_prefix="rss-body")
        if FETCH_BODY
        else None
    )
    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(_fetch, o): o for o in outlets}
            for fut in as_completed(futures):
                outlet = futures[fut]
                label = f"{outlet['country_target']}/{outlet['source']}"
                try:
                    parsed = fut.result()
                except Exception as exc:  # noqa: BLE001
                    _log.warning("rss: %s -> error: %s", label, exc)
                    bad += 1
                    continue
                if parsed.bozo and not parsed.entries:
                    _log.warning("rss: %s -> empty/malformed", label)
                    empty += 1
                    continue
                rows: list[dict[str, Any]] = []
                for entry in parsed.entries:
                    url = entry.get("link")
                    if not url:
                        continue
                    rows.append(
                        {
                            "source": outlet["source"],
                            "country_target": outlet["country_target"],
                            "title": entry.get("title"),
                            "summary": _summary(entry),
                            "url": url,
                            "published_at": _parse_published(entry),
                            "extracted_at": pendulum.now("UTC").to_iso8601_string(),
                        }
                    )
                if body_pool is not None and rows:
                    body_futures = {
                        body_pool.submit(_fetch_body, r["url"]): i
                        for i, r in enumerate(rows)
                        if not _should_skip(r["url"])
                    }
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
                    _log.info(
                        "rss: %s -> %d entries, %d bodies (%d skipped)",
                        label, len(rows), filled,
                        sum(1 for r in rows if _should_skip(r["url"])),
                    )
                else:
                    _log.info("rss: %s -> %d entries", label, len(rows))
                for row in rows:
                    yield row
                ok += 1
    finally:
        if body_pool is not None:
            body_pool.shutdown(wait=True)
    _log.info("rss: done. ok=%d empty=%d bad=%d", ok, empty, bad)


@dlt.resource(name="articles", primary_key="url", write_disposition="merge")
def rss_articles() -> Iterator[dict[str, Any]]:
    """dlt resource wrapper. Used when this source is run standalone."""
    yield from iter_rss_articles()


@dlt.source(name="rss")
def rss_source() -> Any:
    """RSS source. See sources/gdelt.py for the equivalent API pattern."""
    yield rss_articles()
