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
import yaml
from dlt.common.pendulum import pendulum

_log = logging.getLogger("rss")

WORKERS = int(os.environ.get("RSS_WORKERS", "24"))

# Some publishers 403 the default feedparser UA. Identify ourselves as
# a real-looking bot so MM/KZ outlets behind light WAFs let us through.
USER_AGENT = "Mozilla/5.0 (compatible; NewsIntelBot/0.1)"

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


def iter_rss_articles() -> Iterator[dict[str, Any]]:
    """One row per RSS entry across all outlets with `scrape: rss` in
    sources.yaml. Feeds are fetched in parallel via a ThreadPool
    (RSS_WORKERS, default 24). A single dead feed does not abort the
    run. Plain generator so the combined pipeline can chain it with
    the Google News iterator into one dlt resource. `extracted_at` is
    stamped at yield time so downstream dashboards can show a real
    arrivals timeline rather than a single sweep-start spike."""
    outlets = _load_rss_outlets()
    _log.info("rss: %d outlets, %d workers", len(outlets), WORKERS)
    ok = bad = empty = 0
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
            emitted = 0
            for entry in parsed.entries:
                url = entry.get("link")
                if not url:
                    continue
                emitted += 1
                yield {
                    "source": outlet["source"],
                    "country_target": outlet["country_target"],
                    "title": entry.get("title"),
                    "summary": _summary(entry),
                    "url": url,
                    "published_at": _parse_published(entry),
                    "extracted_at": pendulum.now("UTC").to_iso8601_string(),
                }
            ok += 1
            _log.info("rss: %s -> %d entries", label, emitted)
    _log.info("rss: done. ok=%d empty=%d bad=%d", ok, empty, bad)


@dlt.resource(name="articles", primary_key="url", write_disposition="merge")
def rss_articles() -> Iterator[dict[str, Any]]:
    """dlt resource wrapper. Used when this source is run standalone."""
    yield from iter_rss_articles()


@dlt.source(name="rss")
def rss_source() -> Any:
    """RSS source. See sources/gdelt.py for the equivalent API pattern."""
    yield rss_articles()
