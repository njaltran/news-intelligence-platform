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
from pathlib import Path
from typing import Any, Iterator

import dlt
import feedparser
import yaml
from dlt.common.pendulum import pendulum

logger = logging.getLogger(__name__)

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


def iter_rss_articles() -> Iterator[dict[str, Any]]:
    """One row per RSS entry across all outlets with `scrape: rss` in
    sources.yaml. A single dead feed does not abort the run. Plain
    generator so the combined pipeline can chain it with the Google
    News iterator into one dlt resource."""
    extracted_at = pendulum.now("UTC").to_iso8601_string()
    outlets = _load_rss_outlets()
    failures: list[tuple[str, str, str]] = []
    for outlet in outlets:
        try:
            parsed = feedparser.parse(outlet["rss"], agent=USER_AGENT)
        except Exception as exc:
            logger.warning(
                "RSS feed fetch failed: %s (%s): %s",
                outlet["source"], outlet["rss"], exc,
            )
            failures.append((outlet["source"], outlet["rss"], repr(exc)))
            continue
        if parsed.bozo and not parsed.entries:
            err = getattr(parsed, "bozo_exception", "no entries")
            logger.warning(
                "RSS feed empty/malformed: %s (%s): %s",
                outlet["source"], outlet["rss"], err,
            )
            failures.append((outlet["source"], outlet["rss"], str(err)))
            continue
        for entry in parsed.entries:
            url = entry.get("link")
            if not url:
                continue
            yield {
                "source": outlet["source"],
                "country_target": outlet["country_target"],
                "title": entry.get("title"),
                "summary": _summary(entry),
                "url": url,
                "published_at": _parse_published(entry),
                "extracted_at": extracted_at,
            }
    if failures:
        logger.warning(
            "RSS run: %d/%d curated feeds failed or empty",
            len(failures), len(outlets),
        )


@dlt.resource(name="articles", primary_key="url", write_disposition="merge")
def rss_articles() -> Iterator[dict[str, Any]]:
    """dlt resource wrapper. Used when this source is run standalone."""
    yield from iter_rss_articles()


@dlt.source(name="rss")
def rss_source() -> Any:
    """RSS source. See sources/gdelt.py for the equivalent API pattern."""
    yield rss_articles()
