"""Clean the raw RSS + scraper tables into a single interim table.

Reads from rss.duckdb (`rss_raw_*.articles`) and scrapers.duckdb
(`scrapers_raw_*.articles`), applies the cleaning steps below, and
writes the unified result to `interim.duckdb` / `articles_clean`.

Cleaning steps:
  1. URL canonicalisation. Lowercase host, drop trailing slash,
     drop tracking query parameters (utm_*, fbclid, gclid, ref,
     source). Rows with no parseable URL are dropped.
  2. HTML strip on `summary`. RSS feeds (Myanmar Now, Astana Times,
     others) embed <p>, <img>, <a> in the description. BeautifulSoup
     get_text + html.unescape collapses them to readable text.
  3. Drop stub headlines (NYT World live-blog placeholders like
     "Here's the latest.").
  4. Cross-pipeline dedup on the canonical URL.

Idempotent. CREATE OR REPLACE rebuilds `articles_clean` on every run.

EA framing. This is the boundary between the raw lake and the
analytical layer (Watson 2014's "data warehouse" generation). The raw
tables stay untouched (Veracity preserved, replayable); the cleaned
table is the contract for downstream embeddings, topic modelling, and
the dashboard.
"""

from __future__ import annotations

import html
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import duckdb
from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RSS_DB = REPO_ROOT / "rss.duckdb"
DEFAULT_SCRAPERS_DB = REPO_ROOT / "scrapers.duckdb"
DEFAULT_OUT_DB = REPO_ROOT / "interim.duckdb"

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source", "spm",
}
# Prefix matches. Catches BBC's at_medium / at_campaign / at_link_origin (all
# their internal analytics) plus future variants.
TRACKING_PREFIXES = ("utm_", "at_")

# NYT World re-publishes the same live-blog stub multiple times a day.
# Curly + straight apostrophe variants both seen in the data.
STUB_TITLES = {"Here's the latest.", "Here’s the latest."}


def canonicalise_url(url: str | None) -> str | None:
    """Drop tracking params, lowercase host, normalise trailing slash.
    Returns None for unparseable input."""
    if not url:
        return None
    parts = urlparse(url.strip())
    if not parts.scheme or not parts.netloc:
        return None
    netloc = parts.netloc.lower()
    path = parts.path.rstrip("/") or "/"
    kept = [
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=False)
        if k.lower() not in TRACKING_PARAMS
        and not any(k.lower().startswith(p) for p in TRACKING_PREFIXES)
    ]
    query = urlencode(kept)
    return urlunparse((parts.scheme, netloc, path, "", query, ""))


def strip_html(value: str | None) -> str | None:
    """BeautifulSoup get_text + entity unescape. None in, None out."""
    if not value:
        return None
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    text = html.unescape(text).strip()
    return text or None


def _latest_dataset(con: duckdb.DuckDBPyConnection, prefix: str) -> str:
    row = con.execute(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name LIKE ? AND schema_name NOT LIKE '%_staging' "
        "ORDER BY schema_name DESC LIMIT 1",
        [f"{prefix}%"],
    ).fetchone()
    if not row:
        raise RuntimeError(f"No dataset matching {prefix}% in attached database")
    return row[0]


def _read_raw(path: Path, schema_prefix: str) -> list[tuple]:
    """SELECT a stable column projection from the latest schema in a
    raw DB. Tolerates a missing `summary` column (scraper tables may
    drop it when all rows are null)."""
    con = duckdb.connect(str(path), read_only=True)
    schema = _latest_dataset(con, schema_prefix)
    cols = [
        row[0] for row in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = 'articles'"
        ).fetchall()
    ]
    summary_select = "summary" if "summary" in cols else "NULL AS summary"
    rows = con.execute(
        f"SELECT source, country_target, title, {summary_select}, url, "
        f"published_at, extracted_at FROM {schema}.articles"
    ).fetchall()
    con.close()
    return rows


def run(
    rss_db: Path = DEFAULT_RSS_DB,
    scrapers_db: Path = DEFAULT_SCRAPERS_DB,
    out_db: Path = DEFAULT_OUT_DB,
) -> dict[str, int]:
    """Clean and merge raw → articles_clean. Returns counts dict."""
    raw_rows = _read_raw(rss_db, "rss_raw") + _read_raw(scrapers_db, "scrapers_raw")

    cleaned: list[tuple] = []
    seen_urls: set[str] = set()
    dropped_no_url = dropped_stub = dropped_dupe = 0

    for source, country, title, summary, url, pub_at, ext_at in raw_rows:
        canonical = canonicalise_url(url)
        if not canonical:
            dropped_no_url += 1
            continue
        title_clean = (title or "").strip() or None
        if title_clean in STUB_TITLES:
            dropped_stub += 1
            continue
        if canonical in seen_urls:
            dropped_dupe += 1
            continue
        seen_urls.add(canonical)
        cleaned.append((
            source,
            country,
            title_clean,
            strip_html(summary),
            canonical,
            pub_at,
            ext_at,
        ))

    out = duckdb.connect(str(out_db))
    out.execute("""
        CREATE OR REPLACE TABLE articles_clean (
            source         VARCHAR,
            country_target VARCHAR,
            title          VARCHAR,
            summary        VARCHAR,
            url            VARCHAR PRIMARY KEY,
            published_at   TIMESTAMP WITH TIME ZONE,
            extracted_at   TIMESTAMP WITH TIME ZONE
        )
    """)
    out.executemany(
        "INSERT INTO articles_clean VALUES (?, ?, ?, ?, ?, ?, ?)",
        cleaned,
    )
    out.close()

    return {
        "raw_in": len(raw_rows),
        "clean_out": len(cleaned),
        "dropped_no_url": dropped_no_url,
        "dropped_stub": dropped_stub,
        "dropped_dupe": dropped_dupe,
    }
