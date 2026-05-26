"""One-shot backfill: enrich existing ClickHouse rows with article body
text fetched from the publisher page. Lambda-style pass over the lake.

Reads URLs from news___articles where body is missing, fetches in
parallel via sources.rss._fetch_body (trafilatura on browser-UA bytes),
and writes the rows back through dlt with `merge` disposition keyed on
url so only the `body` column is touched.

Skip-list (news.google.com, reddit) is inlined into the SQL so a small
--limit sample still draws from extractable rows. Hosts that block our
UA outright (NYT, Politico, etc.) just return None and stay bodyless.

Run from repo root:

    PYTHONPATH=. uv run python pipelines/backfill_article_bodies.py \\
        --limit 1000 --workers 16 --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import dlt
import requests

from pipelines.kafka._log import get_logger
from sources.rss import _fetch_body

CH_URL = "http://localhost:8123/"
CH_AUTH = ("news", "news")
CH_DB = "news"
TABLE = "news___articles"
DATASET = "news"
DLT_TABLE = "articles"

log = get_logger("backfill_body")


def ch_query(sql: str) -> str:
    resp = requests.post(
        CH_URL,
        params={"database": CH_DB},
        data=sql.encode("utf-8"),
        auth=CH_AUTH,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.text


def body_column_exists() -> bool:
    """Probe system.columns. First-time backfill runs before the column
    has ever been written; in that case we drop the body-null predicate."""
    sql = (
        "SELECT count() FROM system.columns "
        f"WHERE database = 'news' AND table = '{TABLE}' AND name = 'body' "
        "FORMAT TSV"
    )
    return ch_query(sql).strip() == "1"


def _safe_country(country: str | None) -> str | None:
    """ISO alpha-2 only. Reject anything that could escape the SQL literal."""
    if country is None:
        return None
    if not re.fullmatch(r"[A-Za-z]{2}", country):
        raise ValueError(f"--country must be a 2-letter ISO code, got {country!r}")
    return country.upper()


def _safe_source_like(source: str | None) -> str | None:
    """Allow letters, digits, spaces and LIKE wildcards (% and _); nothing else.
    A real source name with a quote (`O'Globo`) is rare and can be added to
    sources.yaml without it; for the backfill filter we keep the grammar strict
    so the value cannot break out of the SQL literal."""
    if source is None:
        return None
    if not re.fullmatch(r"[A-Za-z0-9 _%/\.\-]+", source):
        raise ValueError(f"--source has illegal characters: {source!r}")
    return source


def select_missing_body(
    limit: int, country: str | None, source: str | None
) -> list[dict]:
    country = _safe_country(country)
    source = _safe_source_like(source)
    where_body = (
        "(body IS NULL OR body = '')" if body_column_exists() else "1 = 1"
    )
    where_country = (
        f"AND country_target = '{country}'" if country else ""
    )
    where_source = (
        f"AND source LIKE '{source}'" if source else ""
    )
    sql = f"""
    SELECT url, source, country_target, title, summary, published_at, extracted_at
    FROM {TABLE}
    WHERE {where_body}
      AND url NOT LIKE 'https://news.google.com/%'
      AND url NOT LIKE 'https://www.reddit.com/%'
      AND url NOT LIKE 'https://reddit.com/%'
      {where_country}
      {where_source}
    ORDER BY rand()
    LIMIT {int(limit)}
    FORMAT JSONEachRow
    """
    text = ch_query(sql)
    rows: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill article bodies into ClickHouse.")
    p.add_argument("--limit", type=int, default=1000, help="rows to attempt per run")
    p.add_argument("--workers", type=int, default=16, help="parallel fetchers")
    p.add_argument("--country", default=None, help="ISO country filter (DE, IT, ...)")
    p.add_argument(
        "--source", default=None, help="source name LIKE pattern (e.g. 'Spiegel%%')"
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="fetch + log but skip the dlt write",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    log.info(
        "starting backfill limit=%d workers=%d country=%s source=%s dry_run=%s",
        args.limit,
        args.workers,
        args.country,
        args.source,
        args.dry_run,
    )
    rows = select_missing_body(args.limit, args.country, args.source)
    log.info("selected %d rows to attempt", len(rows))
    if not rows:
        return 0

    filled: list[dict] = []
    by_country_attempt: Counter[str] = Counter()
    by_country_ok: Counter[str] = Counter()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        future_to_row = {pool.submit(_fetch_body, r["url"]): r for r in rows}
        for fut in as_completed(future_to_row):
            row = future_to_row[fut]
            ck = row.get("country_target") or "?"
            by_country_attempt[ck] += 1
            try:
                body = fut.result()
            except Exception as exc:  # noqa: BLE001
                log.debug("fetch failed: %s -> %s", row["url"], exc)
                body = None
            if body:
                filled.append({**row, "body": body})
                by_country_ok[ck] += 1
            if (by_country_attempt.total() % 100) == 0:
                log.info(
                    "progress: %d/%d attempted, %d filled, by_country=%s",
                    by_country_attempt.total(),
                    len(rows),
                    len(filled),
                    dict(by_country_attempt),
                )

    log.info(
        "fetch done: %d/%d filled. by_country_ok=%s by_country_attempt=%s",
        len(filled),
        len(rows),
        dict(by_country_ok),
        dict(by_country_attempt),
    )

    if args.dry_run:
        log.info("--dry-run: skipping dlt write")
        return 0
    if not filled:
        log.info("nothing to write")
        return 0

    pipeline = dlt.pipeline(
        pipeline_name="backfill_article_bodies",
        destination="clickhouse",
        dataset_name=DATASET,
    )
    pipeline.run(
        dlt.resource(
            iter(filled),
            name=DLT_TABLE,
            primary_key="url",
            write_disposition="merge",
        )
    )
    log.info("dlt write done. merged %d rows into %s.%s", len(filled), DATASET, DLT_TABLE)
    return 0


if __name__ == "__main__":
    sys.exit(main())
