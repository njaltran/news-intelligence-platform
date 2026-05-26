# Article Body Extraction: Backfill + Inline Going Forward

Status: draft. Author: Jack. Date: 2026-05-26.

## Context

ClickHouse holds ~250k articles from the streaming arm (`pipelines/kafka/producer_rss.py` → `consumer_to_clickhouse.py`). The schema currently has `source`, `country_target`, `title`, `summary`, `url`, `published_at`, `extracted_at` and no `body` column. Downstream NLP (embeddings, BERTopic, narrative-divergence metrics) needs the article body, not just the RSS summary.

`sources/rss.py` already has an opt-in inline body fetcher (`RSS_FETCH_BODY=1`) but defaults off, and it uses a heuristic `<article>` / `<p>` extractor plus a "NewsIntelBot" UA. `sources/gnews.py` has no body fetcher at all.

EA framing: this raises [[Volume]] per row by 10–50x and increases [[Veracity]] (clean body text replaces noisy RSS summary snippets). It does not change the architecture (still [[Kappa]] for the streaming arm, [[Lambda]] for batch). The backfill itself is a one-off [[Lambda]] pass over the lake-in-ClickHouse.

## Smoke test (already done)

`scripts/sample_body_extraction.py` compared four extraction methods on a 35-URL sample drawn from ClickHouse (mixed publishers + Google News). Results across two runs:

| Method | Extracted |
|---|---|
| bot UA + heuristic `<article>`/`<p>` (status quo) | 15–20 / 35 |
| browser UA + heuristic | 15–19 / 35 |
| browser UA + trafilatura on bytes | **18–21 / 35** |
| Google News URL resolve → trafilatura | 0 / 10 (HTTP 429 `/sorry/index` captcha) |

Browser UA alone barely moves the needle (real blockers check more than UA). Trafilatura gives a small extraction lift and a large quality lift: it cuts paywall boilerplate (Spiegel iTunes pitch), nav menus (Caravan KZ), QR-code app pitches (CNN video pages), and unlocks a few publisher pages the heuristic missed (Il Sole 24 Ore).

One bug surfaced and is fixed in the smoke script: `requests.text` defaults to ISO-8859-1 when no charset header is set, producing mojibake on NUR.KZ-style sites. Solution is to pass raw `response.content` (bytes) into trafilatura / BeautifulSoup, which sniff `<meta charset>` correctly.

Hard-blocked outlets (no fix from UA + extractor swap): NYT, Politico, The Hill, Reddit (any subreddit), Khit Thit Media, Informburo KZ, several Burmese-language paywalled sites. These stay body-less for the PoC. Google News redirect URLs also stay body-less (IP throttle).

## Goals

1. Inline body fetch is the default for new rows hitting the producer.
2. Backfill body for the ~250k rows already in ClickHouse.
3. Both paths use the same extraction code so coverage and noise are predictable.
4. Realistic target: ~55–65% of non-Google-News, non-Reddit publisher rows get a body.

## Non-goals

- Resolving Google News redirect URLs to publisher URLs. The decoder works but Google rate-limits our IP. Out of scope until we have a proxy story.
- Reddit body fetch. Needs the Reddit API; out of scope.
- Paywall bypass.
- Per-URL attempt tracking (`body_fetched_at`, retry counter). YAGNI for the PoC. A rerun of the backfill simply retries everything still missing a body.
- Headless browser. Heuristic + trafilatura covers enough.

## Design

### 1. Swap the extractor in `sources/rss.py`

Replace the heuristic `_extract_body` and the UA in `_fetch_body`:

```python
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

def _extract_body(html: bytes) -> str | None:
    text = trafilatura.extract(
        html, include_comments=False, include_tables=False, favor_recall=False
    )
    if not text:
        return None
    return text[:BODY_MAX_CHARS]

def _fetch_body(url: str) -> str | None:
    try:
        resp = requests.get(
            url, headers={"User-Agent": BROWSER_UA},
            timeout=BODY_TIMEOUT_S, allow_redirects=True,
        )
        resp.raise_for_status()
    except requests.RequestException:
        return None
    return _extract_body(resp.content)
```

The shared `USER_AGENT` in `sources/scrapers/_base.py` stays as-is (the BS4 scrapers have their own UA policy). The RSS body fetcher gets its own browser UA so the scraper UA discipline doesn't change.

Skip-list for URLs we know don't extract: a small `_should_skip(url)` helper returns True for `news.google.com/*` and `*.reddit.com/*`. Saves wasted requests during the inline pass.

### 2. Add inline body fetch to `sources/gnews.py`

Mirror what `sources/rss.py` already does. After parsing each feed into `rows`, fan rows through a body pool when `RSS_FETCH_BODY=1` (reuse the same env flag so there is one knob, not two). The `_fetch_body` import comes from `sources.rss` to keep one implementation.

Note: every gnews row's URL is a `news.google.com/rss/articles/CBM...` redirect, so the skip-list will skip them all. The body pool is effectively a no-op for gnews today; we wire it in so that if/when the redirect-resolve story lands, no further plumbing is needed.

### 3. Make inline body fetch the default

In `scripts/dev_stack.sh`, set `RSS_FETCH_BODY=1` when launching the producer. No code change to `sources/rss.py` defaults; we keep the env knob so an operator can turn it off for a fast bulk sweep.

### 4. Backfill script `pipelines/backfill_article_bodies.py`

Reads from ClickHouse via the HTTP interface, fetches bodies in parallel, writes rows back through dlt with `write_disposition="merge"` keyed on `url`. dlt then updates only the changed columns (`body`) and the consumer's table picks up the new column the first time the producer emits one (or this backfill emits the first one).

Pseudo-flow:

```python
def main(limit, workers, country, source, dry_run):
    if not body_column_exists():
        log.info("body column does not exist yet, will be created by first merge")
    rows = ch_select_missing_body(limit, country, source)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_body, r["url"]): r for r in rows}
        filled = []
        for fut in as_completed(futures):
            row = futures[fut]
            body = fut.result()
            if body:
                filled.append({**row, "body": body})
    log.info("filled %d / %d", len(filled), len(rows))
    if dry_run or not filled:
        return
    pipeline = dlt.pipeline(
        pipeline_name="backfill_article_bodies",
        destination="clickhouse",
        dataset_name="news",
    )
    pipeline.run(
        dlt.resource(filled, name="articles", primary_key="url",
                     write_disposition="merge")
    )
```

ClickHouse SELECT, with the skip-list inlined so we don't waste a fetch:

```sql
SELECT url, source, country_target, title, summary, published_at, extracted_at
FROM news___articles
WHERE (body IS NULL OR body = '')
  AND url NOT LIKE 'https://news.google.com/%'
  AND url NOT LIKE 'https://www.reddit.com/%'
  AND url NOT LIKE 'https://reddit.com/%'
ORDER BY rand()
LIMIT {limit:UInt32}
FORMAT JSONEachRow
```

(`rand()` so a small `--limit` run still gets a representative cross-section.)

CLI flags:

- `--limit N` (default 1000)
- `--workers N` (default 16)
- `--country DE` (filter)
- `--source 'Spiegel%'` (LIKE filter)
- `--dry-run` (fetch + log but skip the dlt write)

Logging: progress every 100 rows, summary at end (filled / total / by-country breakdown), mirrors the consumer log style.

Body-column-doesn't-exist case: first run, `body` is not in `news___articles` yet. Two safe paths:

- Query `system.columns` for the column. If absent, drop the `body IS NULL OR body = ''` predicate (everything qualifies).
- Or `ALTER TABLE ... ADD COLUMN IF NOT EXISTS body Nullable(String)` before the SELECT.

We go with the `system.columns` probe (no DDL from the script).

### 5. Add `trafilatura` to `requirements.txt`

Single line. The library and its transitive deps (`htmldate`, `justext`, `lxml-html-clean`, `selectolax`, `dateparser`, `tld`, `regex`) are already in the venv from the smoke test.

### 6. Tests

Two thin pytest cases:

- `tests/sources/test_rss_body.py::test_extract_body_uses_trafilatura` — feed a small saved HTML fixture (one of the publishers that worked: e.g. NTV or Repubblica) into `_extract_body`, assert the returned string contains an expected sentence and excludes nav-menu text.
- `tests/sources/test_rss_body.py::test_extract_body_returns_none_on_empty` — empty HTML returns None.

The existing News Eleven HTML fixture (`tests/sources/scrapers/fixtures/`) is not reused; we add one small fixture per RSS-publisher test.

## Files touched

- `requirements.txt` — add `trafilatura>=2.0`.
- `sources/rss.py` — swap extractor + UA, switch to bytes, add skip-list helper.
- `sources/gnews.py` — wire body pool (using `sources.rss._fetch_body`).
- `scripts/dev_stack.sh` — default `RSS_FETCH_BODY=1`.
- `pipelines/backfill_article_bodies.py` — new script.
- `tests/sources/test_rss_body.py` — new test file + small fixture.

## Rollout

1. Land the extractor swap + pytest cases. Re-run `scripts/sample_body_extraction.py` (kept in-tree as a one-off comparator) and confirm the M3 column rises by the +1–3 absolute we saw in smoke.
2. Default the env knob on. Producer/consumer pick up bodies on the next sweep.
3. Run the backfill with `--limit 1000 --dry-run` first to eyeball numbers, then unbounded.
4. Verify in ClickHouse: row count where `body` is populated, per-country breakdown, average char length.

## Open questions

- Do we want `body_fetched_at` after all, to avoid re-fetching the same dead URLs on every backfill rerun? Decision: no for v0. If reruns become routine, add it then.
- Should the consumer write directly to a body column instead of having the producer fetch? Decision: no — body fetch belongs upstream of Kafka so messages are self-contained and the consumer stays a thin sink.
- Cap of 20k chars: still right? Decision: keep. Long-form features get truncated but downstream embedding models cap input anyway.
