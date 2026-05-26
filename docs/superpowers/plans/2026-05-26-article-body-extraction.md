# Article Body Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the heuristic article-body extractor with trafilatura (browser UA, bytes input, skip-list for known-blocked URLs), wire body fetch into `sources/gnews.py`, default it on for the producer, and ship a one-shot backfill script for the ~250k rows already in ClickHouse.

**Architecture:** Body fetch lives in `sources/rss.py` so the producer and the backfill use one implementation. The backfill is a Lambda-style pass that reads from ClickHouse, fetches in parallel, and writes back via dlt with `merge` on `url` so only the `body` column is updated.

**Tech Stack:** Python 3.13, `trafilatura` (new), `requests`, `confluent-kafka`, `dlt[clickhouse]`, ClickHouse HTTP interface, `pytest`.

Spec: `docs/superpowers/specs/2026-05-26-article-body-extraction-design.md`.

## Files touched

- Modify: `requirements.txt` — add `trafilatura>=2.0`.
- Modify: `sources/rss.py` — replace `_extract_body`, swap UA to browser, switch to bytes, add `_should_skip` helper, route skip-list through `iter_rss_articles`.
- Modify: `sources/gnews.py` — wire a body pool that reuses `sources.rss._fetch_body` + skip-list.
- Modify: `scripts/dev_stack.sh` — default `RSS_FETCH_BODY=1` when launching the producer.
- Create: `pipelines/backfill_article_bodies.py` — new CLI script.
- Create: `tests/fixtures/article_body_sample.html` — synthetic article fixture.
- Create: `tests/test_rss.py` — unit tests for `_extract_body` and `_should_skip`.

---

## Task 1: Add trafilatura dependency

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add trafilatura to requirements.txt**

Append the new line to `requirements.txt`:

```
trafilatura>=2.0
```

Final file content:

```
beautifulsoup4>=4.12.3
clickhouse-connect>=0.7
confluent-kafka>=2.5.0
dlt[clickhouse,duckdb]>=1.26.0
feedparser>=6.0.11
lxml>=5.2.0
pandas>=2
pyyaml>=6.0.2
requests>=2.32.0
streamlit>=1.36
streamlit-autorefresh>=1.0
pytest>=8
trafilatura>=2.0
```

- [ ] **Step 2: Install into the venv**

Run: `uv pip install -r requirements.txt`
Expected: `trafilatura==2.0.0` plus its transitive deps (`htmldate`, `justext`, `selectolax`, `lxml-html-clean`, `dateparser`, `tld`, `regex`) are installed without errors.

- [ ] **Step 3: Confirm import works**

Run: `uv run python -c "import trafilatura; print(trafilatura.__version__)"`
Expected: prints `2.0.0` (or newer 2.x).

- [ ] **Step 4: Commit**

```bash
git add requirements.txt
git commit -m "Add trafilatura to requirements for article body extraction"
```

---

## Task 2: Add HTML fixture for body-extraction tests

**Files:**
- Create: `tests/fixtures/article_body_sample.html`

- [ ] **Step 1: Create the fixture**

Write this exact content to `tests/fixtures/article_body_sample.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Test Article</title>
</head>
<body>
  <header><nav>Home | About | Contact</nav></header>
  <main>
    <article>
      <h1>Spaghetti Carbonara Recipe</h1>
      <p>Carbonara is a Roman pasta dish made with eggs, hard cheese, cured pork, and pepper. It is one of the most internationally famous Italian dishes.</p>
      <p>The dish takes about twenty minutes to prepare and serves four people generously. Most cooks use a wooden spoon to combine the eggs and cheese without scrambling.</p>
      <p>Use guanciale for authentic flavor, though pancetta is an acceptable substitute. Grated pecorino romano gives the sauce its characteristic sharpness.</p>
    </article>
  </main>
  <footer>Copyright 2026 Test Site</footer>
</body>
</html>
```

- [ ] **Step 2: Commit**

```bash
git add tests/fixtures/article_body_sample.html
git commit -m "Add synthetic article HTML fixture for body extraction tests"
```

---

## Task 3: Write failing tests for `_extract_body` and `_should_skip`

**Files:**
- Create: `tests/test_rss.py`

- [ ] **Step 1: Create the test file**

Write this exact content to `tests/test_rss.py`:

```python
"""Unit tests for sources/rss.py body-extraction helpers.

Layout matches tests/README.md (one file per source module).
"""

from pathlib import Path

from sources.rss import _extract_body, _should_skip

FIXTURES = Path(__file__).parent / "fixtures"


def test_extract_body_returns_main_text():
    """Trafilatura strips header/nav/footer and keeps the article paragraphs."""
    html = (FIXTURES / "article_body_sample.html").read_bytes()
    body = _extract_body(html)
    assert body is not None
    assert "Carbonara is a Roman pasta dish" in body
    assert "guanciale" in body
    # nav + footer must not bleed into the body
    assert "Home | About | Contact" not in body
    assert "Copyright 2026 Test Site" not in body


def test_extract_body_returns_none_on_empty():
    """Empty / non-article HTML yields None so the producer can skip it."""
    assert _extract_body(b"") is None
    assert _extract_body(b"<html><body></body></html>") is None


def test_should_skip_google_news_redirect():
    """Google News redirect URLs are skipped (IP throttle, see spec)."""
    assert _should_skip("https://news.google.com/rss/articles/CBMiABC?oc=5") is True


def test_should_skip_reddit():
    """Reddit needs the API, not HTML scraping."""
    assert _should_skip("https://www.reddit.com/r/news/comments/abc/title/") is True
    assert _should_skip("https://reddit.com/r/news/comments/abc/title/") is True


def test_should_skip_keeps_publisher():
    """Regular publisher URLs are not skipped."""
    assert _should_skip("https://www.spiegel.de/politik/article-a-1234.html") is False
    assert _should_skip("https://www.repubblica.it/economia/2026/05/25/news/x.html") is False
```

- [ ] **Step 2: Run the new tests and confirm they fail**

Run: `uv run pytest tests/test_rss.py -v`
Expected: 5 failures. `_extract_body` exists but signature changed (current version takes `str` not `bytes`) and `_should_skip` does not exist at all → ImportError.

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/test_rss.py
git commit -m "Add failing tests for trafilatura-based body extraction + skip-list"
```

---

## Task 4: Rewrite `_extract_body` to use trafilatura on bytes

**Files:**
- Modify: `sources/rss.py` (lines 22-23, 90-103)

- [ ] **Step 1: Add the trafilatura import**

Edit `sources/rss.py`. Change the existing imports block at the top of the file by adding `trafilatura` (alphabetical order between `requests` and `yaml`).

Find this block (currently around lines 17-23):

```python
import dlt
import feedparser
import requests
import yaml
from bs4 import BeautifulSoup
from dlt.common.pendulum import pendulum
```

Replace with:

```python
import dlt
import feedparser
import requests
import trafilatura
import yaml
from dlt.common.pendulum import pendulum
```

(The `BeautifulSoup` import is no longer needed in this file; the heuristic extractor that used it goes away in step 2.)

- [ ] **Step 2: Replace `_extract_body`**

Find the existing `_extract_body` function (currently around lines 90-103):

```python
def _extract_body(html: str) -> str | None:
    """Heuristic main-text extraction without an external dep. Prefer
    <article>, fall back to all <p>. Strip script/style first. Cap at
    BODY_MAX_CHARS so a giant page cannot inflate one Kafka message."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside"]):
        tag.decompose()
    article = soup.find("article")
    container = article if article else soup
    paras = [p.get_text(" ", strip=True) for p in container.find_all("p")]
    text = "\n\n".join(p for p in paras if p)
    if not text:
        return None
    return text[:BODY_MAX_CHARS]
```

Replace with:

```python
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
```

- [ ] **Step 3: Run the two `_extract_body` tests and confirm they pass**

Run: `uv run pytest tests/test_rss.py::test_extract_body_returns_main_text tests/test_rss.py::test_extract_body_returns_none_on_empty -v`
Expected: 2 passed.

- [ ] **Step 4: Commit**

```bash
git add sources/rss.py
git commit -m "Replace heuristic body extractor with trafilatura on bytes"
```

---

## Task 5: Switch `_fetch_body` to browser UA + bytes, add `_should_skip`

**Files:**
- Modify: `sources/rss.py` (around lines 36-45, 106-120)

- [ ] **Step 1: Add the browser UA constant and skip-list**

Edit `sources/rss.py`. Find the existing UA constant (currently around lines 38-39):

```python
# Some publishers 403 the default feedparser UA. Identify ourselves as
# a real-looking bot so MM/KZ outlets behind light WAFs let us through.
USER_AGENT = "Mozilla/5.0 (compatible; NewsIntelBot/0.1)"
```

Replace with:

```python
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


def _should_skip(url: str) -> bool:
    """True if we know this URL will not yield a body via plain HTTP GET."""
    return url.startswith(_SKIP_HOST_PREFIXES)
```

- [ ] **Step 2: Update `_fetch_body` to use the browser UA and return via the new bytes signature**

Find the existing `_fetch_body` (currently around lines 106-120):

```python
def _fetch_body(url: str) -> str | None:
    """GET article URL and run _extract_body. Returns None on any
    network/HTTP error so a single dead page does not block the sweep."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=BODY_TIMEOUT_S,
            allow_redirects=True,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        _log.debug("body: %s -> %s", url, exc)
        return None
    return _extract_body(resp.text)
```

Replace with:

```python
def _fetch_body(url: str) -> str | None:
    """GET article URL with a browser UA and run _extract_body on the
    raw bytes. Returns None on any network/HTTP error so a single dead
    page does not block the sweep. Caller is expected to short-circuit
    via _should_skip for known-non-extractable hosts."""
    if _should_skip(url):
        return None
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
```

- [ ] **Step 3: Run all `tests/test_rss.py` and confirm green**

Run: `uv run pytest tests/test_rss.py -v`
Expected: 5 passed.

- [ ] **Step 4: Run the full suite to check nothing else regressed**

Run: `uv run pytest -q`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add sources/rss.py
git commit -m "Browser UA + skip-list for article body fetch"
```

---

## Task 6: Short-circuit the body pool on skip-listed URLs in `iter_rss_articles`

**Files:**
- Modify: `sources/rss.py` (around lines 181-196 inside `iter_rss_articles`)

- [ ] **Step 1: Skip the pool submission for skip-listed URLs**

Edit `sources/rss.py`. Find the body-pool block inside `iter_rss_articles` (currently around lines 181-196):

```python
                if body_pool is not None and rows:
                    body_futures = {
                        body_pool.submit(_fetch_body, r["url"]): i
                        for i, r in enumerate(rows)
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
                        "rss: %s -> %d entries, %d bodies", label, len(rows), filled
                    )
```

Replace with:

```python
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
```

(Most curated RSS rows are publishers, so the skip count is usually 0; the log line reports it anyway for visibility.)

- [ ] **Step 2: Run the full suite**

Run: `uv run pytest -q`
Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add sources/rss.py
git commit -m "Skip body-pool submissions for skip-listed URLs in RSS sweep"
```

---

## Task 7: Add inline body pool to `sources/gnews.py`

**Files:**
- Modify: `sources/gnews.py` (imports near top, `iter_gnews_articles` body-build loop)

- [ ] **Step 1: Add the imports**

Edit `sources/gnews.py`. Find the existing imports block (currently around lines 27-36):

```python
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
```

After the last `from dlt.common.pendulum import pendulum` line, add a new import block for the rss body helpers:

```python
from sources.rss import (
    BODY_WORKERS,
    FETCH_BODY,
    _fetch_body,
    _should_skip,
)
```

(Reusing `FETCH_BODY` and `BODY_WORKERS` means one env knob controls both source modules.)

- [ ] **Step 2: Wire the body pool into `iter_gnews_articles`**

Find the place in `iter_gnews_articles` where each feed's rows are yielded (the `for row in rows: yield row` loop, typically right after rows are built from feed entries).

The current rows-yield section looks roughly like (search for `yield from rows` or `for row in rows: yield row` — exact line number depends on current source state):

```python
                for row in rows:
                    yield row
```

Replace that section with a body-fetch block followed by the yield, mirroring `sources/rss.py`:

```python
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
                        "gnews: %s -> %d entries, %d bodies (%d skipped)",
                        label, len(rows), filled,
                        sum(1 for r in rows if _should_skip(r["url"])),
                    )
                for row in rows:
                    yield row
```

Note: `label` should already exist in the surrounding scope as the per-feed log label; if it does not, derive it from the loop variable (e.g. `f"{country}/{topic_or_query}"`). Use the same label string the existing `_log.info("gnews: %s ...")` lines use.

- [ ] **Step 3: Create the body pool in the outer scope of `iter_gnews_articles`**

Find the start of `iter_gnews_articles` (the `def iter_gnews_articles()` line) and the top of its body. Mirror what `sources/rss.py` does at lines 144-148: create a `body_pool` if `FETCH_BODY`, wrap the whole feed loop in a `try` block with `body_pool.shutdown(wait=True)` in `finally`.

Concretely, just after the existing logging at the top of the function (`_log.info("gnews: %d feeds ...")` or similar), add:

```python
    body_pool = (
        ThreadPoolExecutor(max_workers=BODY_WORKERS, thread_name_prefix="gnews-body")
        if FETCH_BODY
        else None
    )
    try:
```

And right before the final summary log line (`_log.info("gnews: done. ...")` or equivalent), close the try/finally:

```python
    finally:
        if body_pool is not None:
            body_pool.shutdown(wait=True)
```

(Pattern identical to `sources/rss.py` lines 144-207.)

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest -q`
Expected: all tests pass. (No new tests required for this task — the wiring is a no-op when `FETCH_BODY=0`, and every gnews URL is skip-listed when `FETCH_BODY=1`, so the body pool fires zero requests.)

- [ ] **Step 5: Smoke-check importability**

Run: `PYTHONPATH=. uv run python -c "from sources.gnews import iter_gnews_articles; print('ok')"`
Expected: prints `ok`. Catches any circular-import problems between `sources.rss` and `sources.gnews`.

- [ ] **Step 6: Commit**

```bash
git add sources/gnews.py
git commit -m "Wire inline body pool into Google News source (uses rss._fetch_body)"
```

---

## Task 8: Default `RSS_FETCH_BODY=1` in `scripts/dev_stack.sh`

**Files:**
- Modify: `scripts/dev_stack.sh` (the producer-launch line)

- [ ] **Step 1: Locate the producer command**

Open `scripts/dev_stack.sh` and find the line that launches the producer. It will look like one of:

```bash
PYTHONPATH=. uv run python pipelines/kafka/producer_rss.py
```

or with other env prefixed:

```bash
PYTHONPATH=. PRODUCER_INTERVAL_S=300 uv run python pipelines/kafka/producer_rss.py
```

- [ ] **Step 2: Add `RSS_FETCH_BODY=1` to that line**

Edit so that `RSS_FETCH_BODY=1` is in the env prefix. Example:

```bash
PYTHONPATH=. RSS_FETCH_BODY=1 PRODUCER_INTERVAL_S=300 uv run python pipelines/kafka/producer_rss.py
```

If there are multiple producer-launch sites (background + foreground), update both.

- [ ] **Step 3: Re-run the help/version sanity-check**

Run: `bash -n scripts/dev_stack.sh`
Expected: no syntax error.

- [ ] **Step 4: Commit**

```bash
git add scripts/dev_stack.sh
git commit -m "Default RSS_FETCH_BODY=1 in dev_stack so producer fetches bodies"
```

---

## Task 9: Backfill script skeleton (ClickHouse read + CLI, no writes yet)

**Files:**
- Create: `pipelines/backfill_article_bodies.py`

- [ ] **Step 1: Create the script with read-only structure**

Write this exact content to `pipelines/backfill_article_bodies.py`:

```python
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


def select_missing_body(
    limit: int, country: str | None, source: str | None
) -> list[dict]:
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
    # TODO: fetch + dlt write in next task.
    log.info("dry skeleton: skipping fetch/write")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Smoke-run the read path**

Bring up the stack if it is not already running, then:

Run: `PYTHONPATH=. uv run python pipelines/backfill_article_bodies.py --limit 5 --dry-run`
Expected: logs `starting backfill limit=5 ...` and `selected 5 rows to attempt`. Exits cleanly.

If the count is `0` it means either ClickHouse is empty or every row already has a body — verify with:

```bash
curl -s -u news:news 'http://localhost:8123/?database=news' --data-binary 'SELECT count() FROM news___articles FORMAT TSV'
```

- [ ] **Step 3: Commit**

```bash
git add pipelines/backfill_article_bodies.py
git commit -m "Add backfill script skeleton: ClickHouse read + CLI flags"
```

---

## Task 10: Fetch bodies in parallel and write back via dlt

**Files:**
- Modify: `pipelines/backfill_article_bodies.py` (the `main()` function and add a helper)

- [ ] **Step 1: Replace the TODO block with the fetch + write logic**

In `pipelines/backfill_article_bodies.py`, replace this block from Task 9:

```python
    rows = select_missing_body(args.limit, args.country, args.source)
    log.info("selected %d rows to attempt", len(rows))
    if not rows:
        return 0
    # TODO: fetch + dlt write in next task.
    log.info("dry skeleton: skipping fetch/write")
    return 0
```

With this complete fetch + write block:

```python
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
```

- [ ] **Step 2: Dry-run on 20 rows to validate the fetch loop without writing**

Run: `PYTHONPATH=. uv run python pipelines/backfill_article_bodies.py --limit 20 --workers 8 --dry-run`
Expected: log lines for `progress` (if 100+ rows would be needed; for 20 rows you see the `fetch done` summary directly), `fetch done: N/20 filled`, and `--dry-run: skipping dlt write`. N is typically ~10-13 (~55% rate per the smoke test).

- [ ] **Step 3: Real write on 10 rows to verify the dlt path**

Run: `PYTHONPATH=. uv run python pipelines/backfill_article_bodies.py --limit 10 --workers 8`
Expected: `dlt write done. merged X rows into news.articles` where X is the fill count.

- [ ] **Step 4: Verify the column now exists and is populated**

Run:

```bash
curl -s -u news:news 'http://localhost:8123/?database=news' --data-binary 'SELECT count() AS total, countIf(body IS NOT NULL AND body != '"'"''"'"') AS with_body FROM news___articles FORMAT TSV'
```

Expected: `total` matches your row count and `with_body` is at least the fill count from the previous step.

- [ ] **Step 5: Spot-check one row**

Run:

```bash
curl -s -u news:news 'http://localhost:8123/?database=news' --data-binary "SELECT url, substring(body, 1, 200) FROM news___articles WHERE body IS NOT NULL AND body != '' LIMIT 1 FORMAT Vertical"
```

Expected: prints one URL and the first 200 chars of its extracted body.

- [ ] **Step 6: Commit**

```bash
git add pipelines/backfill_article_bodies.py
git commit -m "Backfill: fetch bodies in parallel and merge via dlt"
```

---

## Task 11: Sample-scale backfill and quality verification

**Files:** none modified (verification only — but if numbers look off, this task may surface a follow-up fix).

- [ ] **Step 1: Run a 1000-row backfill**

Run: `PYTHONPATH=. uv run python pipelines/backfill_article_bodies.py --limit 1000 --workers 16`
Expected: `fetch done: N/1000 filled`, N roughly in 500-700. Run completes in a few minutes (bounded by `BODY_TIMEOUT_S=15` × slowest hosts).

- [ ] **Step 2: Per-country breakdown**

Run:

```bash
curl -s -u news:news 'http://localhost:8123/?database=news' --data-binary "
SELECT country_target,
       count() AS total,
       countIf(body IS NOT NULL AND body != '') AS with_body,
       round(100 * with_body / total, 1) AS pct,
       round(avg(length(body)) FILTER (WHERE body IS NOT NULL)) AS avg_len
FROM news___articles
GROUP BY country_target
ORDER BY country_target
FORMAT PrettyCompact"
```

Expected: each country shows non-zero `with_body` after enough backfill rounds. `avg_len` typically 1500-5000 chars.

- [ ] **Step 3: Eyeball a handful of bodies per country**

Run:

```bash
curl -s -u news:news 'http://localhost:8123/?database=news' --data-binary "
SELECT country_target, source, substring(body, 1, 250) AS head
FROM news___articles
WHERE body IS NOT NULL AND body != ''
ORDER BY rand()
LIMIT 10 BY country_target
FORMAT Vertical"
```

Expected: human-readable article openings, not nav menus or paywall stubs. If too much boilerplate slips through, that's a follow-up tuning task on the trafilatura options (out of scope for this plan).

- [ ] **Step 4: Decide whether to run unbounded**

If numbers look healthy, run unbounded:

Run: `PYTHONPATH=. uv run python pipelines/backfill_article_bodies.py --limit 1000000 --workers 16`
Expected: many `progress: ...` log lines, then a `fetch done` summary, then a `dlt write done` summary. With ~250k extractable rows at ~16 workers and ~15s per fetch worst case, this is multi-hour in the worst case; in practice closer to 1-2 hours.

(If desired, leave the unbounded run for a separate session — this plan's MVP completes at the end of step 3.)

- [ ] **Step 5: No code commit unless something needed fixing**

If a fix was needed during verification, commit it as a follow-up under a clear message. Otherwise this task does not commit.

---

## Self-review

Spec coverage check:

- Inline body fetch swap → Tasks 4, 5 (extractor + UA + bytes), 6 (skip-list short-circuit in rss), 7 (gnews wiring), 8 (default-on).
- Backfill → Tasks 9, 10, 11.
- Skip-list (gnews + reddit) → Task 5 (helper), Tasks 6, 7 (used in source modules), Task 9 (inlined in SQL).
- trafilatura dep → Task 1.
- Tests → Tasks 2, 3, 4, 5.
- Files-touched list matches Tasks 1-10.
- "No `body_fetched_at`" non-goal honored (no such field anywhere).
- "No gnews resolve" non-goal honored (gnews URLs go straight to skip-list).
- "No Reddit body" non-goal honored (reddit goes to skip-list).
- Rollout step 1 in spec ("smoke test should still pass and improve") is verified informally by the test suite + the smoke comparator script remaining green; the comparator itself is not re-run automatically but the engineer can re-run it manually with the existing `scripts/sample_body_extraction.py` to confirm the M3 column rises.

Placeholder scan: no `TBD`, `TODO`, `implement later`, or unspecified error handling. Task 9's main has an explicit `TODO` comment that is removed in Task 10 (intentional, marked).

Type consistency: `_extract_body(bytes) -> str | None`, `_fetch_body(str) -> str | None`, `_should_skip(str) -> bool`. All call sites match.

No gaps found. Plan is ready.
