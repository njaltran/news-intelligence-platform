# Article-page Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional `parse_article` step to the BS4 `Scraper` base class so each row's `summary` (and other fields) can be filled from the article page itself. First consumer: `MizzimaBurmeseScraper`.

**Architecture:** Single sequential pass per outlet. Homepage parse stays unchanged; for each yielded URL we GET the article page, call the subclass's `parse_article(soup) -> dict`, and merge the result into the row. Homepage values win on conflict; article values only fill NULLs.

**Tech Stack:** Python 3, `requests`, `BeautifulSoup4`, `dlt`, `pytest` (newly added), `uv` for env / runs.

**Spec:** `docs/superpowers/specs/2026-05-25-article-page-backfill-design.md`

**Working directory for every command below:** `/Users/jack/code/news-intelligence-platform` (the team-project repo root). `dlt` resolves `.dlt/secrets.toml` from cwd, and `PYTHONPATH=.` is the project convention for scripts.

---

## Task 1: Install pytest

**Files:**
- Modify: `requirements.txt` (append `pytest>=8`)

- [ ] **Step 1: Add `pytest` to `requirements.txt`**

```text
pytest>=8
```

Append to the existing `requirements.txt` so the file becomes:

```text
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
```

- [ ] **Step 2: Install into the venv**

Run: `uv pip install -r requirements.txt`
Expected: `pytest-X.Y.Z` shown in the install output. No errors.

- [ ] **Step 3: Verify pytest is importable**

Run: `uv run python -c "import pytest; print(pytest.__version__)"`
Expected: a version string like `8.3.x`. No traceback.

- [ ] **Step 4: Commit**

```bash
git add requirements.txt
git commit -m "Add pytest to requirements"
```

---

## Task 2: Default `parse_article` returns `{}`

**Files:**
- Create: `tests/test_scrapers.py`
- Modify: `sources/scrapers/_base.py` (add new method on `Scraper`)

- [ ] **Step 1: Write the failing test**

Create `tests/test_scrapers.py` with:

```python
"""Unit tests for the BS4 scraper base class and outlet subclasses.

Layout matches tests/README.md (one file per source module).
"""

from bs4 import BeautifulSoup

from sources.scrapers._base import Scraper


def test_parse_article_default_returns_empty():
    """Default parse_article returns {} so subclasses can opt out of
    article-page backfill without raising."""
    scraper = Scraper()
    soup = BeautifulSoup("<html><body><p>anything</p></body></html>", "html.parser")
    assert scraper.parse_article(soup) == {}
```

- [ ] **Step 2: Run the test, expect failure**

Run: `uv run pytest tests/test_scrapers.py::test_parse_article_default_returns_empty -v`
Expected: FAIL with `AttributeError: 'Scraper' object has no attribute 'parse_article'`.

- [ ] **Step 3: Add `parse_article` to `Scraper`**

In `sources/scrapers/_base.py`, add this method to the `Scraper` class (place it directly after `parse`):

```python
    def parse_article(self, soup: BeautifulSoup) -> dict[str, Any]:
        """Parse a single article page. Override per outlet.
        Default returns {} so subclasses can opt out of article-page
        backfill."""
        return {}
```

- [ ] **Step 4: Run the test, expect pass**

Run: `uv run pytest tests/test_scrapers.py::test_parse_article_default_returns_empty -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_scrapers.py sources/scrapers/_base.py
git commit -m "Add default parse_article to Scraper base class"
```

---

## Task 3: `fetch_article` swallows network errors

**Files:**
- Modify: `tests/test_scrapers.py` (append test)
- Modify: `sources/scrapers/_base.py` (add `fetch_article` helper)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_scrapers.py`:

```python
import requests

from sources.scrapers._base import Scraper


def test_fetch_article_swallows_request_errors(monkeypatch):
    """A failed article GET must yield {} instead of bubbling up so the
    whole run is not killed by one bad URL."""

    def boom(self, url):
        raise requests.RequestException("connection reset")

    monkeypatch.setattr(Scraper, "fetch", boom)
    # Also no-op the sleep call inside fetch_article (we will not reach
    # it on this path but be defensive).
    monkeypatch.setattr("sources.scrapers._base.time.sleep", lambda *_: None)

    scraper = Scraper()
    assert scraper.fetch_article("http://example.invalid/article") == {}
```

(Note: the `import requests` line goes near the top of the file with the other imports. If pytest complains about duplicate imports, consolidate them at the top.)

- [ ] **Step 2: Run the test, expect failure**

Run: `uv run pytest tests/test_scrapers.py::test_fetch_article_swallows_request_errors -v`
Expected: FAIL with `AttributeError: 'Scraper' object has no attribute 'fetch_article'`.

- [ ] **Step 3: Add `fetch_article` to `Scraper`**

In `sources/scrapers/_base.py`, add this method to the `Scraper` class directly after `parse_article`:

```python
    def fetch_article(self, url: str) -> dict[str, Any]:
        """Fetch one article page and return parsed fields.

        On any requests.RequestException returns {}. Other exceptions
        propagate. Sleeps request_delay_s after a successful fetch.
        """
        try:
            html = self.fetch(url)
        except requests.RequestException:
            return {}
        time.sleep(self.request_delay_s)
        return self.parse_article(BeautifulSoup(html, "html.parser"))
```

`requests`, `time`, and `BeautifulSoup` are already imported at the top of `_base.py`. No new imports needed.

- [ ] **Step 4: Run the test, expect pass**

Run: `uv run pytest tests/test_scrapers.py::test_fetch_article_swallows_request_errors -v`
Expected: PASS.

- [ ] **Step 5: Run the full file to confirm both tests still pass**

Run: `uv run pytest tests/test_scrapers.py -v`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add tests/test_scrapers.py sources/scrapers/_base.py
git commit -m "Add fetch_article helper that swallows network errors"
```

---

## Task 4: `run()` merges article fields with homepage-wins precedence

**Files:**
- Modify: `tests/test_scrapers.py` (append test)
- Modify: `sources/scrapers/_base.py` (update `run` body, lines 42-59 of the current file)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_scrapers.py`:

```python
from typing import Any

from sources.scrapers._base import Scraper


class _FakeScraper(Scraper):
    """Test subclass. Homepage yields two partials, one with a title
    and one without."""

    name = "fake"
    country = "XX"
    base_url = "http://example.invalid/"
    request_delay_s = 0

    def parse(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        return [
            {"url": "http://example.invalid/a", "title": "Home A"},
            {"url": "http://example.invalid/b"},
        ]


def test_run_merges_article_fields(monkeypatch):
    """Homepage values win on conflict; article values fill NULLs only."""

    monkeypatch.setattr(Scraper, "fetch", lambda self, url: "<html></html>")
    article_responses = {
        "http://example.invalid/a": {"title": "Article A title", "summary": "A summary"},
        "http://example.invalid/b": {"title": "Article B title", "summary": "B summary"},
    }
    monkeypatch.setattr(
        Scraper, "fetch_article", lambda self, url: article_responses[url]
    )
    monkeypatch.setattr("sources.scrapers._base.time.sleep", lambda *_: None)

    rows = list(_FakeScraper().run())

    assert len(rows) == 2
    # Row 0: homepage gave title => homepage title wins; summary filled from article.
    assert rows[0]["url"] == "http://example.invalid/a"
    assert rows[0]["title"] == "Home A"
    assert rows[0]["summary"] == "A summary"
    # Row 1: homepage had no title => article title fills it.
    assert rows[1]["url"] == "http://example.invalid/b"
    assert rows[1]["title"] == "Article B title"
    assert rows[1]["summary"] == "B summary"
    # Common columns are present.
    for row in rows:
        assert row["source"] == "fake"
        assert row["country_target"] == "XX"
        assert "extracted_at" in row
```

- [ ] **Step 2: Run the test, expect failure**

Run: `uv run pytest tests/test_scrapers.py::test_run_merges_article_fields -v`
Expected: FAIL — `rows[0]["summary"]` is `None` because the current `run()` does not call `fetch_article`.

- [ ] **Step 3: Update `Scraper.run()`**

Replace the body of `run` in `sources/scrapers/_base.py` so it becomes:

```python
    def run(self) -> Iterator[dict[str, Any]]:
        """Fetch homepage, parse, follow each URL to fill article fields,
        normalise to project schema."""
        extracted_at = pendulum.now("UTC").to_iso8601_string()
        html = self.fetch(self.base_url)
        time.sleep(self.request_delay_s)
        for partial in self.parse(BeautifulSoup(html, "html.parser")):
            url = partial.get("url")
            if not url:
                continue
            article_fields = self.fetch_article(url)
            yield {
                "source": partial.get("source", self.name),
                "country_target": self.country,
                "title": partial.get("title") or article_fields.get("title"),
                "summary": partial.get("summary") or article_fields.get("summary"),
                "url": url,
                "published_at": partial.get("published_at")
                or article_fields.get("published_at"),
                "extracted_at": extracted_at,
            }
```

Only the loop body changes vs. the existing implementation. The `extracted_at`, `html`, `sleep`, and outer `for` lines stay the same.

- [ ] **Step 4: Run the new test, expect pass**

Run: `uv run pytest tests/test_scrapers.py::test_run_merges_article_fields -v`
Expected: PASS.

- [ ] **Step 5: Run the full test file**

Run: `uv run pytest tests/test_scrapers.py -v`
Expected: 3 passed.

- [ ] **Step 6: Confirm existing pipeline still imports**

Run: `uv run python -c "from sources.scrapers.mm.mizzima_burmese import mizzima_burmese; print('ok')"`
Expected: `ok`. Mizzima's `parse_article` is still the default (`{}`) at this point, so `run()` will fall back to homepage-only values until Task 6.

- [ ] **Step 7: Commit**

```bash
git add tests/test_scrapers.py sources/scrapers/_base.py
git commit -m "Wire fetch_article into Scraper.run with homepage-wins precedence"
```

---

## Task 5 (revised): Capture a News Eleven article fixture

**Why revised:** Mizzima Burmese now sits behind Cloudflare (HTTP 403 + `cf-mitigated: challenge`). The PoC's original target is unrunnable from curl / `requests`. We swap to **News Eleven** (`https://news-eleven.com/`), a Burmese-language outlet with real HTML, no Cloudflare gate, no RSS feed, and a `/article/<int>` URL pattern. The Mizzima file stays in tree (unwired) for future revisit.

**Files:**
- Create: `tests/fixtures/news_eleven_article.html`

- [ ] **Step 1: Pick an article URL from the homepage**

Run from `/Users/jack/code/news-intelligence-platform`:

```bash
UA="Mozilla/5.0 (compatible; NewsIntelBot/0.1)"
curl -sL -A "$UA" https://news-eleven.com/ \
  | grep -oE 'href="https://news-eleven\.com/article/[0-9]+"' \
  | head -1
```

Expected: prints one match like `href="https://news-eleven.com/article/309833"`. Strip the `href="` prefix and trailing `"` to get the bare URL.

- [ ] **Step 2: Save the article HTML to a fixture**

Replace `<ARTICLE_URL>` with the URL from Step 1.

```bash
mkdir -p tests/fixtures
UA="Mozilla/5.0 (compatible; NewsIntelBot/0.1)"
curl -sL -A "$UA" "<ARTICLE_URL>" > tests/fixtures/news_eleven_article.html
```

- [ ] **Step 3: Sanity-check the fixture**

```bash
wc -c tests/fixtures/news_eleven_article.html
head -c 200 tests/fixtures/news_eleven_article.html
grep -c 'meta name="description"' tests/fixtures/news_eleven_article.html
```

Expected:
- byte count > 50 000 (a real News Eleven article is roughly 130 KB),
- head looks like `<!DOCTYPE html>` followed by `<html ... lang="my" ...>`,
- exactly 1 `meta name="description"` line (used by `parse_article` in Task 6).

If the byte count is small (< 5 000) or the meta count is 0, retry with a different `/article/<int>` URL.

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/news_eleven_article.html
git commit -m "Add News Eleven article HTML fixture for parse tests"
```

---

## Task 6 (revised): Create `NewsElevenScraper` and wire it into the pipeline

**Files:**
- Create: `sources/scrapers/mm/news_eleven.py`
- Modify: `pipelines/ingest_scrapers.py`
- Modify: `data/config/sources.yaml`
- Modify: `tests/test_scrapers.py` (append test)

This task creates a brand-new outlet module and wires it in. The implementation order is: tests first (test of `parse_article` against the fixture), then the module, then the pipeline rewire, then `sources.yaml`.

- [ ] **Step 1: Inspect the fixture and confirm the selector**

```bash
grep -oE '<meta name="description"[^>]+>' tests/fixtures/news_eleven_article.html | head -1
grep -oE '<meta property="og:description"[^>]+>' tests/fixtures/news_eleven_article.html | head -1
```

Expected: both lines are present. `parse_article` will prefer `name="description"` and fall back to `og:description`.

- [ ] **Step 2: Write the failing test**

Append to `tests/test_scrapers.py`:

```python
from pathlib import Path

from sources.scrapers.mm.news_eleven import NewsElevenScraper

FIXTURES = Path(__file__).parent / "fixtures"


def test_news_eleven_parse_article_extracts_summary():
    html = (FIXTURES / "news_eleven_article.html").read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")
    fields = NewsElevenScraper().parse_article(soup)
    summary = fields.get("summary")
    assert summary is not None
    assert len(summary.strip()) > 20  # not whitespace or a stub
```

- [ ] **Step 3: Run the test, expect failure**

Run: `uv run pytest tests/test_scrapers.py::test_news_eleven_parse_article_extracts_summary -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'sources.scrapers.mm.news_eleven'`.

- [ ] **Step 4: Create `sources/scrapers/mm/news_eleven.py`**

Write the file:

```python
"""News Eleven (Eleven Media Burmese) homepage scraper.

Burmese-language outlet at https://news-eleven.com/ with no RSS feed.
Article URLs follow the pattern `/article/<int>`. Article pages carry
a `<meta name="description">` (Burmese text) that we use to populate
the summary column.
"""

import re
from typing import Any

import dlt
from bs4 import BeautifulSoup

from sources.scrapers._base import Scraper

ARTICLE_HREF_RE = re.compile(r"^https://news-eleven\.com/article/\d+$")
MAX_HOMEPAGE_LINKS = 100


class NewsElevenScraper(Scraper):
    name = "News Eleven"
    country = "MM"
    base_url = "https://news-eleven.com/"
    request_delay_s = 1.0

    def parse(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        """Pull article anchors from the homepage, dedupe by URL."""
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for anchor in soup.find_all("a", href=True):
            url = anchor["href"]
            if not ARTICLE_HREF_RE.match(url) or url in seen:
                continue
            seen.add(url)
            title = anchor.get_text(strip=True) or None
            rows.append({"url": url, "title": title})
            if len(rows) >= MAX_HOMEPAGE_LINKS:
                break
        return rows

    def parse_article(self, soup: BeautifulSoup) -> dict[str, Any]:
        """Pull the article summary from the article page itself.

        News Eleven article pages carry a meta description; fall back
        to og:description.
        """
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            return {"summary": meta["content"].strip()}
        og = soup.find("meta", attrs={"property": "og:description"})
        if og and og.get("content"):
            return {"summary": og["content"].strip()}
        return {}


@dlt.resource(name="articles", primary_key="url", write_disposition="merge")
def news_eleven():
    """One row per article URL discovered on the News Eleven homepage."""
    yield from NewsElevenScraper().run()
```

- [ ] **Step 5: Run the test, expect pass**

Run: `uv run pytest tests/test_scrapers.py::test_news_eleven_parse_article_extracts_summary -v`
Expected: PASS.

- [ ] **Step 6: Run the full test file**

Run: `uv run pytest tests/test_scrapers.py -v`
Expected: 4 passed.

- [ ] **Step 7: Rewire `pipelines/ingest_scrapers.py`**

Open `pipelines/ingest_scrapers.py`. Replace the import + yield to point at News Eleven instead of Mizzima. The file's current shape is:

```python
import dlt

from sources.scrapers.mm.mizzima_burmese import mizzima_burmese


@dlt.source(name="scrapers")
def scrapers_source():
    yield mizzima_burmese()
```

Change it to:

```python
import dlt

from sources.scrapers.mm.news_eleven import news_eleven


@dlt.source(name="scrapers")
def scrapers_source():
    yield news_eleven()
```

Leave the rest of the file (the `run()` function and the `if __name__ == "__main__"` block) untouched.

- [ ] **Step 8: Confirm the rewired pipeline still imports cleanly**

```bash
uv run python -c "from pipelines.ingest_scrapers import scrapers_source; print('ok')"
```

Expected: `ok` printed, no traceback.

- [ ] **Step 9: Update `data/config/sources.yaml`**

Two edits inside the `MM:` section:

1. Add the News Eleven entry at the end of the MM list (after the existing Mizzima Burmese block):

```yaml
    - name: Eleven Media Burmese
      url: https://news-eleven.com/
      rss: ""
      language: my
      scrape: beautifulsoup
      robots_ok: unknown
      notes: "No RSS. Scraped via news_eleven.py."
```

2. Update the `notes:` line of the existing Mizzima Burmese entry to flag that it is currently unreachable:

Find:
```yaml
      notes: "No RSS on Burmese edition. Scraped via mizzima_burmese.py."
```

Replace with:
```yaml
      notes: "No RSS on Burmese edition. Cloudflare-blocks plain HTTP fetches as of 2026-05-25. mizzima_burmese.py stays in tree, currently unwired."
```

Do not touch any other entries in `sources.yaml`.

- [ ] **Step 10: Commit**

```bash
git add tests/test_scrapers.py sources/scrapers/mm/news_eleven.py pipelines/ingest_scrapers.py data/config/sources.yaml
git commit -m "Add News Eleven BS4 scraper, swap Mizzima out of the pipeline"
```

---

## Task 7 (revised): End-to-end smoke check via News Eleven

**Files:** none modified. Verification only.

- [ ] **Step 1: Run the rewired pipeline**

Run: `PYTHONPATH=. uv run python pipelines/ingest_scrapers.py`

Expected: a `LoadInfo` line printed at the end, no traceback. The run typically takes 1-2 minutes because each homepage URL is followed to its article page at 1 req/s.

- [ ] **Step 2: Inspect the loaded `articles` table**

```bash
uv run python -c "import duckdb; con = duckdb.connect('scrapers.duckdb'); print(con.execute(\"select count(*) total, count(summary) filled from scrapers_raw.articles\").fetchall())"
```

Expected: a tuple `[(N, K)]` where `K > 0` (rows with non-NULL summary).

Or open `uv run dlt pipeline scrapers show` to browse interactively and confirm by eye.

- [ ] **Step 3: If `filled` is 0**

Cases and remedies:

- The homepage anchor selector returned nothing (no `/article/<int>` matches): inspect the homepage with `curl ...` and adjust `ARTICLE_HREF_RE` or the loop.
- All article fetches failed: check `request_delay_s`, the User-Agent, and the news-eleven.com response status with a manual curl.
- Articles have no `<meta name="description">` (rare): extend `parse_article` to fall back to a body paragraph extractor.

If `filled > 0`, the task is done. No commit on a successful smoke (it is verification).

---

## Self-review check (revised after Cloudflare-block discovery)

The plan covers every requirement in `docs/superpowers/specs/2026-05-25-article-page-backfill-design.md`:

- **Add `parse_article` default `{}`**: Task 2. Done at commit `66856c3`.
- **Add `fetch_article` swallowing `requests.RequestException`**: Task 3. Done at commit `544434c`.
- **Update `run()` to merge article fields with homepage-wins precedence**: Task 4 + the empty-string precedence fix. Done at commits `ed1a8f9` and `b8e6e07`.
- **Create `NewsElevenScraper.parse_article` returning `{"summary": ...}`**: Task 6 (uses fixture from Task 5).
- **Wire `news_eleven` into `pipelines/ingest_scrapers.py`, unwire `mizzima_burmese`**: Task 6 Step 7.
- **Add News Eleven entry to `data/config/sources.yaml`, annotate Mizzima as Cloudflare-blocked**: Task 6 Step 9.
- **Unit tests in `tests/test_scrapers.py`**: built up across Tasks 2, 3, 4, 6.
- **Fixture in `tests/fixtures/news_eleven_article.html`**: Task 5.
- **Add `pytest` to `requirements.txt`**: Task 1. Done at commit `79018b7`.
- **Smoke run via existing entry point**: Task 7.

Out of scope (per spec): retries, robots.txt, skip-already-fetched, structured logging, additional outlets beyond News Eleven, `body` column, Kafka, Cloudflare-defeating fetch. None of these appear in the tasks.

## Notes for the executor

- Run every command from `/Users/jack/code/news-intelligence-platform/` (repo root). dlt resolves `.dlt/secrets.toml` from cwd, and `PYTHONPATH=.` is the project convention for invoking scripts.
- Use `uv run` for Python invocations; the project venv is managed by uv.
- Commit messages follow the team's imperative-sentence-case style; no `feat:` / `fix:` prefixes.
- Do **not** add a `Co-Authored-By: Claude` trailer (per global user rule).
- If a test fails after your change, fix the code (not the test) unless the test itself is wrong.
- The current branch may be `feat/streaming-feel` or another feature branch; do not switch branches without asking. If you are unsure, ask before pushing.
