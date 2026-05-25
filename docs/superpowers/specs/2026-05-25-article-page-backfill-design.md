# Article-page backfill for BS4 scrapers

**Date:** 2026-05-25
**Repo:** news-intelligence-platform
**Authors:** Jack (EA specialist) + Claude (brainstorming session)
**Status:** Approved (revised 2026-05-25 after Cloudflare-block discovery). Awaiting implementation continuation.

## Goal

Extend the existing `Scraper` base class in `sources/scrapers/_base.py` with an article-page backfill step. Today the homepage parser yields rows with only `title` + `url`. `summary` is always NULL. After this change, each yielded row optionally has `summary` filled from the article page itself.

The first consumer of the new path is a new BS4 outlet: **Eleven Media Burmese** (`https://news-eleven.com/`). Future BS4 outlets inherit the same hook for free.

## Why this outlet, why not Mizzima

The original spec named Mizzima Burmese (`https://www.mizzimaburmese.com/`) as the first consumer. During implementation we discovered Mizzima now sits behind Cloudflare's challenge layer (HTTP 403 + `cf-mitigated: challenge`), which makes the existing PoC unrunnable in practice. The team's prior `sources/scrapers/mm/mizzima_burmese.py` stays in tree (unwired) as future reference; we will revisit it if we add a Cloudflare-defeating fetcher.

Eleven Media Burmese was picked because it:

- Returns real HTML (HTTP 200, ~130 KB pages) with no Cloudflare gate.
- Is a Burmese-language outlet (`<html lang="my">`), keeping the Long Tail thesis intact.
- Has no RSS feed (`/feed` and `/feed/` both 404), so it earns BS4 scraping rather than duplicating RSS.
- Carries `<meta name="description">` and `<meta property="og:description">` on article pages, so `parse_article` can populate `summary` cleanly.
- Uses stable, predictable URL paths (`/article/<int>`).

## Why this is the right next step

The `sources.yaml` registry already covers DE / US / IT / MM / KZ via RSS or APIs. The remaining gap is the explicit "out of scope for the PoC" line in `_base.py`'s docstring: homepage cards do not carry summary text, so summaries stay NULL until a follow-up GET happens per article.

Filling `summary` is what makes scraped rows usable downstream (embeddings, topic modelling, dashboard preview text). This change unblocks every BS4 outlet we add later.

## EA framing

- [[Veracity]]. Title-only rows are low-veracity input for topic modelling. Summaries materially raise the V.
- [[Long Tail]]. Eleven Media Burmese is an RSS-less Long Tail outlet. The pattern unlocks more such outlets without a redesign.
- [[Lambda vs Kappa]]. Scrapers remain in the **Lambda batch arm** of the hybrid architecture (per `CLAUDE.md`). This change does not touch the Kafka / Kappa arm.
- [[Variety]]. The schema is unchanged. Variety stays high because we keep the unified row shape across RSS, API, and BS4 sources.

## Scope

In scope:

- Add `parse_article(soup) -> dict` to `Scraper` with a `{}` default (so homepage-only subclasses keep working). **Done**.
- Add `fetch_article(url) -> dict` helper that GETs the article, calls `parse_article`, swallows network errors, and respects `request_delay_s`. **Done**.
- Update `Scraper.run()` to call `fetch_article` for every yielded URL and merge its fields into the row (homepage value wins, article fields fill NULLs only). **Done**.
- Create `sources/scrapers/mm/news_eleven.py` implementing `NewsElevenScraper` (homepage parser + `parse_article` returning `{"summary": ...}`).
- Wire `news_eleven` into `pipelines/ingest_scrapers.py` and stop wiring `mizzima_burmese`.
- Add a `News Eleven (Eleven Media Burmese)` entry to `data/config/sources.yaml` flagged `scrape: beautifulsoup`. Optionally flip the existing Mizzima Burmese entry to indicate it is blocked (note in `notes`), but leave the file in tree.
- Unit tests for the new code paths.
- Saved News Eleven article HTML fixture for the parse test.

Out of scope (deferred):

- Defeating Cloudflare (cloudscraper, curl_cffi, browserless) to re-enable the Mizzima PoC. Tracked as an open question; would justify its own design.
- Skip-already-fetched optimisation (re-fetching every URL on every run is wasteful but safe; merge on `url` keeps the table tidy).
- Retries / backoff.
- Robots.txt check (matches the current PoC; documented as an open question).
- Structured logging via `pipelines/kafka/_log.py`.
- Adding any new outlet.
- Adding a `body` column to the schema (Phase 2 if downstream embeddings need it).
- Kafka / ClickHouse changes.

## Files changed

| path                                                      | change |
|-----------------------------------------------------------|--------|
| `sources/scrapers/_base.py`                               | edit. Add `parse_article` and `fetch_article`. Update `run()`. **Done**. |
| `sources/scrapers/mm/news_eleven.py`                      | new. `NewsElevenScraper` + `news_eleven()` dlt resource. |
| `pipelines/ingest_scrapers.py`                            | edit. Import and yield `news_eleven` instead of `mizzima_burmese`. |
| `data/config/sources.yaml`                                | edit. Add News Eleven entry under MM with `scrape: beautifulsoup`. Add a `notes:` line to the Mizzima Burmese entry that it is currently Cloudflare-blocked. |
| `tests/test_scrapers.py`                                  | edit. All scraper tests (default `parse_article`, `fetch_article` error swallow, `run()` merge precedence — **done**; News Eleven fixture parse — pending). Matches the per-source layout in `tests/README.md`. |
| `tests/fixtures/news_eleven_article.html`                 | new. One saved News Eleven article page. |
| `requirements.txt`                                        | edit. Add `pytest>=8`. **Done**. |

`tests/` originally contained only `__init__.py` and `README.md`. This change creates the `tests/fixtures/` subdirectory.

`sources/scrapers/mm/mizzima_burmese.py` is **left in tree, unwired**. No changes to schema or any other outlet.

## Architecture

```
homepage HTML --> parse() --> partial rows (url, title, maybe published_at)
                                   |
                                   v                    [new step]
                            article HTML for each url
                                   |
                                   v
                              parse_article() --> {summary, published_at, ...}
                                   |
                                   v
                              merge into row, yield
```

Single sequential pass. No concurrency. No state.

## Component contracts

### `Scraper` (edit)

```python
def parse_article(self, soup: BeautifulSoup) -> dict[str, Any]:
    """Parse a single article page. Override per outlet.
    Default returns {} so subclasses can opt out."""
    return {}

def fetch_article(self, url: str) -> dict[str, Any]:
    """Fetch one article page, return parsed fields. On failure, return {}."""
    try:
        html = self.fetch(url)
    except requests.RequestException:
        return {}
    time.sleep(self.request_delay_s)
    return self.parse_article(BeautifulSoup(html, "html.parser"))
```

`run()` body changes to:

```python
for partial in self.parse(BeautifulSoup(html, "html.parser")):
    url = partial.get("url")
    if not url:
        continue
    article_fields = self.fetch_article(url)  # {} for opt-out subclasses
    yield {
        "source": partial.get("source", self.name),
        "country_target": self.country,
        "title": partial.get("title") or article_fields.get("title"),
        "summary": partial.get("summary") or article_fields.get("summary"),
        "url": url,
        "published_at": partial.get("published_at") or article_fields.get("published_at"),
        "extracted_at": extracted_at,
    }
```

Precedence rule: **homepage value wins**; `article_fields` fill NULLs only. Justified because homepage parsers are written against deterministic CSS selectors per outlet; article pages may have noisy `<meta>` tags or boilerplate.

### `NewsElevenScraper` (new module `sources/scrapers/mm/news_eleven.py`)

Subclasses `Scraper`. Attributes:

- `name = "News Eleven"`
- `country = "MM"`
- `base_url = "https://news-eleven.com/"`
- `request_delay_s = 1.0`

Methods:

- `parse(soup) -> list[dict]`: extract all `<a href="https://news-eleven.com/article/<int>">` anchors, dedupe, yield one partial per article with `url` and `title` (the anchor text, stripped). Capped to first 100 unique URLs per run (cheap parity with the existing Mizzima cap of one homepage scrape).
- `parse_article(soup) -> dict`: return `{"summary": <text>}` from the article page's `<meta name="description">`. Fall back to `<meta property="og:description">`. Returns `{}` if neither is present.

The module also exposes a `news_eleven` dlt resource (`@dlt.resource(name="articles", primary_key="url", write_disposition="merge")`) that yields from `NewsElevenScraper().run()`. Mirrors the existing `mizzima_burmese()` resource shape.

## Data flow

Per call to `mizzima_burmese()` (one per `pipeline.run()`):

1. `fetch(base_url)` -> homepage HTML.
2. `parse(soup)` -> N partial rows (~62 today).
3. For each row's `url`:
   - `fetch_article(url)` GETs the article, sleeps `request_delay_s`, calls `parse_article`.
   - Returns `{"summary": ...}` on success, `{}` on network or parse failure.
4. `run()` merges and yields the row with the unified schema.
5. dlt resource (`@dlt.resource(name="articles", primary_key="url", write_disposition="merge")`) lands rows into `scrapers_raw.articles` in DuckDB, replacing prior rows that share the same `url`.

Total HTTP requests per run: 1 homepage + ~62 articles = ~63. At 1 req/s, ~1 minute. Within any sensible politeness budget.

## Error handling

- `fetch_article` catches `requests.RequestException` only. Covers timeouts, connection errors, and non-2xx (the underlying `fetch` raises via `resp.raise_for_status()`). Other exceptions (e.g. parsing errors inside `parse_article`) propagate; the run dies. Justified: a broken parser is a code bug we want to see, not silently absorb.
- No retries. A transient failure means NULL `summary` for that URL until the next run. The dlt merge will overwrite with the better row when a later run succeeds.
- `KeyboardInterrupt` is not handled (matches existing PoC).

## Politeness

- `time.sleep(self.request_delay_s)` after every fetch. Already applied after the homepage fetch; now also applied after each article fetch.
- `request_delay_s` default stays at 1.0.
- User-Agent stays `"Mozilla/5.0 (compatible; NewsIntelBot/0.1)"` (already defined in `_base.py`).
- Robots.txt check is **not added**. Matches the current PoC. Documented as an open question.

## Logging

One stdout line per article fetch:

```
[mizzima_burmese] article 12/62 ok https://www.mizzimaburmese.com/...
```

Matches the print-based style already in use in `_base.py` and `mizzima_burmese.py`. Switching to structured logging (`pipelines/kafka/_log.py`) is a Phase 2 nicety.

## Testing

Unit tests (pytest, no network). All in `tests/test_scrapers.py` per the team's `tests/README.md` convention.

- `test_parse_article_default()`: confirm `Scraper.parse_article(soup) == {}`. **Done**.
- `test_run_merges_article_fields()`: subclass `Scraper`, stub `fetch` and `fetch_article` to return canned values, assert `run()` yields rows with homepage values preferred and article values filling NULLs. Monkeypatch `time.sleep` to no-op. Covers an empty-string regression case. **Done**.
- `test_fetch_article_swallows_errors()`: monkeypatch `Scraper.fetch` to raise `requests.RequestException`, assert `fetch_article` returns `{}`. **Done**.
- `test_news_eleven_parse_article()`: load `tests/fixtures/news_eleven_article.html`, pass through `NewsElevenScraper().parse_article(BeautifulSoup(html, "html.parser"))`, assert non-empty `summary`.

Developer smoke (opt-in):

- `PYTHONPATH=. uv run python pipelines/ingest_scrapers.py` and inspect `summary` column with `uv run dlt pipeline scrapers show` or DuckDB CLI.

Not tested:

- Live Mizzima availability.
- Robots.txt.
- Retry logic.
- Kafka / ClickHouse.

## Open questions parked for later

- Robots.txt check inside `fetch_article`.
- Skip-already-fetched optimisation (read `pipeline.dataset()` and skip URLs whose `summary IS NOT NULL`).
- Add a `body` column for downstream embeddings.
- Move scraper output to the Kafka producer pattern so it also feeds ClickHouse.
- Identify additional RSS-less Long Tail outlets that justify new BS4 subclasses.
- Resurrect the Mizzima Burmese PoC with a Cloudflare-defeating fetcher (cloudscraper / curl_cffi / browserless). Currently unwired.

## Next step

Hand off to `superpowers:writing-plans` to turn this design into a step-by-step implementation plan.
