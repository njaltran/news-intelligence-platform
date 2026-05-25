# Article-page backfill for BS4 scrapers

**Date:** 2026-05-25
**Repo:** news-intelligence-platform
**Authors:** Jack (EA specialist) + Claude (brainstorming session)
**Status:** Approved design, awaiting implementation plan.

## Goal

Extend the existing `Scraper` base class in `sources/scrapers/_base.py` with an article-page backfill step. Today the homepage parser yields rows with only `title` + `url` (and, for Mizzima, a `published_at` derived from the URL path). `summary` is always NULL. After this change, each yielded row optionally has `summary` filled from the article page itself.

Mizzima Burmese is the first (and currently only) consumer of the new path. Future BS4 outlets inherit the same hook for free.

## Why this is the right next step

The `sources.yaml` registry already covers DE / US / IT / MM / KZ via RSS or APIs. The only outlet currently flagged `scrape: beautifulsoup` is Mizzima Burmese, because the Burmese edition has no RSS. So "more BS4 scrapers" mostly duplicates RSS work that already exists. The remaining gap in the BS4 path is the explicit "out of scope for the PoC" line in `_base.py`'s docstring: homepage cards do not carry summary text, so summaries stay NULL until a follow-up GET happens per article.

Filling `summary` is what makes scraped rows usable downstream (embeddings, topic modelling, dashboard preview text). This change unblocks every BS4 outlet we add later.

## EA framing

- [[Veracity]]. Title-only rows are low-veracity input for topic modelling. Summaries materially raise the V.
- [[Long Tail]]. Mizzima Burmese is the canonical RSS-less Long Tail outlet. The pattern unlocks more such outlets without a redesign.
- [[Lambda vs Kappa]]. Scrapers remain in the **Lambda batch arm** of the hybrid architecture (per `CLAUDE.md`). This change does not touch the Kafka / Kappa arm.
- [[Variety]]. The schema is unchanged. Variety stays high because we keep the unified row shape across RSS, API, and BS4 sources.

## Scope

In scope:

- Add `parse_article(soup) -> dict` to `Scraper` with a `{}` default (so homepage-only subclasses keep working).
- Add `fetch_article(url) -> dict` helper that GETs the article, calls `parse_article`, swallows network errors, and respects `request_delay_s`.
- Update `Scraper.run()` to call `fetch_article` for every yielded URL and merge its fields into the row (homepage value wins, article fields fill NULLs only).
- Implement `MizzimaBurmeseScraper.parse_article` returning `{"summary": ...}`. Selector identified during implementation by inspecting one Mizzima article page.
- Unit tests for the new code paths.
- Saved Mizzima article HTML fixture for the parse test.

Out of scope (deferred):

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
| `sources/scrapers/_base.py`                               | edit. Add `parse_article` and `fetch_article`. Update `run()`. |
| `sources/scrapers/mm/mizzima_burmese.py`                  | edit. Add `parse_article` returning `{"summary": ...}`. |
| `tests/scrapers/__init__.py`                              | new. Subpackage marker. |
| `tests/scrapers/test_parse_article_default.py`            | new. Default returns `{}`. |
| `tests/scrapers/test_run_merges_article_fields.py`        | new. Precedence and NULL-fill behaviour. |
| `tests/scrapers/test_fetch_article_swallows_errors.py`    | new. Failure returns `{}`. |
| `tests/scrapers/test_mizzima_parse_article.py`            | new. Fixture-driven parse test. |
| `tests/fixtures/mizzima_article.html`                     | new. One saved Mizzima article page. |

`tests/` currently contains only `__init__.py` and `README.md`. This change creates the `tests/scrapers/` and `tests/fixtures/` subdirectories.

No changes to `pipelines/ingest_scrapers.py`, `data/config/sources.yaml`, schema, or any other outlet.

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

### `MizzimaBurmeseScraper.parse_article` (new method)

Returns `{"summary": <text>}`. Selector is identified during implementation by inspecting one Mizzima article page (`curl https://www.mizzimaburmese.com/<some-2026-path> | less`) and choosing the right `<meta name="description">` or main `<article>` paragraph. The plan, not the spec, picks the selector.

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

Unit tests (pytest, no network):

- `tests/scrapers/test_parse_article_default.py`: confirm `Scraper.parse_article(soup) == {}`.
- `tests/scrapers/test_run_merges_article_fields.py`: subclass `Scraper`, stub `fetch` and `fetch_article` to return canned values, assert `run()` yields rows with homepage values preferred and article values filling NULLs. Monkeypatch `time.sleep` to no-op.
- `tests/scrapers/test_fetch_article_swallows_errors.py`: monkeypatch `Scraper.fetch` to raise `requests.RequestException`, assert `fetch_article` returns `{}`.
- `tests/scrapers/test_mizzima_parse_article.py`: load `tests/fixtures/mizzima_article.html`, pass through `MizzimaBurmeseScraper().parse_article(BeautifulSoup(html, "html.parser"))`, assert non-empty `summary`.

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

## Next step

Hand off to `superpowers:writing-plans` to turn this design into a step-by-step implementation plan.
