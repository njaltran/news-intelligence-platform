# 05. Scraping Pattern

How HTML-scraped outlets are wired into the ingestion layer. Companion to `04-rss-pattern.md` (RSS) and `06-code-layout.md` (folder rationale).

## When to scrape vs. RSS

Order of preference for any new outlet:

1. **RSS** if the outlet publishes one. Goes in `sources/rss.py`, configured in `data/config/sources.yaml` with `scrape: rss`. No new code.
2. **Scrape** only when no RSS exists, the RSS is dead, or it's gated. Goes in `sources/scrapers/<country>/<outlet>.py`, configured with `scrape: beautifulsoup`.

A scraped outlet is always more fragile than an RSS feed: layout changes break it silently. Use sparingly. The Long Tail outlets (MM Burmese, KZ Kazakh) are the prime case because several of them ship no RSS at all.

## File layout

```
sources/scrapers/
├── _base.py                    # Scraper class: fetch + run skeleton
├── mm/
│   └── mizzima_burmese.py      # one file per outlet
└── kz/
    └── <outlet>.py
```

One file per outlet, organised by ISO 3166-1 alpha-2 country code. Naming: `snake_case.py`, class name `<Outlet>Scraper`.

## The pattern

Each outlet subclasses `Scraper` from `_base.py` and implements one method: `parse(soup) -> list[dict]`. The base class handles the HTTP fetch, the User-Agent, the politeness delay, the row normalisation, and yields rows to dlt.

```python
from sources.scrapers._base import Scraper

class MyOutletScraper(Scraper):
    name = "My Outlet"
    country = "MM"
    base_url = "https://example.com/"

    def parse(self, soup):
        rows = []
        for card in soup.select("article.post"):
            heading = card.find("h2")
            anchor = card.find("a", href=True)
            if not heading or not anchor:
                continue
            rows.append({
                "title": heading.get_text(strip=True),
                "url": anchor["href"],
                "published_at": None,  # backfill later or parse from page
            })
        return rows
```

Then wrap as a dlt resource and register in `pipelines/ingest_scrapers.py`:

```python
@dlt.resource(name="articles", primary_key="url", write_disposition="merge")
def my_outlet():
    yield from MyOutletScraper().run()
```

All scraped resources share the same logical table (`articles`) and merge on `url`, so reruns dedupe automatically.

## What `_base.Scraper` provides

- **`USER_AGENT`** identifies the bot. Several MM/KZ outlets behind light WAFs reject the default `requests` UA with 403. A real-looking UA is the cheapest workaround.
- **`fetch(url)`** GET with the UA, raises on non-2xx.
- **`request_delay_s`** politeness sleep after each fetch.
- **`run()`** the orchestration loop: fetch homepage, parse, normalise each partial row to the project schema, yield to dlt.

Rows yielded follow `source, country_target, title, summary, url, published_at, extracted_at`. The subclass fills what it can from the homepage card; the rest is None and can be backfilled by a later per-article pass (out of scope for the PoC).

## Checklist for a new outlet

1. Check `robots.txt`. Respect it. Note the result in `sources.yaml` (`robots_ok: true/false/unknown`).
2. Confirm no RSS exists (or the RSS is unusable). If it does, use `sources/rss.py` instead.
3. Add the outlet to `data/config/sources.yaml` with `scrape: beautifulsoup`.
4. Write `sources/scrapers/<country>/<outlet>.py` subclassing `Scraper`.
5. Test parsing against a single homepage GET before wiring into the pipeline.
6. Register the `@dlt.resource` in `pipelines/ingest_scrapers.py`.
7. Run end-to-end. Confirm rows land in the merged `articles` table and that `country_target` matches.

## EA framing

- **Variety** (one of the 5 Vs): scraping is the only way to defend multi-language coverage when the outlet does not expose a feed. Without this path, the corpus collapses to English-only outlets in non-EU/US countries.
- **Long Tail**: the pattern exists primarily because Myanmar (Burmese) and Kazakhstan (Kazakh, Russian) outlets are exactly the ones least likely to ship RSS or be covered by aggregators like GDELT and NewsAPI.
- **Veracity**: scrapes break silently when layouts change. The pattern keeps the blast radius small (one file per outlet) so a layout change does not break unrelated countries.
- **Lambda vs Kappa**: scrapes are batch-pull only. They sit on the Lambda arm of the architecture, complementing the Kafka streaming arm (see `infra/docker-compose.yml`).

## Worked example: Mizzima Burmese

`sources/scrapers/mm/mizzima_burmese.py`.

- Burmese-language edition of Mizzima. No RSS on this edition (the English edition has one, see `sources.yaml`).
- Homepage serves ~60 post tiles in `div.mag-post-single`. Title is in the first heading inside the tile; URL is in the first anchor; the article ID and the publication date are encoded in the URL path: `https://bur.mizzima.com/YYYY/MM/DD/<id>`.
- Summary is not present on the tile and would require a follow-up GET per article. Skipped for the PoC. Downstream cleaning can backfill if needed.
- Output: ~40 unique articles per run (after `url` merge deduplication), all in Burmese script.
