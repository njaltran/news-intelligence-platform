# sources/

dlt source and resource definitions. **No I/O orchestration here.** Each file declares *how to pull* from a source; the `pipelines/` folder wires sources to destinations.

## Layout

```
sources/
├── __init__.py
├── newsapi.py            # @dlt.source for NewsAPI (Nadi)
├── gdelt.py              # @dlt.source for GDELT (Nadi)
├── rss.py                # @dlt.source for RSS feeds; reads data/config/sources.yaml (Nadi)
└── scrapers/             # BeautifulSoup scrapers wrapped as dlt resources (Jack)
    ├── _base.py          # shared scraper-as-resource factory
    ├── mm/               # one file per Myanmar outlet
    └── kz/               # one file per Kazakhstan outlet
```

## Conventions

- Each source returns dlt resources via `@dlt.source` and `@dlt.resource`.
- Use `dlt.sources.incremental` for any source with a date cursor.
- Pass auth via `dlt.secrets[...]`, never as function arguments.
- Each scraper subclass extends `_base.Scraper` and implements `parse(html) -> list[dict]`.
