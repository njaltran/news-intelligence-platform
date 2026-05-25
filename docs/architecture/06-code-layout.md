# 06. Code Layout

Where code lives and why. Keep each concern in its own folder so the team can work in parallel without merge conflicts.

## Folders

| Folder | Concern | Owner |
|--------|---------|-------|
| `sources/` | dlt `@source` / `@resource` per data source. **Pull logic only**, no I/O orchestration. | Nadi (APIs, RSS), Jack (scrapers) |
| `sources/scrapers/` | BeautifulSoup scrapers wrapped as dlt resources. One file per outlet, organised by country (`mm/`, `kz/`). | Jack |
| `processing/` | Post-ingestion transforms: clean, embed, topic-model, compute divergence. | Nadi (Jack reviews schema) |
| `warehouse/` | Destination configs (DuckDB raw, ClickHouse modelled) + DDL + model schemas. | Nadi + Jack |
| `dashboard/` | Two views over ClickHouse: `streamlit_app.py` for the live Kappa-path feed (auto-refresh, sparklines), `app.py` for the marimo narrative-divergence notebook (planned full story). | All |
| `pipelines/` | Thin entry points wiring sources to destinations. Batch arm at the top level (`ingest_apis.py`, `ingest_rss.py`, `ingest_scrapers.py`), streaming arm under `pipelines/kafka/` (Kappa path: producers + dlt consumer). **No business logic.** | Nadi |
| `scripts/` | CLI helpers. Currently: `dev_stack.sh` (one-command local dev loop: broker + consumer + producer + Streamlit). Future: source validation, schema dumps. | Anyone |
| `tests/` | pytest suite mirroring the source layout. | Author of code under test |

## Dependency direction

```
pipelines/  ->  sources/   +   warehouse/destinations
pipelines/  ->  processing/
processing/ ->  warehouse/models
dashboard/  ->  warehouse/  (reads ClickHouse, no writes)
```

Sources do not import from processing. Processing does not import from sources. Pipelines tie them together.

## Migration from the current flat layout

Existing files at the repo root will be moved as follows. Migration is a follow-up PR per file, not all at once.

| Current file | Target location |
|--------------|-----------------|
| ~~`rest_api_pipeline.py`~~ | ~~split into `sources/gdelt.py`, `sources/newsapi.py` (stub), `pipelines/ingest_apis.py`~~ **done** |
| ~~`gdelt_dashboard.py`~~ | ~~`dashboard/app.py`~~ **done** |

Migration complete. All new code follows the layout described above.

## Naming

- Files: `snake_case.py`.
- Classes (scrapers): `<Outlet>Scraper`, e.g. `IrrawaddyScraper`.
- dlt resources: `@dlt.resource(name="articles", ...)` — every source emits to the same logical table.
- Per-country folders use ISO 3166-1 alpha-2 codes (`mm`, `kz`, `de`, `us`, `it`).
