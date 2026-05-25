# pipelines/

Entry points that wire sources to destinations. Each pipeline is a thin runner: import sources, import destinations, call `pipeline.run()`. **No business logic here.**

Owned by Nadi.

## Layout

```
pipelines/
├── __init__.py
├── ingest_apis.py        # NewsAPI + GDELT   -> DuckDB raw lake   (batch / Lambda arm)
├── ingest_rss.py         # RSS feeds         -> DuckDB raw lake   (batch / Lambda arm)
├── ingest_scrapers.py    # MM + KZ scrapers  -> DuckDB raw lake   (batch / Lambda arm)
├── kafka/                # streaming arm (Kappa path), see kafka/README.md
│   ├── producer_newsapi.py
│   ├── producer_bbc.py
│   ├── producer_local_scrapers.py
│   └── consumer_to_duckdb.py
├── process.py            # clean + embeddings + topics + divergence
└── build_warehouse.py    # model raw tables -> ClickHouse
```

## Run

```bash
uv run python pipelines/ingest_apis.py
uv run python pipelines/ingest_rss.py
uv run python pipelines/ingest_scrapers.py
uv run python pipelines/process.py
uv run python pipelines/build_warehouse.py
```

## Conventions

- One pipeline file per logical batch. Keep each under ~100 lines.
- All pipelines use the same `pipeline_name="news_intel"` so they share dlt state.
- Incremental loading via `dlt.sources.incremental` so reruns only pull deltas.
