# dashboard/

marimo reactive notebook that reads from ClickHouse and serves the narrative-divergence view.

Owned by all (Karina drives demo storyline, Nadi wires queries, Jack frames EA narrative).

## Layout

```
dashboard/
├── __init__.py
├── app.py              # marimo narrative-divergence notebook (planned full view)
└── streamlit_app.py    # lightweight live-feed view over the Kappa path
```

Two dashboards on purpose. `streamlit_app.py` is the live eyeball-test on top of the Kafka -> ClickHouse stream (auto-refreshes every 5s, sparklines for arrival rate, recent-articles table). `app.py` is the heavier marimo notebook that will host the narrative-divergence story once embeddings and topics land.

## Run

```bash
# Streamlit live feed (reads news.news___articles in ClickHouse).
uv run streamlit run dashboard/streamlit_app.py

# marimo notebook.
uv run marimo edit dashboard/app.py     # reactive edit mode
uv run marimo run dashboard/app.py      # read-only app mode
```

The Streamlit app is also wired into `scripts/dev_stack.sh` for the one-command local dev loop (broker + consumer + producer + dashboard).

## Config

`streamlit_app.py` reads ClickHouse connection details from env vars (defaults match `infra/docker-compose.yml`):

```
CLICKHOUSE_HOST, CLICKHOUSE_HTTP_PORT, CLICKHOUSE_USER,
CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, CLICKHOUSE_TABLE,
DASHBOARD_SPARKLINE_MIN
```

## Conventions

- Queries hit ClickHouse for aggregations, DuckDB for ad-hoc raw inspection.
- Use ibis for complex queries (joins, computed columns), SQL for the simple ones.
- Charts use altair, never matplotlib. Themed consistently.
