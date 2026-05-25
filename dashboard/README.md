# dashboard/

marimo reactive notebook that reads from ClickHouse and serves the narrative-divergence view.

Owned by all (Karina drives demo storyline, Nadi wires queries, Jack frames EA narrative).

## Layout

```
dashboard/
├── __init__.py
└── app.py          # marimo notebook (was gdelt_dashboard.py at repo root)
```

## Run

```bash
uv run marimo edit dashboard/app.py     # reactive edit mode
uv run marimo run dashboard/app.py      # read-only app mode
```

## Conventions

- Queries hit ClickHouse for aggregations, DuckDB for ad-hoc raw inspection.
- Use ibis for complex queries (joins, computed columns), SQL for the simple ones.
- Charts use altair, never matplotlib. Themed consistently.
