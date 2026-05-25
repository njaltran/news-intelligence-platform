# warehouse/

Destination configuration and modelled-table builders. DuckDB is the raw lake, ClickHouse is the analytical DWH (see [ADR 0001](../docs/architecture/adr/0001-clickhouse-as-dwh.md)).

Owned by Nadi (Jack on schema for modelled tables).

## Layout

```
warehouse/
├── __init__.py
├── destinations.py  # dlt destination configs: DuckDB (raw), ClickHouse (modelled)
├── models.py        # dataclass / Pydantic schemas for modelled tables
└── ddl.sql          # ClickHouse DDL for modelled tables
```

## Modelled tables (target)

- `article` — canonical article row, deduped.
- `country_topic_daily` — articles per country per topic per day.
- `narrative_divergence` — per topic, divergence metric across countries.
- `top_outlets_per_topic` — leaderboard view for the dashboard.

## Conventions

- DDL lives in `ddl.sql`. Migrations are append-only (no drop-and-recreate in prod).
- The dlt pipeline owns the raw lake schema. ClickHouse models are built by `pipelines/build_warehouse.py`.
