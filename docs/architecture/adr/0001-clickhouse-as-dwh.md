# 0001. ClickHouse as analytical DWH

Date: 2026-05-25
Status: accepted

## Context

The dashboard needs sub-second aggregations over the modelled tables (country x topic x time windows). DuckDB serves the raw lake well but is single-process and not designed to back a concurrent dashboard. Postgres handles concurrency but is row-oriented and slow for the aggregation patterns we need. The course rubric rewards a choice that names a clear architectural style.

## Decision

Use **ClickHouse** as the analytical data warehouse. DuckDB stays as the local raw lake; ClickHouse holds the modelled tables that back the marimo dashboard.

## Consequences

**Easier**:
- Sub-second aggregation across country, topic, and time at our scale (~300k records).
- Maps cleanly to course concepts: ClickHouse is a [[Columnar Database]] with [[MPP]] execution, falls into Watson 2014's data-warehouse generation.
- dlt has a native ClickHouse destination, so no custom connector code.

**Harder**:
- One more runtime to operate (docker-compose).
- Local-only by default; need a deployment view for the report (where would this run in production?).

## Alternatives considered

- **DuckDB only**: simpler ops, but single-process. Adequate for a static report but not for a live dashboard demo with multiple viewers.
- **Postgres**: handles concurrency, but row-oriented storage. Aggregation queries that scan millions of rows would be slow without heavy indexing or materialised views.
