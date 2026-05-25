# 0002. dlt as ingestion framework

Date: 2026-05-25
Status: accepted

## Context

The pipeline must pull from multiple heterogeneous sources (NewsAPI, GDELT, RSS feeds, scraped HTML) and land cleanly into DuckDB and ClickHouse. We need pagination, schema inference, incremental loading, and idempotent re-runs without hand-rolling each one.

## Decision

Use [**dlt**](https://dlthub.com) as the ingestion framework for all sources.

## Consequences

**Easier**:
- Schema inference and evolution come for free.
- Native destinations for both DuckDB and ClickHouse.
- `dlt.sources.incremental` handles cursor state, so reruns only pull deltas.
- REST API source covers NewsAPI and GDELT pagination patterns out of the box.
- BeautifulSoup scrapers wrap cleanly as `@dlt.resource` functions.

**Harder**:
- One more framework for the team to learn. Mitigated by Jack already being familiar.
- `.dlt/secrets.toml` is gitignored; team members must populate locally.

## Alternatives considered

- **Plain `requests` + `pandas`**: total control but every pipeline reinvents pagination, retries, and incremental loading.
- **Airbyte**: heavier infra, overkill for a 6-week prototype.
