# Architecture Decision Records

Per-decision short docs in the [Michael Nygard ADR](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions) format. One file per decision. Numbered sequentially.

## Format

```markdown
# NNNN. Title

Date: YYYY-MM-DD
Status: proposed | accepted | superseded by [NNNN](./NNNN-other.md) | deprecated

## Context

What is the issue that we are facing?

## Decision

What we chose to do.

## Consequences

What becomes easier or harder because of this decision?
```

## Index

- [0001 — ClickHouse as analytical DWH](0001-clickhouse-as-dwh.md)
- [0002 — dlt as ingestion framework](0002-dlt-as-ingestion.md)
