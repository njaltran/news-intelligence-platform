## What

<!-- 1-2 sentence summary of the change -->

## EA lens

<!-- One line. Which course concept does this advance?
Examples:
- "adds Long Tail coverage (MM outlets)"
- "implements ClickHouse/MPP storage layer"
- "lifts Velocity ceiling via incremental loading"
- "adds Data viewpoint per IEEE 1471" -->

## How to test

<!-- Commands to run, or "n/a" for docs-only changes -->

```bash
# example
uv run python pipelines/ingest_apis.py
```

## Checklist

- [ ] No secrets in the diff
- [ ] If schema changes: updated `docs/architecture/` and/or relevant ADR
- [ ] If new dependency: added to `requirements.txt` with pinned version
