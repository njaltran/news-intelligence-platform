# tests/

pytest test suite. Mirrors the source layout.

## Layout

```
tests/
├── test_sources.py       # source resource shapes, fixture-based
├── test_scrapers.py      # scrapers parse fixture HTML correctly
├── test_processing.py    # clean / embeddings / topics / divergence
├── test_warehouse.py     # DDL applies, modelled tables build
└── fixtures/             # sample HTML, JSON, small CSVs
```

## Conventions

- Unit tests are mock-free for data shapes: use real DuckDB in-memory.
- Network is mocked or recorded (vcrpy) for ingestion tests.
- Run: `uv run pytest`.
