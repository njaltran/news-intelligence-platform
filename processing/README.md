# processing/

Post-ingestion transforms. Each module takes one of the raw / interim tables and produces a derived table.

Owned by Nadi (schema cross-checked by Jack).

## Layout

```
processing/
├── __init__.py
├── clean.py         # dedup (URL canonicalisation), language detection, whitespace normalization
├── embeddings.py    # multilingual sentence-transformer pipeline; one vector per article
├── topics.py        # BERTopic or LDA per language; cluster IDs per article
└── divergence.py    # narrative-divergence metric across countries for the same topic cluster
```

## Conventions

- Each module exposes a single `run(input_table, output_table)` function.
- Idempotent: re-running with the same input produces the same output.
- No HTTP calls in this layer. Inputs are tables, outputs are tables.
- Use ibis or SQL against DuckDB for cheap transforms; only fall back to Pandas when the operation is row-by-row.
