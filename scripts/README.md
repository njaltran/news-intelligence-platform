# scripts/

One-off CLI helpers. Not part of the pipeline graph. Use for diagnostics, migrations, source validation.

## Examples

- `validate_sources.py`: walk `data/config/sources.yaml`, check each URL is alive, fetch and report `robots.txt` policy, flag JS-rendered pages.
- `dump_schema.py`: dump current DuckDB or ClickHouse schema to a file for review.
- `seed_ground_truth.py`: one-time loader for `data/ground_truth/`.

## Conventions

- Each script is standalone, runnable as `python scripts/<name>.py`.
- Use `argparse` for flags. No external CLI framework needed.
- Read-only by default; mutations require a `--apply` flag.
