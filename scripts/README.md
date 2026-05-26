# scripts/

One-off CLI helpers. Not part of the pipeline graph. Use for diagnostics, migrations, source validation.

## Current scripts

- `dev_stack.sh`: one-command local dev loop. Brings up the docker stack (Kafka + ClickHouse), starts the Streamlit dashboard, the Kafka -> ClickHouse consumer, and the RSS + Google News producer in the background, then tails all three logs into one merged view with per-component prefixes. `Ctrl+C` tears the background processes down. `./scripts/dev_stack.sh --down` also stops the docker stack. Logs default to `/tmp/news-{producer,consumer,streamlit}.log`; override with `LOG_DIR=...`.

## Planned helpers

- `validate_sources.py`: walk `data/config/sources.yaml`, check each URL is alive, fetch and report `robots.txt` policy, flag JS-rendered pages.
- `dump_schema.py`: dump current DuckDB or ClickHouse schema to a file for review.
- `seed_ground_truth.py`: one-time loader for `data/ground_truth/`.

## Conventions

- Each script is standalone, runnable as `python scripts/<name>.py`.
- Use `argparse` for flags. No external CLI framework needed.
- Read-only by default; mutations require a `--apply` flag.
