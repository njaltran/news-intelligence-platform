"""Entry point for API-based ingestion (NewsAPI + GDELT).

Run from the repo root so dlt resolves .dlt/secrets.toml from cwd:

    uv run python pipelines/ingest_apis.py
"""

import dlt

from sources.gdelt import gdelt_source


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="gdelt",
        destination="duckdb",
        dataset_name="gdelt_data",
        dev_mode=True,
    )
    load_info = pipeline.run(gdelt_source())
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    run()
