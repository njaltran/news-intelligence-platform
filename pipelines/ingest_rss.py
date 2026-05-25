"""Entry point for RSS-based ingestion.

Run from the repo root so dlt resolves .dlt/secrets.toml from cwd:

    uv run python pipelines/ingest_rss.py
"""

import dlt

from sources.rss import rss_source


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rss",
        destination="duckdb",
        dataset_name="rss_raw",
        dev_mode=True,
    )
    load_info = pipeline.run(rss_source())
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    run()
