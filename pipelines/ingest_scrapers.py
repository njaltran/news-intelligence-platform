"""Entry point for BeautifulSoup-based scrapers.

Run from the repo root so dlt resolves .dlt/secrets.toml from cwd:

    PYTHONPATH=. uv run python pipelines/ingest_scrapers.py

Pattern. Each scraper module exposes a dlt resource named `articles`
(merge on `url`). This entry point wires all of them into one source
so they land in the same dataset as the RSS + API pipelines.
"""

import dlt

from sources.scrapers.mm.mizzima_burmese import mizzima_burmese


@dlt.source(name="scrapers")
def scrapers_source():
    yield mizzima_burmese()


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="scrapers",
        destination="duckdb",
        dataset_name="scrapers_raw",
        dev_mode=True,
    )
    load_info = pipeline.run(scrapers_source())
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    run()
