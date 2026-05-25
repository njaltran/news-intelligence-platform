"""Entry point for post-ingestion processing.

Run from the repo root:

    PYTHONPATH=. uv run python pipelines/process.py

Currently wires only the cleaning step. Embeddings, topic modelling,
and divergence will plug in here as they ship.
"""

from processing import clean


def run() -> None:
    counts = clean.run()
    print(counts)  # noqa: T201


if __name__ == "__main__":
    run()
