import time
from typing import Any, Iterator

import dlt
import requests
from dlt.common.pendulum import pendulum

GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_MIN_WINDOW_MINUTES = 60
GDELT_RATE_LIMIT_SECONDS = 10


def _gdelt_get(params: dict) -> dict:
    """GET with exponential backoff on 429 + connection errors. GDELT throttles
    >1 req per ~5s and stays sticky for a while after bursts."""
    backoff = 30
    last_err: Exception | None = None
    for _ in range(6):
        try:
            resp = requests.get(GDELT_DOC_URL, params=params, timeout=30)
        except requests.exceptions.RequestException as exc:
            last_err = exc
            time.sleep(backoff)
            backoff = min(backoff * 2, 240)
            continue
        if resp.status_code == 429:
            time.sleep(backoff)
            backoff = min(backoff * 2, 240)
            continue
        resp.raise_for_status()
        if "json" not in resp.headers.get("content-type", ""):
            raise RuntimeError(
                f"GDELT non-JSON response: {resp.text[:200]} for {params}"
            )
        return resp.json()
    raise RuntimeError(f"GDELT failed after retries for {params}: {last_err}")


@dlt.resource(name="articles", primary_key="url", write_disposition="merge")
def gdelt_articles(
    query: str = dlt.config.value,
    start: str = None,
    end: str = None,
    chunk_minutes: int = dlt.config.value,
    maxrecords: int = 250,
) -> Iterator[list[dict[str, Any]]]:
    """GDELT 2.0 DOC ArtList, chunked by time window to bypass 250-row cap.

    GDELT enforces 60min minimum window and ~1 req per 5s rate limit.

    Args:
        query: GDELT query string.
        start: ISO8601 UTC start (e.g. "2026-04-29T00:00:00Z"). Default: 24h ago.
        end: ISO8601 UTC end. Default: now.
        chunk_minutes: Window size per request. Min 60.
        maxrecords: Max articles per request (GDELT cap = 250).
    """
    if chunk_minutes < GDELT_MIN_WINDOW_MINUTES:
        chunk_minutes = GDELT_MIN_WINDOW_MINUTES

    end_dt = pendulum.parse(end) if end else pendulum.now("UTC")
    start_dt = pendulum.parse(start) if start else end_dt.subtract(days=1)

    cursor = start_dt
    while cursor < end_dt:
        window_end = min(cursor.add(minutes=chunk_minutes), end_dt)
        if (window_end - cursor).total_minutes() < GDELT_MIN_WINDOW_MINUTES:
            break

        time.sleep(GDELT_RATE_LIMIT_SECONDS)

        params = {
            "query": query,
            "mode": "ArtList",
            "format": "json",
            "startdatetime": cursor.format("YYYYMMDDHHmmss"),
            "enddatetime": window_end.format("YYYYMMDDHHmmss"),
            "maxrecords": maxrecords,
            "sort": "datedesc",
        }
        articles = _gdelt_get(params).get("articles", [])
        if articles:
            yield articles
        cursor = window_end


@dlt.source(name="gdelt")
def gdelt_source(
    query: str = dlt.config.value,
    start: str = None,
    end: str = None,
    chunk_minutes: int = dlt.config.value,
) -> Any:
    """GDELT 2.0 source.

    Examples:
        pipeline.run(gdelt_source())
        pipeline.run(gdelt_source(query="opentelemetry", start="2026-04-23T00:00:00Z"))
    """
    yield gdelt_articles(
        query=query,
        start=start,
        end=end,
        chunk_minutes=chunk_minutes,
    )
