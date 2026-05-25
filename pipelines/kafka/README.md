# pipelines/kafka/

Streaming ingestion path. Producers fetch news (NewsAPI, BBC, RSS + Google News, native-language scrapers), publish standardised messages to a single Kafka topic, a dlt consumer drains the topic into ClickHouse (or DuckDB for local debugging).

Owned by Nadi. Originally prototyped in [`nadikyaw/Enterprise_Architecture_BigData`](https://github.com/nadikyaw/Enterprise_Architecture_BigData), merged here on 2026-05-25.

## EA framing

- **Stream Processing** is the chosen course technology. This directory is the implementation.
- The broker is the **decoupling layer** between producers (variable cadence, can fail individually) and the consumer (durable, replayable). EA viewpoint: process viewpoint cleanly separated from data viewpoint.
- Shape: **Kappa** path. One topic is the system of record for streaming articles. The batch arm (`ingest_rss.py`, `ingest_apis.py`) coexists for outlets that only expose batch interfaces, which makes the overall architecture **Lambda**.
- 5 Vs: this path is where **Velocity** lives. **Variety** comes from the producer mix (NewsAPI top-headlines + 11 native-language outlets + BBC). **Volume** stays small.
- **Long Tail**: `producer_local_scrapers.py` is the Long Tail bet (Mizzima, Myanmar Now, The Irrawaddy, Tengrinews, Qazinform).

## Files

| File | Role |
|------|------|
| `producer_newsapi.py` | NewsAPI: top-headlines for US/DE/IT, `everything` keyword search for MM/KZ. |
| `producer_bbc.py` | BBC search results across all five countries (English-language outside view). |
| `producer_local_scrapers.py` | Hybrid HTML + RSS scraping across 11 native outlets. |
| `producer_rss.py` | Curated section feeds + Google News topic / search RSS. Reuses `sources/rss.py` + `sources/gnews.py`. ~40k messages per run. |
| `consumer_to_duckdb.py` | Kafka consumer + dlt pipeline. Lands every message in `news_articles` table in DuckDB. |
| `consumer_to_clickhouse.py` | Kafka consumer + dlt pipeline targeting the ClickHouse DWH (`news.news___articles`). Production path. |

All producers publish to topic `unified_news_topic` with the unified message schema (`source`, `country_target`, `title`, `url`, `summary`, `published_at`, `extracted_at`). dlt infers the table schema from these messages.

## Setup

1. **NewsAPI key.** Add to `.dlt/secrets.toml`:

   ```toml
   [sources.newsapi]
   api_key = "your-key-here"
   ```

2. **Bring up the broker** (Docker required):

   ```bash
   docker compose -f infra/docker-compose.yml up -d
   ```

3. **Install Python deps** (already in `requirements.txt`):

   ```bash
   uv pip install -r requirements.txt
   ```

## Run

Open two terminals. Consumer first so producers' early messages land:

```bash
# Terminal 1: consumer (stops after 15s of silence). Pick the
# destination you want; both consume the same topic.
PYTHONPATH=. uv run python pipelines/kafka/consumer_to_clickhouse.py
# or, for local debugging only:
PYTHONPATH=. uv run python pipelines/kafka/consumer_to_duckdb.py

# Terminal 2: any/all producers
PYTHONPATH=. uv run python pipelines/kafka/producer_rss.py
uv run python pipelines/kafka/producer_newsapi.py
uv run python pipelines/kafka/producer_bbc.py
uv run python pipelines/kafka/producer_local_scrapers.py
```

For the typical local dev loop (broker + ClickHouse + Streamlit dashboard + consumer + RSS producer in one terminal, merged log tail) use the helper script instead:

```bash
./scripts/dev_stack.sh           # bring everything up
./scripts/dev_stack.sh --down    # stop docker stack
```

Verify a clickhouse load:

```bash
docker exec infra-clickhouse-1 clickhouse-client --user news --password news \
  -q "SELECT count(), uniqExact(url) FROM news.news___articles"
```

Tear down:

```bash
docker compose -f infra/docker-compose.yml down
```

## Streaming cadence

The point of the Kappa path is that the dashboard sees rows trickle in, not arrive in one big burst.

- `producer_rss.py` is single-shot by default. Set `PRODUCER_INTERVAL_S=300` (or any positive value) to loop the sweep on that interval. Set `PRODUCER_MSG_DELAY_MS` to space individual messages out across a sweep instead of bursting.
- `consumer_to_clickhouse.py` consumes in micro-batches so dlt loads land incrementally. Tunables: `CONSUMER_BATCH_MAX` (default 500), `CONSUMER_BATCH_FLUSH_S` (default 5), `CONSUMER_IDLE_TIMEOUT_S` (default 15, `0` = run forever).
- Per-feed structured logging from the producer (`pipelines/kafka/_log.py`) lets `dev_stack.sh` interleave RSS, Google News, and producer lines into one merged view.

## Known limitations (from initial run, 2026-05-06)

- NewsAPI `top-headlines?country=de` and `country=it` return 0 articles. NewsAPI has been deprecating non-US `country=` queries. Workaround: switch DE/IT to the `everything` endpoint with country-name keyword search (same pattern already used for MM/KZ).
- Several HTML selectors are stale (ANSA, La Repubblica, Die Welt, NYT, CNN, NBC, Qazinform return 0). Re-derive selectors per outlet as a Week-3 task.
- `https://www.irrawaddy.com/` returns 403 against the desktop UA. Add an RSS fallback the same way Mizzima / Myanmar Now do.
- Producers should publish via the same `sources.yaml` registry that the batch RSS arm reads (`data/config/sources.yaml`) so outlets are configured in one place. Currently the outlet list is hardcoded in `producer_local_scrapers.py`.
