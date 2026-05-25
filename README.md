# News Intelligence Platform

> Real-time intelligence system that collects multilingual news from five countries, classifies topic and sentiment, and visualises how each country frames the same global events.

Team project for *Enterprise Architectures for Big Data* (Prof. Dr. Roland M. Mueller, HWR Berlin). The goal is to break away from mindless scrolling by surfacing **narrative divergence** in a live dashboard.

## Why this project

- **Long Tail framing.** Most cross-country news projects compare EU and US sources. We start in Germany, USA, Italy, **Myanmar, and Kazakhstan**. The last two are the differentiation: they sit on the Long Tail of global news coverage and are exactly where the "more data beats smarter algorithms" argument bites.
- **Architecture, not scale.** The course rubric asks for an enterprise architecture lens, not big-data benchmarks. The interesting question is how to wire ingestion, storage, ML, and serving so the system can grow from a 1.5 GB raw corpus into a streaming-first deployment without rewrites.
- **Stream Processing** is the chosen course technology, realised here as continuous ingestion pipelines.

## Architecture

```
Sources (NewsAPI, GDELT, RSS, scraping)
    -> Ingestion (dlt + BeautifulSoup)
    -> Raw lake (DuckDB)
    -> Cleaning + dedup
    -> Embeddings (multilingual) + Topic modelling (BERTopic / LDA)
    -> ClickHouse (analytical DWH, columnar / MPP)
    -> marimo dashboard (narrative divergence view)
```

**Storage split.** DuckDB is the local raw lake: cheap, embedded, dev-friendly. ClickHouse is the analytical warehouse: columnar with MPP execution, sized for sub-second aggregations over the modelled tables (country x topic x time) that back the dashboard.

## Stack

| Layer | Tool | Why |
|-------|------|-----|
| Ingestion | [dlt](https://dlthub.com) + BeautifulSoup | dlt for REST APIs (NewsAPI, GDELT), BeautifulSoup for RSS and scraping |
| Raw lake | [DuckDB](https://duckdb.org) | embedded, fast over local files, zero-ops |
| Analytical DWH | [ClickHouse](https://clickhouse.com) | columnar, MPP, sub-second aggregations for dashboard |
| Embeddings | [Sentence Transformers](https://www.sbert.net) | multilingual, sentence-level vectors |
| Topic modelling | [BERTopic](https://maartengr.github.io/BERTopic/) or LDA | per-language topic clusters |
| Dashboard | [marimo](https://marimo.io) | reactive Python notebook, no template engine |

## Schema (post-processing)

```
source           text     publisher / outlet identifier
country_target   text     country of the outlet (DE, US, IT, MM, KZ)
title            text     article title
summary          text     short summary or lede
url              text     canonical article URL
published_at     ts       article publication time
extracted_at     ts       ingestion time (for incremental loading)
```

## Scale estimate (6-week window)

- 126,000 to 294,000 records
- 2 to 5 KB text per record
- 300 MB to 1.5 GB raw text plus embedding vectors

In course vocabulary: **Volume** = small, **Velocity** = medium (steady ingestion), **Variety** = high (five countries, many outlets, multiple languages).

## Team

| Role | Owner | Owns |
|------|-------|------|
| Enterprise Architecture | Jack | architecture diagrams, EA hierarchy, IEEE 1471 viewpoints, course-concept framing |
| Data Engineering | Nadi | ingestion pipelines, streaming, DWH modelling |
| Business | Karina | pitch, value framing, stakeholder narrative |

## Getting started

```bash
git clone https://github.com/njaltran/news-intelligence-platform.git
cd news-intelligence-platform

# create venv
uv venv
uv pip install -r requirements.txt

# configure dlt secrets
# edit .dlt/secrets.toml with NewsAPI key, GDELT credentials, ClickHouse password

# run ingestion
uv run python rest_api_pipeline.py

# launch dashboard
uv run marimo edit gdelt_dashboard.py
```

dlt resolves `.dlt/secrets.toml` and configs from cwd, so all commands run from the repo root.

## Files

- `rest_api_pipeline.py`: dlt ingestion pipeline (NewsAPI, GDELT, RSS).
- `gdelt_dashboard.py`: marimo dashboard.
- `gdelt.duckdb`: local DuckDB warehouse (gitignored, rebuildable).
- `2026-05-08_gdelt_analysis_plan.md`: analysis plan and chart specs.
- `pitch-email.md`, `Pitch_Deck_-_Additional_Slides.pptx`: pitch artifacts.
- `must-read.md`: project briefing.
- `requirements.txt`: Python deps.
- `.dlt/`: dlt workspace (secrets gitignored).

## Open questions

- Topic modelling: BERTopic or LDA? BERTopic wins on multilingual embedding integration; LDA wins on explainability for the report.
- Embedding model for cross-language similarity. `paraphrase-multilingual-MiniLM-L12-v2` is the candidate.
- Quantifying narrative divergence beyond raw sentiment (entity framing, headline polarity, attention asymmetry).
- Entity linking across languages.

## EA framing (course tie-in)

- **5 Vs**: Variety dominates (multi-language, multi-outlet). Velocity drives the ClickHouse choice. Value is the dashboard.
- **Lambda vs Kappa**: current shape is closer to Kappa (continuous ingestion, replayable from raw lake). Lambda branch would split if batch enrichment runs on a different cadence than streaming ingestion.
- **Columnar / MPP**: ClickHouse maps to the [[Columnar Database]] and [[MPP]] concepts in the vault and to Watson 2014's data-warehouse generation.
- **Long Tail**: Myanmar and Kazakhstan are the differentiation, see Halevy/Norvig/Pereira 2009.
