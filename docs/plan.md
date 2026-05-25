# News Intelligence Platform: 6-week plan

Team plan for the *Enterprise Architectures for Big Data* project. Owners are Jack (Enterprise Architecture **and second extraction engineer**), Nadi (Data Engineering lead), Karina (Business).

**Note on Jack's role.** Jack is the EA specialist, but also helps with data extraction. The natural split: Jack owns the **RSS / scraping path** (BeautifulSoup) and the **Myanmar and Kazakhstan outlets** specifically. Both fit the EA narrative: the data viewpoint (IEEE 1471) and the Long Tail thesis. Nadi owns the API path (NewsAPI, GDELT, anything dlt-native) and the overall pipeline shape.

- **Start**: 2026-05-25
- **Finish (prototype + report submission)**: ~2026-07-06
- **Assessment**: Report 36%, Presentation 30%, Oral Exam 24%, Assignments 10%

## Current status

Nadi and Karina have already started extracting data (initial pull). The corpus is far below the target volume. The plan treats **data extraction as an ongoing workstream across Weeks 1 to 3**, not a one-off Week 1 task. The Week 1 deliverable is consolidating what exists and **agreeing on the extraction targets** that the next two weeks will hit.

### Extraction targets (working numbers, refine in Week 1)

| Dimension | Initial pull (now) | Week 3 target | Stretch |
|-----------|--------------------|---------------|---------|
| Records | <existing> | 126k to 294k (CLAUDE.md baseline) | 500k+ |
| Outlets per country | <existing> | ~5 (25 total) | 8 to 10 (40 to 50 total) |
| Time window | <existing> | 6 weeks rolling | 6 months historical (GDELT) |
| Coverage per country | <existing> | DE / US / IT / MM / KZ all live | + 1 to 2 backup outlets per country |

Update the `<existing>` row in Week 1 once Nadi reports current counts.

## Week 1 (now to 2026-06-01): Consolidate existing extracts, set repo conventions, plan the bigger pull

**Goal**: get everyone's existing work into the repo on a shared standard, and lock in **what "much more" means quantitatively** so Nadi can execute against a clear target.

### Repo conventions (everyone, one-time setup)

- Branching: feature branches, PR into `main`. No direct pushes to `main`.
- Branch naming: `nadi/<thing>`, `karina/<thing>`, `jack/<thing>`.
- Each PR includes: what changed, EA-lens justification (one line), how to test.
- Folder structure already scaffolded (see [README.md](../README.md#repository-layout) for the full tree):

```
data/
  raw/             # untouched extracts (Nadi's dumps land here)
  interim/         # cleaned, dedup'd
  ground_truth/    # Karina's annotations
  config/
    sources.yaml   # outlets per country
docs/
  plan.md          # this file
  briefing.md
  analysis_plan.md
  pitch/           # pitch email + slide deck
  architecture/    # context, viewpoints, ADRs
```

- `*.csv`, `*.parquet`, `*.json` larger than ~1 MB go to a shared drive or Git LFS. Small samples in repo, big files out.

### Nadi (Data Engineering): consolidate existing extract, plan the scaled pull

- Drop existing extracts into `data/raw/` with a `README.md` describing source, date range, row count, schema.
- Fill the `<existing>` column of the Extraction Targets table above with real numbers.
- Compare existing schema to the documented one (`source, country_target, title, summary, url, published_at, extracted_at`). Note gaps and fields to add.
- If data is already in a DuckDB file, copy it to a shared location and point a one-off ingestion script at it that re-writes through dlt, so the path is reproducible. `rest_api_pipeline.py` is the home for that script.
- **Extraction plan**: list the APIs and outlets needed to hit the Week 3 target. For each: rate limit, free-tier quota, auth method, and estimated time to fill the 6-week window.
- Identify the bottleneck source (NewsAPI quota? GDELT BigQuery cost? RSS coverage in MM/KZ?) and propose a workaround.

### Karina (Business): onboard sources, annotations, pitch material

- `data/config/sources.yaml`: outlets per country (DE, US, IT, MM, KZ) with RSS URLs where available.
- `data/ground_truth/`: hand-coded examples of narrative divergence (same event, different framing). 5 to 10 to start. Becomes the evaluation set later.
- `docs/pitch/`: refined pitch and stakeholder map. Consolidates `pitch-email.md` and the pptx.

### Jack (Enterprise Architecture + RSS / MM + KZ extraction): EA scaffolding + start the Long Tail pull

**EA scaffolding**
- `docs/architecture/`:
  - `00-context.md`: business problem, scope, out-of-scope.
  - `01-ea-hierarchy.md`: mission to process to data to application to technology, mapped to this project.
  - `02-viewpoints.md`: IEEE 1471 viewpoints (logical, physical, deployment, security, data).
  - `adr/0001-clickhouse-as-dwh.md`: ADR for why ClickHouse over Postgres or DuckDB-as-server.
  - `adr/0002-dlt-as-ingestion.md`: ADR for why dlt.
- Architecture diagram per viewpoint (mermaid in markdown, or PNG export).

**Extraction (RSS + MM/KZ scrape)**
- Pair with Karina on MM and KZ outlet discovery. Languages: Burmese, Russian (KZ media), Kazakh. Note encoding and font quirks early.
- Write the first BeautifulSoup-based scraper as a dlt resource for one MM outlet end-to-end (proof of concept). Goal by end of Week 1: 1 MM outlet flowing into `data/raw/`.
- Document the scraping pattern in `docs/architecture/05-scraping-pattern.md` so it can be copied for the other 4 outlets.

## Week 2 (2026-06-01 to 06-08): Pipeline + storage spine + bulk extraction

### Nadi
- `rest_api_pipeline.py`: add NewsAPI, GDELT, RSS resources. Each writes to DuckDB raw lake first.
- **Bulk extraction**: kick off the scaled pull (target ~50% of Week 3 target by end of Week 2). Use dlt's `dlt.sources.incremental` so reruns are cheap.
- Stand up ClickHouse locally (`docker-compose.yml` at the repo root). Single-node is fine.
- Add a dlt destination for ClickHouse. Pattern: raw to DuckDB, modelled aggregates to ClickHouse.
- Document secrets needed in `.dlt/secrets.toml` (NewsAPI key, GDELT, ClickHouse password).

### Karina
- Expand `sources.yaml` to ~5 outlets per country (target ~25 outlets total).
- Annotate ~20 narrative-divergence examples for evaluation.

### Jack
- **Extraction**: extend the MM proof-of-concept to all 5 MM outlets and start on KZ. Target by end of Week 2: 3 MM + 2 KZ outlets flowing.
- **Schema design**: own the modelled-table schema in ClickHouse (article, country_topic_daily, narrative_divergence). Cross-cut role: EA data viewpoint meets data engineering.
- 5 Vs profile of the project: `docs/architecture/03-5vs.md` quantifying V/V/V/V/V for this project.
- Lambda vs Kappa positioning: `docs/architecture/04-streaming-shape.md` justifying current (Kappa-ish) shape and the trigger for adding a batch lane.
- Review Nadi's pipeline PRs through the EA lens.

## Week 3 (06-08 to 06-15): Cleaning, dedup, embeddings, extraction at target

### Nadi
- **Extraction at target volume**: full Week 3 target hit by end of week (see Extraction Targets table). Lock the corpus.
- Cleaning and dedup transform (in dlt or a follow-up SQL step in DuckDB). Dedup is critical at scale: GDELT + RSS overlap.
- Multilingual embeddings (`paraphrase-multilingual-MiniLM-L12-v2` or similar). Store vectors next to article rows.
- Topic modelling: BERTopic on a 1k-row sample. Note multilingual gotchas.

### Karina
- Finalise ~50 evaluation examples.
- Define "narrative divergence" quantitatively: sentiment delta, framing-axis classifier, attention asymmetry, or a mix. Pick the metric the dashboard will show.

### Jack
- **Extraction**: all 10 MM + KZ outlets flowing. Hit Week 3 target for those two countries.
- ADR `0003-bertopic-vs-lda.md` once Nadi has results.
- Start report skeleton in `docs/report/` with sections sized to assessment weights.

## Week 4 (06-15 to 06-22): Dashboard prototype

### Nadi
- ClickHouse modelled tables: `country_topic_daily`, `narrative_divergence`, `top_outlets_per_topic`.
- Wire `dashboard/app.py` (marimo) to read from ClickHouse instead of DuckDB.

### Karina
- Walk-through script for the presentation: which 3 stories does the demo tell?
- Pitch-deck v2 against the working prototype.

### Jack
- Demo-day walkthrough from each EA viewpoint. Every viewpoint maps to a section of the report.
- Deployment view: where would this actually run (HWR VPS, docker-compose, cloud)? Single slide.

## Week 5 (06-22 to 06-29): Polish, evaluation, report draft

### Nadi
- Bug-fix, dedup quality, ClickHouse query latency.
- Run the pipeline against the full 6-week corpus.

### Karina
- Evaluation: precision and recall (or qualitative) against the ground-truth set.
- Final pitch polish.

### Jack
- Report draft (Report = 36% of grade, biggest piece). Karina's pitch as intro, Nadi's pipeline as engineering chapter, EA viewpoints as architecture chapter, evaluation as conclusion.
- Cross-reference vault concepts heavily.

## Week 6 (06-29 to 07-06): Presentation + report final

**All**
- Dry-run presentation twice.
- Report final pass: citations, figures, layout.
- Submit.

---

## Cross-cutting risks

| Risk | Mitigation |
|------|------------|
| Myanmar or Kazakhstan sources blocked or sparse | Backup outlets in `sources.yaml`. GDELT covers both as a safety net. |
| ClickHouse local setup eats a week | Start docker-compose on Day 1 of Week 2. Fall back to DuckDB serving if blocked. |
| Multilingual embedding quality on MM and KZ | Test early Week 3. If poor, fall back to translate-then-English embeddings. Lower fidelity but acceptable for prototype. |
| Direct push to `main` blocked | Document the PR flow in `CONTRIBUTING.md` so the team does not fight it. |

---

## Immediate next actions (this week)

1. **Jack**: scaffold `data/`, `docs/architecture/`, ADRs, and `CONTRIBUTING.md`. **Done** ✓.
2. **Nadi**: dump existing extracts into `data/raw/` on a branch with a `README.md` describing source / date range / row count / schema. **Fill in the `<existing>` row of the Extraction Targets table.** Open PR.
3. **Nadi + Jack**: agree on Week 3 extraction target numbers in the table above (working numbers are fine, lock them by end of Week 1). Agree on the API vs RSS / scrape split (Nadi = APIs, Jack = scraping + MM/KZ).
4. **Jack**: ship a proof-of-concept BeautifulSoup scraper for 1 MM outlet, wired as a dlt resource. Document the pattern.
5. **Karina + Jack**: outlet discovery for MM and KZ. Karina collects candidates, Jack assesses scrapability (robots.txt, JS-rendered, paywalls, encoding).
6. **Karina**: `sources.yaml` and `pitch.md` on a branch, including the MM / KZ shortlist. Open PR.
7. **All**: agree on weekly sync cadence (Monday short stand-up, Thursday demo).
