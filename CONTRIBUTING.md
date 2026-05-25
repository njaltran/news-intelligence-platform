# Contributing

Team project workflow. Keep it light.

## Branching

- `main` is protected. **No direct pushes.** All changes land via PR.
- Branch naming: `<initials>/<short-thing>`. Examples: `jack/mm-scraper`, `nadi/clickhouse-setup`, `karina/sources-yaml`.
- Open a PR early (draft is fine), iterate, mark ready when done.

## PR description

Each PR includes:

1. **What changed** (1-2 sentences).
2. **EA lens** (one line): which course concept does this advance? Examples: "adds Long Tail coverage (MM)", "implements the ClickHouse/MPP storage choice", "lifts Velocity ceiling by adding incremental loading".
3. **How to test** (commands to run, or "n/a" for docs-only).

## Commit messages

- Subject line in imperative mood, under 70 chars.
- Body explains *why*, not *what* (the diff shows what).
- Conventional Commits prefixes welcome but not required: `feat:`, `fix:`, `docs:`, `chore:`.

## Code conventions

- Python: black-compatible formatting (no enforced linter yet).
- dlt: keep resources in `rest_api_pipeline.py` until it gets too big. Split per source once it crosses ~300 lines.
- No secrets in code. Use `dlt.secrets[...]` and `.dlt/secrets.toml` (gitignored).
- Pin library versions in `requirements.txt`.

## Folder layout

Full tree in [README.md](README.md#repository-layout). Rationale and migration plan in [`docs/architecture/06-code-layout.md`](docs/architecture/06-code-layout.md). Headline:

- `sources/` — dlt sources, one file per data source. Scrapers in `sources/scrapers/<country>/`.
- `processing/` — post-ingestion transforms (clean, embed, topic, divergence).
- `warehouse/` — DuckDB raw + ClickHouse modelled destinations, DDL, schemas.
- `dashboard/` — marimo notebook.
- `pipelines/` — thin entry points wiring sources to destinations.
- `scripts/` — one-off CLI helpers.
- `tests/` — pytest, mirrors source layout.
- `data/` — extracts, ground truth, configs. Large files via shared drive.
- `docs/` — plan, briefing, pitch, and `docs/architecture/` (EA artifacts + ADRs).
- `.dlt/` — dlt workspace (`secrets.toml` is gitignored).
- `.github/pull_request_template.md` — PR template.

## Working directory

All dlt commands run from the repo root:

```bash
uv run python rest_api_pipeline.py
uv run dlt --non-interactive pipeline gdelt info
```

## Reviewing

- One reviewer is enough.
- Block merge on red CI (when CI exists).
- Squash-merge is the default. Keeps `main` history readable.
