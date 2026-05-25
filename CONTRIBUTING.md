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

```
.
├── docs/                   # team plan, architecture notes, ADRs
├── data/                   # raw extracts, ground truth, source configs (large files via shared drive)
├── .dlt/                   # dlt workspace (secrets.toml gitignored)
├── rest_api_pipeline.py    # ingestion entry point
├── gdelt_dashboard.py      # marimo dashboard
└── requirements.txt
```

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
