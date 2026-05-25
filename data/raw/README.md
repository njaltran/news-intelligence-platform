# data/raw

Untouched extracts straight from the source. **Do not commit large files** (over ~1 MB). Keep small samples here so anyone cloning the repo can run a smoke-test ingestion; keep the full corpus on a shared drive or Git LFS.

## Layout

Suggested file naming:

```
data/raw/
  newsapi/         # NewsAPI dumps (JSON)
  gdelt/           # GDELT extracts (CSV / Parquet)
  rss/             # RSS feed snapshots (XML / JSON)
  scraped/<country>/<outlet>/  # BeautifulSoup-scraped HTML or JSON
```

Each extract folder should have a `README.md` describing:

- Source URL or API endpoint
- Date range covered
- Row count
- Schema (column names + types)
- Extraction date
- Who extracted it
