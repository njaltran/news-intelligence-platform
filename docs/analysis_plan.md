# Analysis Plan: gdelt

**Date:** 2026-05-08
**Pipeline:** `gdelt`
**Dataset:** default (DuckDB)
**Destination:** DuckDB (`gdelt.duckdb`)

> **Note:** **Myanmar and Kazakhstan are absent** from this snapshot (0 articles in the 3h window 2026-04-29 12:00–15:15 UTC). The remaining focus countries — Germany, United States, Italy — are charted below. To include Myanmar and Kazakhstan, re-run the pipeline with a wider date range or query GDELT directly for those countries.

## Connection

```python
import dlt
pipeline = dlt.attach("gdelt")
dataset = pipeline.dataset()
```

## Profile Summary

| Table | Rows | Time range |
| --- | --- | --- |
| `articles` | 580 | 2026-04-29 12:00 → 15:15 UTC (3h snapshot) |

Cardinality: 38 languages, 77 countries, 449 domains.

## Country focus

Charts are filtered to a small comparison set:

| Country | Articles in snapshot |
| --- | --- |
| United States | 108 |
| Germany | 50 |
| Italy | 46 |
| Myanmar | 0 (absent) |
| Kazakhstan | 0 (absent) |

Myanmar and Kazakhstan have no articles in this 3h window — to include them, re-run the pipeline with a wider date range or query the GDELT API directly for those countries.

## Project context

This dashboard prototypes a **global news intelligence platform**: a real-time system that collects, classifies, and visualizes news from multiple countries to expose how each country reports on the same global events.

**Current stage:** raw extraction from a *single* API (GDELT). One snapshot, 580 records, no processing layer yet. Schema is the GDELT-native shape (`url, title, seendate, domain, language, sourcecountry`). Charts here serve as a baseline view of what one ingestion source surfaces and what we can already say about cross-country coverage.

**Target schema** once additional sources (more APIs, RSS, web scraping with BeautifulSoup) and processing (normalization, summarization, classification) land:

| Column | Notes |
| --- | --- |
| `source` | publisher domain or feed |
| `country_target` | which country the *story* is about (vs `sourcecountry` = where it is published) |
| `title` | headline |
| `summary` | LLM-generated abstract |
| `url`, `published_at`, `extracted_at` | provenance + timing |

**Volume target:** 126k–294k records over the 6-week project (≈300 MB–1.5 GB raw text + embedding vectors). Chart 9 below extrapolates the current GDELT rate to compare against this band.

## Questions

- [x] Top domains by country (filtered to focus set)
- [x] Language mix in focus countries
- [x] Publishing pace per country across the snapshot window
- [x] Title length distribution per country
- [x] Mobile-friendly URL share per country
- [x] Top headline keywords per country (cross-narrative comparison)
- [x] Cumulative article volume across the snapshot
- [x] Domain concentration (CR3) and unique-domain count per country
- [x] Projected 6-week volume per country (capacity planning)

## Data Gaps

- Myanmar and Kazakhstan absent in current 3h snapshot.
- For deeper analysis: GDELT GKG fields (themes, sentiment, persons, locations) and a wider time window.

---

## Chart 1: Top domains by country (focus set)

**Question:** Which publishers dominate coverage in the focus countries?
**Type:** stacked bar
**X:** `sourcecountry`
**Y:** count of articles
**Color:** `domain` (top 5 per country, rest = `other`)

### SQL

```sql
WITH base AS (
  SELECT sourcecountry, domain
  FROM articles
  WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
),
ranked AS (
  SELECT sourcecountry, domain, COUNT(*) AS n,
    ROW_NUMBER() OVER (PARTITION BY sourcecountry ORDER BY COUNT(*) DESC) AS rk
  FROM base
  GROUP BY 1, 2
)
SELECT sourcecountry,
  CASE WHEN rk <= 5 THEN domain ELSE 'other' END AS domain,
  SUM(n) AS articles
FROM ranked
GROUP BY 1, 2
ORDER BY sourcecountry, articles DESC
```

## Chart 2: Language mix in focus countries

**Question:** What languages does each focus country publish in?
**Type:** heatmap
**X:** `sourcecountry`
**Y:** `language`
**Color:** count

### SQL

```sql
SELECT sourcecountry, language, COUNT(*) AS articles
FROM articles
WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
GROUP BY 1, 2
ORDER BY articles DESC
```

## Chart 3: Publishing pace within snapshot

**Question:** How does article volume change across the 3-hour window for each country?
**Type:** line chart
**X:** 15-min `bucket`
**Y:** count
**Color:** `sourcecountry`

### SQL

```sql
SELECT sourcecountry,
  date_trunc('hour', seendate)
    + INTERVAL (FLOOR(EXTRACT(MINUTE FROM seendate)/15)*15) MINUTE AS bucket,
  COUNT(*) AS articles
FROM articles
WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
GROUP BY 1, 2
ORDER BY 2, 1
```

## Chart 4: Title length distribution

**Question:** Do publishers in different countries write longer headlines?
**Type:** boxplot
**X:** `sourcecountry`
**Y:** `LENGTH(title)`

### SQL

```sql
SELECT sourcecountry, LENGTH(title) AS title_len
FROM articles
WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
  AND title IS NOT NULL
```

## Chart 6: Top headline keywords per country

**Question:** What words dominate headlines in each country? (Reveals divergent framing of shared events.)
**Type:** faceted bar chart (one column per country)
**Source:** `articles.title`

### SQL

```sql
WITH toks AS (
  SELECT sourcecountry,
    LOWER(unnest(regexp_split_to_array(title, '[^[:alnum:]]+'))) AS token
  FROM articles
  WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
    AND title IS NOT NULL
)
SELECT sourcecountry, token, COUNT(*) AS n
FROM toks
WHERE LENGTH(token) >= 5
  AND token NOT IN ('about','after','their','these','those','which','where','there','would','could','should','being','until','while','among','other','first','years','today','sagte','wurde','haben','dieser','einer','sopra','dopo','prima','dalla','contro','degli')
GROUP BY 1, 2
QUALIFY ROW_NUMBER() OVER (PARTITION BY sourcecountry ORDER BY COUNT(*) DESC) <= 8
ORDER BY sourcecountry, n DESC
```

## Chart 7: Cumulative article volume

**Question:** How does coverage accumulate across the snapshot window per country?
**Type:** area chart
**X:** `seendate` (15-min)
**Y:** running sum of articles
**Color:** `sourcecountry`

### SQL

```sql
SELECT sourcecountry, bucket,
  SUM(articles) OVER (PARTITION BY sourcecountry ORDER BY bucket) AS cumulative
FROM (
  SELECT sourcecountry,
    date_trunc('hour', seendate)
      + INTERVAL (FLOOR(EXTRACT(MINUTE FROM seendate)/15)*15) MINUTE AS bucket,
    COUNT(*) AS articles
  FROM articles
  WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
  GROUP BY 1, 2
)
ORDER BY bucket, sourcecountry
```

## Chart 8: Domain concentration (CR3) per country

**Question:** How concentrated is the media landscape per country? Top-3 publishers' share + unique domain count signal media plurality.
**Type:** combo bar (CR3 %) with unique-domain count as text label
**Source:** `articles`

### SQL

```sql
WITH base AS (
  SELECT sourcecountry, COUNT(*) AS total,
         COUNT(DISTINCT domain) AS unique_domains
  FROM articles
  WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
  GROUP BY 1
),
top3 AS (
  SELECT sourcecountry, SUM(n) AS top3_n FROM (
    SELECT sourcecountry, domain, COUNT(*) AS n,
      ROW_NUMBER() OVER (PARTITION BY sourcecountry ORDER BY COUNT(*) DESC) AS rk
    FROM articles
    WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
    GROUP BY 1, 2
  ) WHERE rk <= 3 GROUP BY 1
)
SELECT b.sourcecountry, b.total, b.unique_domains,
       ROUND(100.0 * t.top3_n / b.total, 1) AS top3_share_pct
FROM base b
JOIN top3 t USING (sourcecountry)
ORDER BY top3_share_pct DESC
```

## Chart 9: Projected 6-week volume

**Question:** If we extrapolate today's ingestion rate, where do we land on the 126k–294k 6-week target?
**Type:** bar chart with reference rules at 126k and 294k
**Source:** `articles` + project parameters (24h × 42d)

### SQL

```sql
WITH span AS (
  SELECT EPOCH(MAX(seendate) - MIN(seendate)) / 3600.0 AS hours
  FROM articles
)
SELECT a.sourcecountry,
  COUNT(*) AS snapshot_articles,
  ROUND(COUNT(*) / s.hours, 2) AS per_hour,
  CAST(ROUND(COUNT(*) / s.hours * 24 * 42) AS BIGINT) AS projected_6w
FROM articles a, span s
WHERE a.sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
GROUP BY a.sourcecountry, s.hours
ORDER BY projected_6w DESC
```

---

## Chart 5: Mobile-friendly URL share

**Question:** What share of articles publish a mobile-specific URL per country?
**Type:** bar chart
**X:** `sourcecountry`
**Y:** mobile share (%)

### SQL

```sql
SELECT sourcecountry,
  COUNT(*) AS total,
  SUM(CASE WHEN url_mobile IS NOT NULL AND url_mobile <> '' THEN 1 ELSE 0 END) AS with_mobile,
  ROUND(100.0 * SUM(CASE WHEN url_mobile IS NOT NULL AND url_mobile <> '' THEN 1 ELSE 0 END)
        / COUNT(*), 1) AS mobile_pct
FROM articles
WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
GROUP BY 1
ORDER BY mobile_pct DESC
```
