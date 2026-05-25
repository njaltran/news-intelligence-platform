import marimo

__generated_with = "0.23.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import altair as alt
    import dlt

    return alt, dlt, mo


@app.cell
def _(mo):
    mo.md("""
    # Global News Intelligence — GDELT Baseline

    Real-time intelligence prototype: collect, classify, visualize news across countries to reveal divergent coverage of shared events.

    **Stage:** raw extraction from a single API (GDELT). 580 records, snapshot **2026-04-29, 12:00–15:15 UTC**.
    Future stages add RSS + web scraping, processing (normalization, summarization, classification) and the target schema (`source, country_target, title, summary, url, published_at, extracted_at`).

    Country focus: **Germany, United States, Italy, Myanmar, Kazakhstan**.
    *Myanmar and Kazakhstan are absent from this 3h GDELT window — re-run with a wider date range or add an alternate source for those countries.*
    """)
    return


@app.cell
def _(dlt):
    pipeline = dlt.attach("gdelt")
    dataset = pipeline.dataset()
    return (dataset,)


@app.cell
def _(mo):
    mo.md("""
    ## 1. Top domains by country
    """)
    return


@app.cell
def _(dataset):
    df_chart1 = dataset("""
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
    """).df()
    return (df_chart1,)


@app.cell
def _(alt, df_chart1):
    _chart = alt.Chart(df_chart1).mark_bar().encode(
        x=alt.X("sourcecountry:N", title="Source country"),
        y=alt.Y("articles:Q", title="Articles"),
        color=alt.Color("domain:N", title="Domain"),
        tooltip=["sourcecountry:N", "domain:N", "articles:Q"],
    ).properties(
        title="Top domains by country (focus set)",
        width=600,
        height=400,
    )
    _chart
    return


@app.cell
def _(mo):
    mo.md("""
    ## 2. Language mix
    """)
    return


@app.cell
def _(dataset):
    df_chart2 = dataset("""
        SELECT sourcecountry, language, COUNT(*) AS articles
        FROM articles
        WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
        GROUP BY 1, 2
        ORDER BY articles DESC
    """).df()
    return (df_chart2,)


@app.cell
def _(alt, df_chart2):
    _chart = alt.Chart(df_chart2).mark_rect().encode(
        x=alt.X("sourcecountry:N", title="Source country"),
        y=alt.Y("language:N", title="Language"),
        color=alt.Color("articles:Q", title="Articles", scale=alt.Scale(scheme="blues")),
        tooltip=["sourcecountry:N", "language:N", "articles:Q"],
    ).properties(
        title="Language mix per country",
        width=400,
        height=400,
    )
    _chart
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3. Publishing pace (15-min buckets)
    """)
    return


@app.cell
def _(dataset):
    df_chart3 = dataset("""
        SELECT sourcecountry,
          date_trunc('hour', seendate)
            + INTERVAL (FLOOR(EXTRACT(MINUTE FROM seendate)/15)*15) MINUTE AS bucket,
          COUNT(*) AS articles
        FROM articles
        WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
        GROUP BY 1, 2
        ORDER BY 2, 1
    """).df()
    return (df_chart3,)


@app.cell
def _(alt, df_chart3):
    _chart = alt.Chart(df_chart3).mark_line(point=True).encode(
        x=alt.X("bucket:T", title="15-min bucket (UTC)"),
        y=alt.Y("articles:Q", title="Articles"),
        color=alt.Color("sourcecountry:N", title="Country"),
        tooltip=["bucket:T", "sourcecountry:N", "articles:Q"],
    ).properties(
        title="Article volume per 15 min",
        width=700,
        height=350,
    )
    _chart
    return


@app.cell
def _(mo):
    mo.md("""
    ## 4. Title length distribution
    """)
    return


@app.cell
def _(dataset):
    df_chart4 = dataset("""
        SELECT sourcecountry, LENGTH(title) AS title_len
        FROM articles
        WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
          AND title IS NOT NULL
    """).df()
    return (df_chart4,)


@app.cell
def _(alt, df_chart4):
    _chart = alt.Chart(df_chart4).mark_boxplot(extent="min-max").encode(
        x=alt.X("sourcecountry:N", title="Source country"),
        y=alt.Y("title_len:Q", title="Title length (chars)"),
        color=alt.Color("sourcecountry:N", legend=None),
        tooltip=["sourcecountry:N", "title_len:Q"],
    ).properties(
        title="Headline length per country",
        width=500,
        height=350,
    )
    _chart
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5. Mobile-friendly URL share
    """)
    return


@app.cell
def _(dataset):
    df_chart5 = dataset("""
        SELECT sourcecountry,
          COUNT(*) AS total,
          SUM(CASE WHEN url_mobile IS NOT NULL AND url_mobile <> '' THEN 1 ELSE 0 END) AS with_mobile,
          ROUND(100.0 * SUM(CASE WHEN url_mobile IS NOT NULL AND url_mobile <> '' THEN 1 ELSE 0 END)
                / COUNT(*), 1) AS mobile_pct
        FROM articles
        WHERE sourcecountry IN ('Germany','United States','Italy','Myanmar','Kazakhstan')
        GROUP BY 1
        ORDER BY mobile_pct DESC
    """).df()
    return (df_chart5,)


@app.cell
def _(alt, df_chart5):
    _chart = alt.Chart(df_chart5).mark_bar().encode(
        x=alt.X("sourcecountry:N", sort="-y", title="Source country"),
        y=alt.Y("mobile_pct:Q", title="Mobile-URL share (%)"),
        color=alt.Color("sourcecountry:N", legend=None),
        tooltip=["sourcecountry:N", "total:Q", "with_mobile:Q", "mobile_pct:Q"],
    ).properties(
        title="Share of articles with mobile-specific URL",
        width=500,
        height=350,
    )
    _chart
    return


@app.cell
def _(mo):
    mo.md("""
    ## 6. Top headline keywords per country

    Reveals divergent framing — which words dominate each country's headlines for the same global news cycle.
    """)
    return


@app.cell
def _(dataset):
    df_chart6 = dataset("""
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
          AND token NOT IN ('about','after','their','these','those','which','where','there',
                            'would','could','should','being','until','while','among','other',
                            'first','years','today','sagte','wurde','haben','dieser','einer',
                            'sopra','dopo','prima','dalla','contro','degli')
        GROUP BY 1, 2
        QUALIFY ROW_NUMBER() OVER (PARTITION BY sourcecountry ORDER BY COUNT(*) DESC) <= 8
        ORDER BY sourcecountry, n DESC
    """).df()
    return (df_chart6,)


@app.cell
def _(alt, df_chart6):
    _chart = alt.Chart(df_chart6).mark_bar().encode(
        x=alt.X("n:Q", title="Mentions in headlines"),
        y=alt.Y("token:N", sort="-x", title=None),
        color=alt.Color("sourcecountry:N", legend=None),
        tooltip=["sourcecountry:N", "token:N", "n:Q"],
    ).properties(
        width=200,
        height=200,
    ).facet(
        column=alt.Column("sourcecountry:N", title=None),
    ).properties(
        title="Top 8 headline keywords per country",
    )
    _chart
    return


@app.cell
def _(mo):
    mo.md("""
    ## 7. Domain concentration (CR3) per country

    Top-3 publisher share + unique domain count. Higher CR3 = a few outlets dominate (lower media plurality). Hover reveals total articles and unique domains.
    """)
    return


@app.cell
def _(dataset):
    df_chart8 = dataset("""
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
    """).df()
    return (df_chart8,)


@app.cell
def _(alt, df_chart8):
    _bars = alt.Chart(df_chart8).mark_bar().encode(
        x=alt.X("sourcecountry:N", sort="-y", title="Source country"),
        y=alt.Y("top3_share_pct:Q", title="Top-3 publisher share (%)"),
        color=alt.Color("sourcecountry:N", legend=None),
        tooltip=[
            "sourcecountry:N",
            alt.Tooltip("total:Q", title="Articles"),
            alt.Tooltip("unique_domains:Q", title="Unique domains"),
            alt.Tooltip("top3_share_pct:Q", title="CR3 (%)"),
        ],
    )
    _labels = alt.Chart(df_chart8).mark_text(dy=-8, fontSize=12).encode(
        x=alt.X("sourcecountry:N", sort="-y"),
        y=alt.Y("top3_share_pct:Q"),
        text=alt.Text("unique_domains:Q", format="d"),
    )
    _chart = (_bars + _labels).properties(
        title="Media concentration: top-3 share % (label = unique domains)",
        width=500,
        height=350,
    )
    _chart
    return


@app.cell
def _(mo):
    mo.md("""
    ## 8. Projected 6-week volume vs target

    Extrapolating the current GDELT rate (snapshot per_hour × 24h × 42d). Reference rules at **126k** (low) and **294k** (high) of the project's stated 6-week target band — for the *full* multi-source platform across all five countries.
    """)
    return


@app.cell
def _(dataset):
    df_chart9 = dataset("""
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
    """).df()
    return (df_chart9,)


@app.cell
def _(alt, df_chart9):
    import pandas as _pd

    _bars = alt.Chart(df_chart9).mark_bar().encode(
        x=alt.X("sourcecountry:N", sort="-y", title="Source country"),
        y=alt.Y("projected_6w:Q", title="Projected articles over 6 weeks"),
        color=alt.Color("sourcecountry:N", legend=None),
        tooltip=[
            "sourcecountry:N",
            alt.Tooltip("snapshot_articles:Q", title="Snapshot count"),
            alt.Tooltip("per_hour:Q", title="Per hour"),
            alt.Tooltip("projected_6w:Q", title="6-week projection"),
        ],
    )
    _ref = alt.Chart(_pd.DataFrame({"y": [126000, 294000], "label": ["target low (126k)", "target high (294k)"]})).mark_rule(strokeDash=[4, 4], color="#888").encode(
        y="y:Q",
    )
    _ref_text = alt.Chart(_pd.DataFrame({"y": [126000, 294000], "label": ["target low (126k)", "target high (294k)"]})).mark_text(align="left", dx=4, dy=-4, fontSize=11, color="#555").encode(
        y="y:Q",
        text="label:N",
    )
    _chart = (_bars + _ref + _ref_text).properties(
        title="6-week projection per country (current GDELT rate) vs project target",
        width=550,
        height=400,
    )
    _chart
    return


if __name__ == "__main__":
    app.run()
