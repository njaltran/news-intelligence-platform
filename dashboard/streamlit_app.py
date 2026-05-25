"""Lightweight Streamlit dashboard for the streaming ingest.

Reads from the local ClickHouse DWH (see infra/docker-compose.yml) and
shows the raw articles arriving via the Kappa path. Intended as a
quick eyeball-test of producer + consumer health, not the marimo
narrative-divergence dashboard.

Run from the repo root:

    uv run streamlit run dashboard/streamlit_app.py
"""

from __future__ import annotations

import os

import clickhouse_connect
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "news")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "news")
CH_DB = os.getenv("CLICKHOUSE_DATABASE", "news")
TABLE = os.getenv("CLICKHOUSE_TABLE", "news___articles")

st.set_page_config(page_title="News Intel — live feed", layout="wide")


@st.cache_resource
def client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )


def _query(sql: str) -> pd.DataFrame:
    return client().query_df(sql)


st.title("News Intelligence — live ingest")
st.caption(f"ClickHouse at {CH_USER}@{CH_HOST}:{CH_PORT}/{CH_DB}.{TABLE}")

interval_s = st.sidebar.slider("auto-refresh seconds", 0, 60, 5, step=1)
if interval_s > 0:
    st_autorefresh(interval=interval_s * 1000, key="dashboard_autorefresh")
    st.sidebar.caption(f"refreshing every {interval_s}s")
else:
    st.sidebar.caption("auto-refresh paused")

# Always re-pull on rerun. The 30s @st.cache_data was too sticky for a
# live-ingest view, so the cache is cleared each tick.
st.cache_data.clear()
if st.sidebar.button("Refresh now"):
    st.rerun()


@st.cache_data(ttl=2)
def fetch_totals() -> dict:
    df = _query(
        f"SELECT count() AS rows, uniqExact(url) AS unique_urls FROM {TABLE}"
    )
    return df.iloc[0].to_dict()


@st.cache_data(ttl=2)
def fetch_by_country() -> pd.DataFrame:
    return _query(
        f"""
        SELECT country_target, count() AS articles
        FROM {TABLE}
        GROUP BY country_target
        ORDER BY articles DESC
        """
    )


@st.cache_data(ttl=2)
def fetch_top_sources(limit: int = 30) -> pd.DataFrame:
    return _query(
        f"""
        SELECT source, country_target, count() AS articles
        FROM {TABLE}
        GROUP BY source, country_target
        ORDER BY articles DESC
        LIMIT {limit}
        """
    )


@st.cache_data(ttl=2)
def fetch_recent(limit: int = 50) -> pd.DataFrame:
    return _query(
        f"""
        SELECT extracted_at, published_at, country_target, source, title, url
        FROM {TABLE}
        ORDER BY extracted_at DESC, published_at DESC NULLS LAST
        LIMIT {limit}
        """
    )


@st.cache_data(ttl=2)
def fetch_by_day() -> pd.DataFrame:
    return _query(
        f"""
        SELECT toDate(published_at) AS day, country_target, count() AS articles
        FROM {TABLE}
        WHERE published_at IS NOT NULL
        GROUP BY day, country_target
        ORDER BY day
        """
    )


try:
    totals = fetch_totals()
except Exception as exc:  # noqa: BLE001
    st.error(f"ClickHouse query failed: {exc}")
    st.stop()

col1, col2 = st.columns(2)
col1.metric("rows loaded", f"{int(totals['rows']):,}")
col2.metric("unique URLs", f"{int(totals['unique_urls']):,}")

st.subheader("By country")
by_country = fetch_by_country()
st.bar_chart(by_country.set_index("country_target")["articles"], height=300)
st.dataframe(by_country, use_container_width=True, hide_index=True)

st.subheader("Top sources")
st.dataframe(fetch_top_sources(), use_container_width=True, hide_index=True)

st.subheader("Articles per day (by country)")
by_day = fetch_by_day()
if not by_day.empty:
    pivot = by_day.pivot_table(
        index="day", columns="country_target", values="articles", fill_value=0
    )
    st.line_chart(pivot, height=320)
else:
    st.info("No rows with parsed published_at yet.")

st.subheader("Latest 50 articles")
recent = fetch_recent()
st.dataframe(
    recent,
    use_container_width=True,
    hide_index=True,
    column_config={
        "url": st.column_config.LinkColumn("url"),
    },
)
