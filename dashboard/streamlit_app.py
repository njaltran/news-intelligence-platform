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
import pydeck as pdk
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# Country centroids for the live coverage map. Rough centers, good
# enough for a bubble at country scale. Extend when new countries
# join the catalogue.
COUNTRY_CENTROIDS: dict[str, tuple[float, float]] = {
    "DE": (51.0, 10.4),
    "US": (39.8, -98.6),
    "IT": (41.9, 12.6),
    "MM": (21.9, 95.9),
    "KZ": (48.0, 67.0),
}

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "news")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "news")
CH_DB = os.getenv("CLICKHOUSE_DATABASE", "news")
TABLE = os.getenv("CLICKHOUSE_TABLE", "news___articles")
SPARKLINE_WINDOW_MIN = int(os.getenv("DASHBOARD_SPARKLINE_MIN", "10"))

st.set_page_config(page_title="News Intel — live feed", layout="wide")


@st.cache_resource
def client():
    # autogenerate_session_id=False so the shared client does not bind
    # queries to a single ClickHouse session. Streamlit autorefresh can
    # fire overlapping script reruns; without this flag, clickhouse-connect
    # raises "concurrent queries within the same session".
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
        autogenerate_session_id=False,
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

if st.sidebar.button("Refresh now"):
    st.rerun()


def fetch_totals() -> dict:
    df = _query(
        f"SELECT count() AS rows, uniqExact(url) AS unique_urls FROM {TABLE}"
    )
    return df.iloc[0].to_dict()


def fetch_by_country() -> pd.DataFrame:
    return _query(
        f"""
        SELECT country_target, count() AS articles
        FROM {TABLE}
        GROUP BY country_target
        ORDER BY articles DESC
        """
    )


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


def fetch_recent(limit: int = 50) -> pd.DataFrame:
    return _query(
        f"""
        SELECT extracted_at, published_at, country_target, source, title, url
        FROM {TABLE}
        ORDER BY extracted_at DESC, published_at DESC NULLS LAST
        LIMIT {limit}
        """
    )


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


def fetch_arrivals(window_min: int) -> pd.DataFrame:
    """Per-second arrivals in the last `window_min` minutes. Drives
    the sparkline that gives the dashboard its streaming feel."""
    return _query(
        f"""
        SELECT toStartOfSecond(extracted_at) AS sec, count() AS arrivals
        FROM {TABLE}
        WHERE extracted_at > now() - INTERVAL {window_min} MINUTE
        GROUP BY sec
        ORDER BY sec
        """
    )


def fetch_country_pulse() -> pd.DataFrame:
    """Per-country totals plus last-minute arrivals. Drives the map.
    `recent` is the heat signal: countries that are actively
    ingesting glow; quiet ones fade to baseline color."""
    return _query(
        f"""
        SELECT
            country_target,
            count() AS articles,
            countIf(extracted_at > now() - INTERVAL 1 MINUTE) AS recent
        FROM {TABLE}
        WHERE country_target IS NOT NULL
        GROUP BY country_target
        """
    )


def fetch_distinct_sources() -> set[str]:
    df = _query(f"SELECT DISTINCT source FROM {TABLE}")
    return set(df["source"].dropna().tolist())


totals = fetch_totals()

current_rows = int(totals["rows"])
prev_rows = st.session_state.get("prev_rows")
delta = None if prev_rows is None else current_rows - prev_rows
st.session_state["prev_rows"] = current_rows

col1, col2, col3 = st.columns(3)
col1.metric(
    "rows loaded",
    f"{current_rows:,}",
    delta=(f"+{delta:,}" if delta and delta > 0 else None),
)
col2.metric("unique URLs", f"{int(totals['unique_urls']):,}")

arrivals = fetch_arrivals(SPARKLINE_WINDOW_MIN)
if not arrivals.empty:
    last_minute = arrivals[arrivals["sec"] >= arrivals["sec"].max() - pd.Timedelta(minutes=1)]
    rate_per_s = last_minute["arrivals"].sum() / max(len(last_minute), 1)
    col3.metric("msgs/s (last min)", f"{rate_per_s:.1f}")
else:
    col3.metric("msgs/s (last min)", "0.0")

# Toast on first-ever sighting of a new source. session_state seeds on
# first refresh so the very first load does not toast every source.
seen_sources: set[str] = st.session_state.get("seen_sources", set())
current_sources = fetch_distinct_sources()
if seen_sources:
    fresh = current_sources - seen_sources
    for src in sorted(fresh)[:6]:
        st.toast(f"new source: {src}")
st.session_state["seen_sources"] = current_sources

st.subheader(f"Arrivals per second (last {SPARKLINE_WINDOW_MIN} min)")
if not arrivals.empty:
    st.line_chart(
        arrivals.set_index("sec")["arrivals"],
        height=180,
        use_container_width=True,
    )
else:
    st.info("No recent arrivals yet. Bring up the producer with ./scripts/dev_stack.sh.")

st.subheader("Live coverage map")
pulse = fetch_country_pulse()
if not pulse.empty:
    pulse = pulse.copy()
    pulse["lat"] = pulse["country_target"].map(
        lambda c: COUNTRY_CENTROIDS.get(c, (0.0, 0.0))[0]
    )
    pulse["lon"] = pulse["country_target"].map(
        lambda c: COUNTRY_CENTROIDS.get(c, (0.0, 0.0))[1]
    )
    # Drop rows whose country code isn't on the centroid map. Don't
    # plot a phantom bubble at (0,0) in the Gulf of Guinea.
    pulse = pulse[pulse["country_target"].isin(COUNTRY_CENTROIDS)]
    max_articles = max(int(pulse["articles"].max() or 0), 1)
    max_recent = max(int(pulse["recent"].max() or 0), 1)
    # Radius in meters. sqrt scaling so a country with 10x articles
    # doesn't draw a bubble that eats the continent.
    pulse["radius"] = (
        (pulse["articles"] / max_articles).pow(0.5) * 600_000 + 80_000
    )
    # Heat: red ramps with last-minute arrivals; quiet countries stay
    # cool blue-grey. Alpha bumps when actively streaming.
    heat = (pulse["recent"] / max_recent).clip(0, 1)
    pulse["r"] = (60 + 195 * heat).round().astype(int)
    pulse["g"] = (120 * (1 - heat)).round().astype(int) + 40
    pulse["b"] = (200 * (1 - heat)).round().astype(int) + 30
    pulse["a"] = (160 + 95 * (pulse["recent"] > 0)).astype(int)
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=pulse,
        get_position="[lon, lat]",
        get_radius="radius",
        radius_min_pixels=6,
        radius_max_pixels=90,
        get_fill_color="[r, g, b, a]",
        pickable=True,
        stroked=True,
        get_line_color=[255, 255, 255, 180],
        line_width_min_pixels=1,
    )
    view = pdk.ViewState(latitude=30.0, longitude=30.0, zoom=1.2, pitch=25)
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view,
        tooltip={
            "text": "{country_target}\narticles: {articles}\nlast min: {recent}"
        },
        map_provider="carto",
        map_style="dark",
    )
    st.pydeck_chart(deck, use_container_width=True)
else:
    st.info("No country-tagged rows yet.")

st.subheader("By country")
by_country = fetch_by_country()
st.bar_chart(by_country.set_index("country_target")["articles"], height=260)
st.dataframe(by_country, use_container_width=True, hide_index=True)

st.subheader("Top sources")
st.dataframe(fetch_top_sources(), use_container_width=True, hide_index=True)

st.subheader("Articles per day (by country)")
by_day = fetch_by_day()
if not by_day.empty:
    pivot = by_day.pivot_table(
        index="day", columns="country_target", values="articles", fill_value=0
    )
    st.line_chart(pivot, height=280)
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
