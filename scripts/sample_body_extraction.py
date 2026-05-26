"""Compare 4 article-body extraction methods on a ClickHouse sample.

  M1  bot UA       + heuristic <article>/<p>   (current baseline)
  M2  browser UA   + heuristic <article>/<p>
  M3  browser UA   + trafilatura
  M4  gnews-only: resolve redirect → publisher URL → browser UA + trafilatura

Sample mix: 5 publisher URLs per country + 2 Google News URLs per country.

Run from repo root:
    PYTHONPATH=. uv run python scripts/sample_body_extraction.py
"""

from __future__ import annotations

import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import requests
import trafilatura
from googlenewsdecoder import gnewsdecoder

from sources.rss import _extract_body  # heuristic extractor

CH_URL = "http://localhost:8123/"
CH_AUTH = ("news", "news")
CH_DB = "news"

WORKERS = 8

BOT_UA = "Mozilla/5.0 (compatible; NewsIntelBot/0.1)"
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


@dataclass
class Sample:
    country: str
    source: str
    url: str
    kind: str  # "publisher" | "gnews"


@dataclass
class Result:
    m1: str | None = None
    m2: str | None = None
    m3: str | None = None
    m4: str | None = None
    resolved_url: str | None = None


def ch_query(sql: str) -> str:
    r = requests.post(
        CH_URL,
        params={"database": CH_DB},
        data=sql.encode("utf-8"),
        auth=CH_AUTH,
        timeout=30,
    )
    r.raise_for_status()
    return r.text


def pick_samples() -> list[Sample]:
    pub_sql = """
    SELECT country_target, source, url
    FROM (
      SELECT country_target, source, url,
        row_number() OVER (PARTITION BY country_target, source ORDER BY rand()) AS rn_src
      FROM news___articles
      WHERE url != '' AND source NOT LIKE 'Google News%'
    )
    WHERE rn_src = 1
    ORDER BY country_target, rand()
    LIMIT 5 BY country_target
    FORMAT TSV
    """
    gn_sql = """
    SELECT country_target, source, url
    FROM news___articles
    WHERE url LIKE 'https://news.google.com/%'
    ORDER BY country_target, rand()
    LIMIT 2 BY country_target
    FORMAT TSV
    """
    samples: list[Sample] = []
    for line in ch_query(pub_sql).strip().splitlines():
        p = line.split("\t")
        if len(p) >= 3:
            samples.append(Sample(p[0], p[1], p[2], "publisher"))
    for line in ch_query(gn_sql).strip().splitlines():
        p = line.split("\t")
        if len(p) >= 3:
            samples.append(Sample(p[0], p[1], p[2], "gnews"))
    return samples


def fetch_html(url: str, ua: str, timeout: float = 15.0) -> bytes | None:
    """Return raw bytes; trafilatura + BeautifulSoup sniff the encoding."""
    try:
        r = requests.get(
            url, headers={"User-Agent": ua}, timeout=timeout, allow_redirects=True
        )
        if r.status_code >= 400:
            return None
        return r.content
    except requests.RequestException:
        return None


def heuristic(html: bytes | None) -> str | None:
    if not html:
        return None
    return _extract_body(html)


def via_trafilatura(html: bytes | None) -> str | None:
    if not html:
        return None
    text = trafilatura.extract(
        html, include_comments=False, include_tables=False, favor_recall=False
    )
    if not text:
        return None
    return text[:20000]


def resolve_gnews(url: str) -> str | None:
    try:
        out = gnewsdecoder(url, interval=1)
        if isinstance(out, dict) and out.get("status"):
            return out.get("decoded_url")
    except Exception:
        return None
    return None


def run_one(s: Sample) -> Result:
    res = Result()

    html_bot = fetch_html(s.url, BOT_UA)
    res.m1 = heuristic(html_bot)

    html_br = fetch_html(s.url, BROWSER_UA)
    res.m2 = heuristic(html_br)
    res.m3 = via_trafilatura(html_br)

    if s.kind == "gnews":
        resolved = resolve_gnews(s.url)
        res.resolved_url = resolved
        if resolved:
            html_r = fetch_html(resolved, BROWSER_UA)
            res.m4 = via_trafilatura(html_r)

    return res


def fmt(text: str | None) -> str:
    if not text:
        return "<none>"
    head = textwrap.shorten(text[:300], width=240, placeholder=" ...")
    return f"{len(text):>6,} chars | {head}"


def main() -> int:
    samples = pick_samples()
    print(f"sampled {len(samples)} urls\n")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = list(pool.map(run_one, samples))

    tally = {"m1": 0, "m2": 0, "m3": 0, "m4": 0, "m4_eligible": 0}
    for s, r in zip(samples, results):
        print("=" * 100)
        print(f"[{s.country}] ({s.kind}) {s.source}")
        print(f"  url: {s.url}")
        if r.resolved_url:
            print(f"  resolved: {r.resolved_url}")
        print(f"  M1 bot/heur:  {fmt(r.m1)}")
        print(f"  M2 br/heur:   {fmt(r.m2)}")
        print(f"  M3 br/traf:   {fmt(r.m3)}")
        if s.kind == "gnews":
            tally["m4_eligible"] += 1
            print(f"  M4 gn/traf:   {fmt(r.m4)}")
            if r.m4:
                tally["m4"] += 1
        if r.m1:
            tally["m1"] += 1
        if r.m2:
            tally["m2"] += 1
        if r.m3:
            tally["m3"] += 1

    n = len(samples)
    print()
    print("summary (extracted / total)")
    print(f"  M1 bot UA      + heuristic   : {tally['m1']}/{n}")
    print(f"  M2 browser UA  + heuristic   : {tally['m2']}/{n}")
    print(f"  M3 browser UA  + trafilatura : {tally['m3']}/{n}")
    print(f"  M4 gnews resolve + traf       : {tally['m4']}/{tally['m4_eligible']} (gnews only)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
