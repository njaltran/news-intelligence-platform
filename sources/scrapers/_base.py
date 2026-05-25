"""Shared scraping primitive. Each outlet subclass implements `parse`
(homepage HTML → list of partial rows) and `run` is provided.

Rows yielded match the project schema (source, country_target, title,
summary, url, published_at, extracted_at). Subclass fills what it can
from the homepage; missing fields are None and get backfilled by a
later pass over individual article pages (out of scope for the PoC).

EA framing. This is the BeautifulSoup half of the ingestion layer:
needed because the Long Tail outlets (MM Burmese, KZ Kazakh) often
ship no RSS, or the RSS is dead, or it's gated. Matches the project
Variety V and the Long Tail thesis.
"""

import time
from typing import Any, Iterator

import requests
from bs4 import BeautifulSoup
from dlt.common.pendulum import pendulum

USER_AGENT = "Mozilla/5.0 (compatible; NewsIntelBot/0.1)"


class Scraper:
    name: str = ""           # outlet slug, e.g. "mizzima_burmese"
    country: str = ""        # ISO 3166-1 alpha-2, e.g. "MM"
    base_url: str = ""       # homepage to fetch
    request_delay_s: float = 1.0  # politeness between requests

    def fetch(self, url: str) -> str:
        """GET with UA. Raises on non-2xx."""
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
        resp.raise_for_status()
        return resp.text

    def parse(self, html: str) -> list[dict[str, Any]]:
        """Parse homepage HTML to a list of partial rows. Each row must
        contain at minimum `url` and `title`. Override per outlet."""
        raise NotImplementedError

    def run(self) -> Iterator[dict[str, Any]]:
        """Fetch homepage, parse, normalise to project schema."""
        extracted_at = pendulum.now("UTC").to_iso8601_string()
        html = self.fetch(self.base_url)
        time.sleep(self.request_delay_s)
        for partial in self.parse(BeautifulSoup(html, "html.parser")):
            url = partial.get("url")
            if not url:
                continue
            yield {
                "source": partial.get("source", self.name),
                "country_target": self.country,
                "title": partial.get("title"),
                "summary": partial.get("summary"),
                "url": url,
                "published_at": partial.get("published_at"),
                "extracted_at": extracted_at,
            }
