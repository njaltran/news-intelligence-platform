"""Mizzima Burmese homepage scraper.

No RSS available on the Burmese edition (the English edition has one).
Homepage serves 62+ post tiles in `div.mag-post-single`. Title is in
the first heading, URL is in the first anchor, date is encoded in the
URL path (`https://bur.mizzima.com/YYYY/MM/DD/<id>`).

Summary is not present on the homepage card and would require a
follow-up GET per article. Skipped for the PoC: downstream cleaning
can backfill if needed.
"""

import re
from typing import Any

import dlt
from bs4 import BeautifulSoup
from dlt.common.pendulum import pendulum

from sources.scrapers._base import Scraper

URL_DATE_RE = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/")


class MizzimaBurmeseScraper(Scraper):
    name = "Mizzima Burmese"
    country = "MM"
    base_url = "https://www.mizzimaburmese.com/"

    def parse(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for post in soup.select("div.mag-post-single"):
            heading = post.find(["h2", "h3"])
            anchor = post.find("a", href=True)
            if not heading or not anchor:
                continue
            url = anchor["href"]
            rows.append(
                {
                    "title": heading.get_text(strip=True),
                    "url": url,
                    "published_at": _date_from_url(url),
                }
            )
        return rows


def _date_from_url(url: str) -> str | None:
    """Extract YYYY-MM-DD from Mizzima URL path. Returns ISO8601 UTC midnight."""
    match = URL_DATE_RE.search(url)
    if not match:
        return None
    year, month, day = (int(g) for g in match.groups())
    return pendulum.datetime(year, month, day, tz="UTC").to_iso8601_string()


@dlt.resource(name="articles", primary_key="url", write_disposition="merge")
def mizzima_burmese():
    """One row per homepage post tile."""
    yield from MizzimaBurmeseScraper().run()
