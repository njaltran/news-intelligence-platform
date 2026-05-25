"""News Eleven (Eleven Media Burmese) homepage scraper.

Burmese-language outlet at https://news-eleven.com/ with no RSS feed.
Article URLs follow the pattern `/article/<int>`. Article pages carry
a `<meta name="description">` (Burmese text) used to populate the
summary column.
"""

import re
from typing import Any

import dlt
from bs4 import BeautifulSoup

from sources.scrapers._base import Scraper

ARTICLE_HREF_RE = re.compile(r"^https://news-eleven\.com/article/\d+$")
MAX_HOMEPAGE_LINKS = 100


class NewsElevenScraper(Scraper):
    name = "News Eleven"
    country = "MM"
    base_url = "https://news-eleven.com/"
    request_delay_s = 1.0

    def parse(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        """Pull article anchors from the homepage, dedupe by URL."""
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for anchor in soup.find_all("a", href=True):
            url = anchor["href"]
            if not ARTICLE_HREF_RE.match(url) or url in seen:
                continue
            seen.add(url)
            title = anchor.get_text(strip=True) or None
            rows.append({"url": url, "title": title})
            if len(rows) >= MAX_HOMEPAGE_LINKS:
                break
        return rows

    def parse_article(self, soup: BeautifulSoup) -> dict[str, Any]:
        """Pull the article summary from the article page itself.

        News Eleven article pages carry a meta description; fall back
        to og:description.
        """
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            return {"summary": meta["content"].strip()}
        og = soup.find("meta", attrs={"property": "og:description"})
        if og and og.get("content"):
            return {"summary": og["content"].strip()}
        return {}


@dlt.resource(name="articles", primary_key="url", write_disposition="merge")
def news_eleven():
    """One row per article URL discovered on the News Eleven homepage."""
    yield from NewsElevenScraper().run()
