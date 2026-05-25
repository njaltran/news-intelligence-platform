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
        """Pull article anchors from the homepage, dedupe by URL.

        News Eleven typically renders two anchors per article: an
        image-only wrapper and a text-only headline link. Walk every
        anchor first, then keep the best title we saw for each URL
        (non-empty wins; first non-empty wins on tie).
        """
        titles_by_url: dict[str, str | None] = {}
        for anchor in soup.find_all("a", href=True):
            url = anchor["href"]
            if not ARTICLE_HREF_RE.match(url):
                continue
            title = anchor.get_text(strip=True) or None
            existing = titles_by_url.get(url, "__missing__")
            if existing == "__missing__":
                titles_by_url[url] = title
            elif existing is None and title is not None:
                titles_by_url[url] = title
            if len(titles_by_url) >= MAX_HOMEPAGE_LINKS:
                break
        return [{"url": url, "title": title} for url, title in titles_by_url.items()]

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
