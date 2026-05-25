"""Unit tests for the BS4 scraper base class and outlet subclasses.

Layout matches tests/README.md (one file per source module).
"""

from bs4 import BeautifulSoup

from sources.scrapers._base import Scraper


def test_parse_article_default_returns_empty():
    """Default parse_article returns {} so subclasses can opt out of
    article-page backfill without raising."""
    scraper = Scraper()
    soup = BeautifulSoup("<html><body><p>anything</p></body></html>", "html.parser")
    assert scraper.parse_article(soup) == {}
