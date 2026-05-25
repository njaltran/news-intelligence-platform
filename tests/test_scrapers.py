"""Unit tests for the BS4 scraper base class and outlet subclasses.

Layout matches tests/README.md (one file per source module).
"""

import requests
from bs4 import BeautifulSoup

from sources.scrapers._base import Scraper


def test_parse_article_default_returns_empty():
    """Default parse_article returns {} so subclasses can opt out of
    article-page backfill without raising."""
    scraper = Scraper()
    soup = BeautifulSoup("<html><body><p>anything</p></body></html>", "html.parser")
    assert scraper.parse_article(soup) == {}


def test_fetch_article_swallows_request_errors(monkeypatch):
    """A failed article GET must yield {} instead of bubbling up so the
    whole run is not killed by one bad URL."""

    def boom(self, url):
        raise requests.RequestException("connection reset")

    monkeypatch.setattr(Scraper, "fetch", boom)
    monkeypatch.setattr("sources.scrapers._base.time.sleep", lambda *_: None)

    scraper = Scraper()
    assert scraper.fetch_article("http://example.invalid/article") == {}
