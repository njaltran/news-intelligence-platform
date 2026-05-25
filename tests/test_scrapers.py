"""Unit tests for the BS4 scraper base class and outlet subclasses.

Layout matches tests/README.md (one file per source module).
"""

from typing import Any

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


class _FakeScraper(Scraper):
    """Test subclass. Homepage yields two partials, one with a title
    and one without."""

    name = "fake"
    country = "XX"
    base_url = "http://example.invalid/"
    request_delay_s = 0

    def parse(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        return [
            {"url": "http://example.invalid/a", "title": "Home A"},
            {"url": "http://example.invalid/b"},
            {"url": "http://example.invalid/c", "title": ""},
        ]


def test_run_merges_article_fields(monkeypatch):
    """Homepage values win on conflict; article values fill NULLs only."""

    monkeypatch.setattr(Scraper, "fetch", lambda self, url: "<html></html>")
    article_responses = {
        "http://example.invalid/a": {"title": "Article A title", "summary": "A summary"},
        "http://example.invalid/b": {"title": "Article B title", "summary": "B summary"},
        "http://example.invalid/c": {"title": "Article C title", "summary": "C summary"},
    }
    monkeypatch.setattr(
        Scraper, "fetch_article", lambda self, url: article_responses[url]
    )
    monkeypatch.setattr("sources.scrapers._base.time.sleep", lambda *_: None)

    rows = list(_FakeScraper().run())

    assert len(rows) == 3
    # Row 0: homepage gave title => homepage title wins; summary filled from article.
    assert rows[0]["url"] == "http://example.invalid/a"
    assert rows[0]["title"] == "Home A"
    assert rows[0]["summary"] == "A summary"
    # Row 1: homepage had no title => article title fills it.
    assert rows[1]["url"] == "http://example.invalid/b"
    assert rows[1]["title"] == "Article B title"
    assert rows[1]["summary"] == "B summary"
    # Row 2: empty-string homepage title is a value, not a NULL — homepage still wins.
    assert rows[2]["url"] == "http://example.invalid/c"
    assert rows[2]["title"] == ""
    # Summary was None on the homepage, so article fills it.
    assert rows[2]["summary"] == "C summary"
    # Common columns are present.
    for row in rows:
        assert row["source"] == "fake"
        assert row["country_target"] == "XX"
        assert "extracted_at" in row
