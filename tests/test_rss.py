"""Unit tests for sources/rss.py body-extraction helpers.

Layout matches tests/README.md (one file per source module).
"""

from pathlib import Path

from sources.rss import _extract_body, _should_skip

FIXTURES = Path(__file__).parent / "fixtures"


def test_extract_body_returns_main_text():
    """Trafilatura strips header/nav/footer and keeps the article paragraphs."""
    html = (FIXTURES / "article_body_sample.html").read_bytes()
    body = _extract_body(html)
    assert body is not None
    assert "Carbonara is a Roman pasta dish" in body
    assert "guanciale" in body
    # nav + footer must not bleed into the body
    assert "Home | About | Contact" not in body
    assert "Copyright 2026 Test Site" not in body


def test_extract_body_returns_none_on_empty():
    """Empty / non-article HTML yields None so the producer can skip it."""
    assert _extract_body(b"") is None
    assert _extract_body(b"<html><body></body></html>") is None


def test_should_skip_google_news_redirect():
    """Google News redirect URLs are skipped (IP throttle, see spec)."""
    assert _should_skip("https://news.google.com/rss/articles/CBMiABC?oc=5") is True


def test_should_skip_reddit():
    """Reddit needs the API, not HTML scraping."""
    assert _should_skip("https://www.reddit.com/r/news/comments/abc/title/") is True
    assert _should_skip("https://reddit.com/r/news/comments/abc/title/") is True


def test_should_skip_keeps_publisher():
    """Regular publisher URLs are not skipped."""
    assert _should_skip("https://www.spiegel.de/politik/article-a-1234.html") is False
    assert _should_skip("https://www.repubblica.it/economia/2026/05/25/news/x.html") is False
