"""Kafka producer: native-language outlets across the five target countries.

Hybrid strategy: HTML scraping where the page is open, RSS for Myanmar
outlets (Mizzima, Myanmar Now) where HTML returns 403 against bot UAs.

EA framing. This is the Long Tail half of the Variety story: Mizzima,
The Irrawaddy, Tengrinews, Qazinform are outlets no global aggregator
indexes well. Including them is why this project moves the needle on
narrative divergence.
"""

import json
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer

TOPIC = "unified_news_topic"


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")


class NewsScraper:
    def __init__(self, topic: str) -> None:
        self.producer = Producer({
            "bootstrap.servers": "localhost:9092",
            "client.id": "local-scraper-producer",
            "socket.timeout.ms": 10000,
        })
        self.topic = topic
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def produce(self, source, country, title, url, summary):
        msg = {
            "source": source,
            "country_target": country,
            "title": title.strip() if title else None,
            "url": url,
            "summary": summary.strip() if summary else None,
            "extracted_at": datetime.now().isoformat(),
        }
        self.producer.produce(self.topic, json.dumps(msg).encode("utf-8"), callback=delivery_report)
        self.producer.poll(0)

    def scrape_site(self, url, country, source, item_selector, title_selector, link_selector=None):
        try:
            time.sleep(1)
            res = self.session.get(url, timeout=20)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            items = soup.select(item_selector)
            count = 0
            for item in items[:15]:
                t_tag = item.select_one(title_selector)
                if link_selector:
                    l_tag = item.select_one(link_selector)
                else:
                    l_tag = item.find("a") if hasattr(item, "find") else None

                if not l_tag and item.name == "a":
                    l_tag = item

                if t_tag and l_tag and l_tag.get("href"):
                    title = t_tag.get_text(strip=True)
                    link = l_tag["href"]
                    if not link.startswith("http"):
                        link = requests.compat.urljoin(url, link)

                    self.produce(source, country, title, link, "")
                    count += 1
            print(f"Scraped {count} articles from {source} ({country})")
        except Exception as e:
            print(f"Error scraping {source}: {e}")

    def scrape_rss(self, url, country, source):
        try:
            res = self.session.get(url, timeout=20)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "xml")
            items = soup.find_all("item")
            count = 0
            for item in items[:15]:
                title = item.find("title").get_text() if item.find("title") else ""
                link = item.find("link").get_text() if item.find("link") else ""
                summary = item.find("description").get_text() if item.find("description") else ""
                summary = BeautifulSoup(summary, "html.parser").get_text() if summary else ""

                if title and link:
                    self.produce(source, country, title, link, summary)
                    count += 1
            print(f"Scraped {count} articles via RSS from {source} ({country})")
        except Exception as e:
            print(f"Error scraping RSS {source}: {e}")

    def run_all(self) -> None:
        # Italy
        self.scrape_site("https://www.ansa.it/sito/notizie/mondo/mondo.shtml", "Italy", "ANSA", "article", "h3")
        self.scrape_site("https://www.repubblica.it/", "Italy", "La Repubblica", "article", "h2, h3")

        # Germany
        self.scrape_site("https://www.tagesschau.de/", "Germany", "Tagesschau", ".teaser", ".teaser__headline")
        self.scrape_site("https://www.spiegel.de/", "Germany", "Der Spiegel", "article", "h2")
        self.scrape_site("https://www.welt.de/", "Germany", "Die Welt", "article", "h3")

        # Myanmar (RSS bypasses 403 bot blocks)
        self.scrape_rss("https://www.mizzima.com/rss.xml", "Myanmar", "Mizzima")
        self.scrape_rss("https://myanmar-now.org/en/feed/", "Myanmar", "Myanmar Now")
        self.scrape_site("https://www.irrawaddy.com/", "Myanmar", "The Irrawaddy", "article", "h3")

        # Kazakhstan
        self.scrape_site(
            "https://tengrinews.kz/",
            "Kazakhstan",
            "Tengrinews",
            ".main-news_top_item, .main-news_column_item",
            "span",
        )
        self.scrape_site("https://qazinform.com/", "Kazakhstan", "Qazinform", ".news_item, article", "h3, .title")

        # US
        self.scrape_site("https://www.nytimes.com/international/", "US", "NYT", "section.story-wrapper", "h3")
        self.scrape_site("https://edition.cnn.com/world", "US", "CNN", ".card", ".container__headline-text")
        self.scrape_site("https://www.nbcnews.com/", "US", "NBC News", ".alacarte__item", "h2, h3")

        self.producer.flush()


if __name__ == "__main__":
    scraper = NewsScraper(TOPIC)
    print("Starting specialized Local Scrapers...")
    scraper.run_all()
    print("Local Scrapers finished.")
