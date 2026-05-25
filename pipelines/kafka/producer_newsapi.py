"""Kafka producer: NewsAPI top-headlines + everything queries -> unified_news_topic.

API key comes from .dlt/secrets.toml under [sources.newsapi]:

    [sources.newsapi]
    api_key = "..."

Run order (separate terminals):
    1. docker compose -f infra/docker-compose.yml up -d
    2. uv run python pipelines/kafka/consumer_to_duckdb.py
    3. uv run python pipelines/kafka/producer_newsapi.py
"""

import json
import time
from datetime import datetime

import dlt
import requests
from confluent_kafka import Producer

API_KEY = dlt.secrets["sources.newsapi.api_key"]
TOPIC = "unified_news_topic"


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")


def fetch_newsapi(api_key: str, topic: str) -> None:
    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "client.id": "newsapi-producer",
        "socket.timeout.ms": 10000,
    })

    countries = {
        "US": {"code": "us", "type": "top-headlines"},
        "Germany": {"code": "de", "type": "top-headlines"},
        "Italy": {"code": "it", "type": "top-headlines"},
        "Myanmar": {"query": "Myanmar", "type": "everything"},
        "Kazakhstan": {"query": "Kazakhstan", "type": "everything"},
    }

    total_count = 0
    extracted_at = datetime.now().isoformat()

    for country_name, config in countries.items():
        print(f"Fetching NewsAPI for {country_name}...")

        if config["type"] == "top-headlines":
            url = f"https://newsapi.org/v2/top-headlines?country={config['code']}&apiKey={api_key}"
        else:
            url = (
                f"https://newsapi.org/v2/everything?q={config['query']}"
                f"&pageSize=20&sortBy=publishedAt&apiKey={api_key}"
            )

        response = requests.get(url)
        data = response.json()

        if data.get("status") != "ok":
            print(f"Error for {country_name}: {data.get('message')}")
            continue

        articles = data.get("articles", [])
        for article in articles:
            standardized_msg = {
                "source": "NewsAPI",
                "country_target": country_name,
                "title": article.get("title"),
                "url": article.get("url"),
                "summary": article.get("description"),
                "published_at": article.get("publishedAt"),
                "extracted_at": extracted_at,
            }
            producer.produce(
                topic,
                json.dumps(standardized_msg).encode("utf-8"),
                callback=delivery_report,
            )
            total_count += 1
            producer.poll(0)

        producer.flush()
        print(f"Sent {len(articles)} articles for {country_name}")
        time.sleep(1)

    print(f"Finished. Total NewsAPI articles sent to Kafka: {total_count}")


if __name__ == "__main__":
    print("Starting NewsAPI Producer...")
    fetch_newsapi(API_KEY, TOPIC)
