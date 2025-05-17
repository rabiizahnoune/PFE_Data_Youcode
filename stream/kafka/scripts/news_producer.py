import os
import requests
import json
from datetime import datetime
import time
from kafka import KafkaProducer
import uuid

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Get API key from environment variable
API_KEY = '4cdba65592e54df9bad3aad6361f2d0b'
if not API_KEY:
    print("ERROR: NEWSAPI_KEY environment variable not set!")
    exit(1)
# Base URL for NewsAPI
BASE_URL = "https://newsapi.org/v2/everything"

# Keywords relevant to gold market
KEYWORDS = [
    "gold price", "XAUUSD", "gold market", "Federal Reserve", "interest rates",
    "tariffs", "geopolitical tensions", "inflation", "US dollar", "safe-haven assets"
]
QUERY = " OR ".join(KEYWORDS)

def fetch_news():
    """Fetch news articles and send to Kafka."""
    params = {
        "q": QUERY,
        "apiKey": API_KEY,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 10
    }
    
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data["status"] != "ok":
            print(f"Error from NewsAPI: {data.get('message', 'Unknown error')}")
            return
        
        articles = data.get("articles", [])
        if not articles:
            print("No new articles found.")
            return
        
        print(f"\nFetched {len(articles)} articles at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        for article in articles:
            # Structure the article data with UUID
            article_data = {
                "id": str(uuid.uuid4()),  # Generate UUID for Cassandra primary key
                "title": article["title"],
                "source": article["source"]["name"],
                "published_at": article["publishedAt"],
                "description": article.get("description", ""),
                "url": article["url"],
                "ingestion_time": datetime.now().isoformat()
            }
            # Send to Kafka topic
            producer.send('gold-news', article_data)
            print(f"Sent to Kafka: {article['title'][:50]}...")
    
    except requests.RequestException as e:
        print(f"Error fetching news: {e}")

if __name__ == "__main__":
    print("Starting news ingestion for gold market...")
    print(f"Searching for keywords: {QUERY}")
    
   
    fetch_news()
    producer.flush()
    print("\nWaiting for next poll...")
    