import os
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("NEWS_API_KEY")
URL = f"https://newsapi.org/v2/top-headlines?country=us&apiKey={API_KEY}"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_articles():
    response = requests.get(URL)
    data = response.json()
    for article in data.get("articles", []):
        if article.get("description"):
            text = article["description"]
            producer.send("news_topic", value={"text": text})
            print("Envoyé :", text)

get_articles()
