"""
PulseLite - Day 4-5
A Kafka consumer that watches the 'reddit-posts' topic, scores sentiment
with VADER, extracts hashtag entities with regex, and prints results
in real time.
"""

import json
import re

from kafka import KafkaConsumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "reddit-posts"
CONSUMER_GROUP = "pulselite-processors"

# One shared analyzer instance - reused for every post, not recreated each time
analyzer = SentimentIntensityAnalyzer()

# Matches hashtags like #cricket, #IPL, #cricket_team
HASHTAG_PATTERN = re.compile(r"#(\w+)")


def score_sentiment(text: str) -> dict:
    """Returns VADER's sentiment scores for a piece of text.
    'compound' is the overall score: -1 (very negative) to +1 (very positive).
    """
    return analyzer.polarity_scores(text)


def extract_entities(text: str) -> list:
    """Pulls out hashtags as our 'entities' for this project."""
    return HASHTAG_PATTERN.findall(text)


def label_sentiment(compound_score: float) -> str:
    """Turns the raw number into a human-readable label."""
    if compound_score >= 0.05:
        return "positive"
    elif compound_score <= -0.05:
        return "negative"
    else:
        return "neutral"


def main():
    print(f"Connecting to Kafka at {KAFKA_BROKER}, topic '{KAFKA_TOPIC}'...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    print("Connected. Listening for posts... (Ctrl+C to stop)\n")

    for message in consumer:
        post = message.value
        title = post["title"]

        # --- Day 5: sentiment + entity extraction ---
        sentiment_scores = score_sentiment(title)
        sentiment_label = label_sentiment(sentiment_scores["compound"])
        entities = extract_entities(title)

        print(f"[offset {message.offset}] u/{post['author']} | "
              f"sentiment={sentiment_label} ({sentiment_scores['compound']:+.2f}) | "
              f"entities={entities}")
        print(f"    {title}")
        print("-" * 80)


if __name__ == "__main__":
    main()