"""
PulseLite - Day 4-6
A Kafka consumer that watches the 'reddit-posts' topic, scores sentiment
with VADER, extracts hashtag entities with regex, tracks volume per
minute, and saves everything into a local DuckDB database.
"""

import json
import re
from datetime import datetime, timezone

import duckdb
from kafka import KafkaConsumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "reddit-posts"
CONSUMER_GROUP = "pulselite-processors"
DB_PATH = "pulselite.db"

analyzer = SentimentIntensityAnalyzer()
HASHTAG_PATTERN = re.compile(r"#(\w+)")


def score_sentiment(text: str) -> dict:
    """Returns VADER's sentiment scores. 'compound' ranges -1 to +1."""
    return analyzer.polarity_scores(text)


def extract_entities(text: str) -> list:
    """Pulls out hashtags as our 'entities' for this project."""
    return HASHTAG_PATTERN.findall(text)


def label_sentiment(compound_score: float) -> str:
    if compound_score >= 0.05:
        return "positive"
    elif compound_score <= -0.05:
        return "negative"
    else:
        return "neutral"


def setup_database(con):
    """Creates the table if it doesn't already exist. Safe to run every time."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id VARCHAR PRIMARY KEY,
            author VARCHAR,
            title VARCHAR,
            score INTEGER,
            num_comments INTEGER,
            created_utc TIMESTAMP,
            minute_bucket TIMESTAMP,   -- created_utc rounded down to the minute
            sentiment_label VARCHAR,
            sentiment_score DOUBLE,
            entities VARCHAR[],        -- list of hashtags found
            kafka_offset BIGINT
        )
    """)


def minute_bucket(dt: datetime) -> datetime:
    """Rounds a timestamp down to the start of its minute.
    e.g. 12:47:33 -> 12:47:00. This is how we group posts for
    'volume per minute'.
    """
    return dt.replace(second=0, microsecond=0)


def main():
    print(f"Connecting to Kafka at {KAFKA_BROKER}, topic '{KAFKA_TOPIC}'...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    con = duckdb.connect(DB_PATH)
    setup_database(con)

    print(f"Connected. Writing to {DB_PATH}. Listening for posts... (Ctrl+C to stop)\n")

    current_bucket = None
    current_bucket_count = 0

    for message in consumer:
        post = message.value
        title = post["title"]
        created_dt = datetime.fromtimestamp(post["created_utc"], tz=timezone.utc)
        bucket = minute_bucket(created_dt)

        sentiment_scores = score_sentiment(title)
        sentiment_label = label_sentiment(sentiment_scores["compound"])
        entities = extract_entities(title)

        # --- Day 6: save to DuckDB ---
        con.execute("""
            INSERT OR REPLACE INTO posts
            (id, author, title, score, num_comments, created_utc,
             minute_bucket, sentiment_label, sentiment_score, entities, kafka_offset)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            post["id"], post["author"], title, post["score"], post["num_comments"],
            created_dt, bucket, sentiment_label, sentiment_scores["compound"],
            entities, message.offset,
        ])

        # --- Day 6: rolling volume-per-minute counter (console readout) ---
        if bucket != current_bucket:
            if current_bucket is not None:
                print(f"\n>>> Minute {current_bucket.strftime('%H:%M')} finished with "
                      f"{current_bucket_count} post(s)\n")
            current_bucket = bucket
            current_bucket_count = 0
        current_bucket_count += 1

        print(f"[offset {message.offset}] u/{post['author']} | "
              f"sentiment={sentiment_label} ({sentiment_scores['compound']:+.2f}) | "
              f"entities={entities} | minute={bucket.strftime('%H:%M')}")
        print(f"    {title}")
        print("-" * 80)


if __name__ == "__main__":
    main()