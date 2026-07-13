"""
PulseLite - Day 4-7
A Kafka consumer that watches the 'reddit-posts' topic, scores sentiment
with VADER, extracts hashtag entities with regex, tracks volume per
minute, computes rolling topic drift, and saves everything into DuckDB.
"""

import json
import re
from collections import Counter, deque
from datetime import datetime, timezone

import duckdb
from kafka import KafkaConsumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "reddit-posts"
CONSUMER_GROUP = "pulselite-processors"
DB_PATH = "pulselite.db"

# How far back "recent" means for topic drift, e.g. the last N posts.
# Using a post-count window instead of a strict time window keeps this
# simple - a common, acceptable simplification for a first version.
DRIFT_WINDOW_SIZE = 50
TOP_N_TOPICS = 5

analyzer = SentimentIntensityAnalyzer()
HASHTAG_PATTERN = re.compile(r"#(\w+)")


def score_sentiment(text: str) -> dict:
    return analyzer.polarity_scores(text)


def extract_entities(text: str) -> list:
    return HASHTAG_PATTERN.findall(text)


def label_sentiment(compound_score: float) -> str:
    if compound_score >= 0.05:
        return "positive"
    elif compound_score <= -0.05:
        return "negative"
    else:
        return "neutral"


def setup_database(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id VARCHAR PRIMARY KEY,
            author VARCHAR,
            title VARCHAR,
            score INTEGER,
            num_comments INTEGER,
            created_utc TIMESTAMP,
            minute_bucket TIMESTAMP,
            sentiment_label VARCHAR,
            sentiment_score DOUBLE,
            entities VARCHAR[],
            kafka_offset BIGINT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS topic_drift (
            computed_at TIMESTAMP,
            topic VARCHAR,
            mentions INTEGER,
            rank INTEGER
        )
    """)


def minute_bucket(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


class TopicDriftTracker:
    """Keeps a rolling window of the last N posts' hashtags and reports
    the current top topics. As new posts come in and old ones fall out
    of the window, the top list 'drifts' over time - that's the point.
    """

    def __init__(self, window_size: int):
        self.window = deque(maxlen=window_size)  # holds lists of entities per post

    def add(self, entities: list):
        self.window.append(entities)

    def top_topics(self, n: int) -> list:
        counter = Counter()
        for entities in self.window:
            counter.update(entities)
        return counter.most_common(n)


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
    drift_tracker = TopicDriftTracker(DRIFT_WINDOW_SIZE)

    print(f"Connected. Writing to {DB_PATH}. Listening for posts... (Ctrl+C to stop)\n")

    current_bucket = None
    current_bucket_count = 0
    posts_since_drift_print = 0

    for message in consumer:
        post = message.value
        title = post["title"]
        created_dt = datetime.fromtimestamp(post["created_utc"], tz=timezone.utc)
        bucket = minute_bucket(created_dt)

        sentiment_scores = score_sentiment(title)
        sentiment_label = label_sentiment(sentiment_scores["compound"])
        entities = extract_entities(title)

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

        # --- Day 7: topic drift ---
        drift_tracker.add(entities)
        posts_since_drift_print += 1

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

        # Every 10 posts, print + save the current trending topics
        if posts_since_drift_print >= 10:
            posts_since_drift_print = 0
            top_topics = drift_tracker.top_topics(TOP_N_TOPICS)
            now = datetime.now(timezone.utc)

            print(f"\n### TRENDING NOW (last {len(drift_tracker.window)} posts) ###")
            for rank, (topic, mentions) in enumerate(top_topics, start=1):
                print(f"  {rank}. #{topic} - {mentions} mentions")
                con.execute("""
                    INSERT INTO topic_drift (computed_at, topic, mentions, rank)
                    VALUES (?, ?, ?, ?)
                """, [now, topic, mentions, rank])
            print()


if __name__ == "__main__":
    main()