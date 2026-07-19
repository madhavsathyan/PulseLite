"""
PulseLite - Day 4-8 (connection-per-write version)
A Kafka consumer that watches the 'reddit-posts' topic, scores sentiment
with VADER, extracts hashtag entities with regex, tracks volume per
minute, computes rolling topic drift, detects volume anomalies per
topic, and saves everything into DuckDB.

Note on DuckDB connections: DuckDB is an embedded, single-writer database
(no server process). We open a short-lived connection for each write and
close it immediately - this keeps the lock held for milliseconds instead
of the entire runtime. Even so, brief conflicts with the dashboard's read
connection can still happen, so writes are wrapped in run_with_retry(),
which retries a few times with a short backoff instead of crashing.
See ADR-02 in the README.
"""

import json
import re
import time as time_module
from collections import Counter, deque, defaultdict
from datetime import datetime, timezone

import duckdb
from kafka import KafkaConsumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import os
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "reddit-posts"
CONSUMER_GROUP = "pulselite-processors"
DB_PATH = os.environ.get("DB_PATH", "pulselite.db")

DRIFT_WINDOW_SIZE = 50
TOP_N_TOPICS = 5

ROLLING_MINUTES = 5
ANOMALY_MULTIPLIER = 3

analyzer = SentimentIntensityAnalyzer()
HASHTAG_PATTERN = re.compile(r"#(\w+)")


def run_with_retry(fn, max_attempts=5, base_delay=0.2):
    """Runs a DuckDB operation, retrying briefly if the file is
    momentarily locked by another process (e.g. the dashboard reading
    at the same instant). This is expected in a multi-process setup
    with an embedded database - retrying is the standard fix, rather
    than crashing the whole consumer over a millisecond-long conflict.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            if "lock" not in str(e).lower() or attempt == max_attempts:
                raise
            time_module.sleep(base_delay * attempt)


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


def setup_database():
    def _do_setup():
        with duckdb.connect(DB_PATH) as con:
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
            con.execute("""
                CREATE TABLE IF NOT EXISTS anomalies (
                    detected_at TIMESTAMP,
                    topic VARCHAR,
                    minute_bucket TIMESTAMP,
                    current_volume INTEGER,
                    rolling_avg DOUBLE,
                    multiplier DOUBLE
                )
            """)
    run_with_retry(_do_setup, max_attempts=10, base_delay=0.5)


def save_post(post, title, created_dt, bucket, sentiment_label, sentiment_scores, entities, offset):
    with duckdb.connect(DB_PATH) as db:
        db.execute("""
            INSERT OR REPLACE INTO posts
            (id, author, title, score, num_comments, created_utc,
             minute_bucket, sentiment_label, sentiment_score, entities, kafka_offset)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            post["id"], post["author"], title, post["score"], post["num_comments"],
            created_dt, bucket, sentiment_label, sentiment_scores["compound"],
            entities, offset,
        ])


def save_anomaly(topic, bucket, current_count, rolling_avg):
    def _do_save():
        with duckdb.connect(DB_PATH) as db:
            db.execute("""
                INSERT INTO anomalies
                (detected_at, topic, minute_bucket, current_volume, rolling_avg, multiplier)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                datetime.now(timezone.utc), topic, bucket,
                current_count, rolling_avg, current_count / rolling_avg,
            ])
    run_with_retry(_do_save)


def save_drift_snapshot(now, top_topics):
    def _do_save():
        with duckdb.connect(DB_PATH) as db:
            for rank, (topic, mentions) in enumerate(top_topics, start=1):
                db.execute("""
                    INSERT INTO topic_drift (computed_at, topic, mentions, rank)
                    VALUES (?, ?, ?, ?)
                """, [now, topic, mentions, rank])
    run_with_retry(_do_save)


def minute_bucket(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


class TopicDriftTracker:
    def __init__(self, window_size: int):
        self.window = deque(maxlen=window_size)

    def add(self, entities: list):
        self.window.append(entities)

    def top_topics(self, n: int) -> list:
        counter = Counter()
        for entities in self.window:
            counter.update(entities)
        return counter.most_common(n)


class TopicVolumeTracker:
    def __init__(self, rolling_minutes: int, multiplier: float):
        self.rolling_minutes = rolling_minutes
        self.multiplier = multiplier
        self.topic_minute_counts = defaultdict(lambda: deque(maxlen=rolling_minutes))
        self.current_minute_counts = defaultdict(int)
        self.current_bucket = None

    def add(self, bucket: datetime, entities: list):
        if bucket != self.current_bucket:
            self._roll_over(bucket)
        for topic in entities:
            self.current_minute_counts[topic] += 1

    def _roll_over(self, new_bucket: datetime):
        if self.current_bucket is not None:
            for topic, count in self.current_minute_counts.items():
                self.topic_minute_counts[topic].append(count)
        self.current_minute_counts = defaultdict(int)
        self.current_bucket = new_bucket

    def check_anomalies(self) -> list:
        anomalies = []
        for topic, current_count in self.current_minute_counts.items():
            history = self.topic_minute_counts[topic]
            if len(history) == 0:
                continue
            rolling_avg = sum(history) / len(history)
            if rolling_avg > 0 and current_count > rolling_avg * self.multiplier:
                anomalies.append((topic, current_count, rolling_avg))
        return anomalies


def main():
    print(f"Connecting to Kafka at {KAFKA_BROKER}, topic '{KAFKA_TOPIC}'...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    setup_database()
    drift_tracker = TopicDriftTracker(DRIFT_WINDOW_SIZE)
    volume_tracker = TopicVolumeTracker(ROLLING_MINUTES, ANOMALY_MULTIPLIER)

    print(f"Connected. Writing to {DB_PATH}. Listening for posts... (Ctrl+C to stop)\n")

    current_bucket = None
    current_bucket_count = 0
    posts_since_drift_print = 0
    already_flagged = set()

    for message in consumer:
        post = message.value
        title = post["title"]
        created_dt = datetime.fromtimestamp(post["created_utc"], tz=timezone.utc)
        bucket = minute_bucket(created_dt)

        sentiment_scores = score_sentiment(title)
        sentiment_label = label_sentiment(sentiment_scores["compound"])
        entities = extract_entities(title)

        save_post(post, title, created_dt, bucket, sentiment_label, sentiment_scores, entities, message.offset)

        drift_tracker.add(entities)
        volume_tracker.add(bucket, entities)
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

        for topic, current_count, rolling_avg in volume_tracker.check_anomalies():
            flag_key = (topic, bucket)
            if flag_key in already_flagged:
                continue
            already_flagged.add(flag_key)

            print(f"\n🚨 ANOMALY DETECTED: #{topic} has {current_count} mentions this "
                  f"minute vs a rolling average of {rolling_avg:.1f} "
                  f"({current_count / rolling_avg:.1f}x normal)\n")

            save_anomaly(topic, bucket, current_count, rolling_avg)

        if posts_since_drift_print >= 10:
            posts_since_drift_print = 0
            top_topics = drift_tracker.top_topics(TOP_N_TOPICS)
            now = datetime.now(timezone.utc)

            print(f"\n### TRENDING NOW (last {len(drift_tracker.window)} posts) ###")
            for rank, (topic, mentions) in enumerate(top_topics, start=1):
                print(f"  {rank}. #{topic} - {mentions} mentions")
            print()

            save_drift_snapshot(now, top_topics)


if __name__ == "__main__":
    main()