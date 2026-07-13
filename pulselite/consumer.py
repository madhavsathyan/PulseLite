"""
PulseLite - Day 4-8
A Kafka consumer that watches the 'reddit-posts' topic, scores sentiment
with VADER, extracts hashtag entities with regex, tracks volume per
minute, computes rolling topic drift, detects volume anomalies per
topic, and saves everything into DuckDB.
"""

import json
import re
from collections import Counter, deque, defaultdict
from datetime import datetime, timezone

import duckdb
from kafka import KafkaConsumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "reddit-posts"
CONSUMER_GROUP = "pulselite-processors"
DB_PATH = "pulselite.db"

DRIFT_WINDOW_SIZE = 50
TOP_N_TOPICS = 5

# --- Day 8: anomaly detection config ---
ROLLING_MINUTES = 5      # how many past minutes count toward the "normal" average
ANOMALY_MULTIPLIER = 3   # flag if current minute is > 3x the rolling average

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
    """Tracks how many mentions each topic gets, per minute, and can
    detect when the current minute is way above the recent normal.

    topic_minute_counts[topic] is a deque of the last ROLLING_MINUTES
    completed minutes' counts for that topic - this is our 'rolling
    average' window.
    """

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
        """Called when a new minute starts: push the just-finished
        minute's counts into history, then reset for the new minute."""
        if self.current_bucket is not None:
            for topic, count in self.current_minute_counts.items():
                self.topic_minute_counts[topic].append(count)
        self.current_minute_counts = defaultdict(int)
        self.current_bucket = new_bucket

    def check_anomalies(self) -> list:
        """Compares the CURRENT (in-progress) minute's counts against
        each topic's rolling average from previous minutes. Returns a
        list of (topic, current_count, rolling_avg) for anything over
        the threshold.
        """
        anomalies = []
        for topic, current_count in self.current_minute_counts.items():
            history = self.topic_minute_counts[topic]
            if len(history) == 0:
                continue  # not enough history yet to judge "normal"
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

    con = duckdb.connect(DB_PATH)
    setup_database(con)
    drift_tracker = TopicDriftTracker(DRIFT_WINDOW_SIZE)
    volume_tracker = TopicVolumeTracker(ROLLING_MINUTES, ANOMALY_MULTIPLIER)

    print(f"Connected. Writing to {DB_PATH}. Listening for posts... (Ctrl+C to stop)\n")

    current_bucket = None
    current_bucket_count = 0
    posts_since_drift_print = 0
    already_flagged = set()  # (topic, minute_bucket) pairs we've already alerted on

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

        # --- Day 8: anomaly detection, checked after every post ---
        for topic, current_count, rolling_avg in volume_tracker.check_anomalies():
            flag_key = (topic, bucket)
            if flag_key in already_flagged:
                continue  # don't spam the same alert repeatedly within one minute
            already_flagged.add(flag_key)

            print(f"\n🚨 ANOMALY DETECTED: #{topic} has {current_count} mentions this "
                  f"minute vs a rolling average of {rolling_avg:.1f} "
                  f"({current_count / rolling_avg:.1f}x normal)\n")

            con.execute("""
                INSERT INTO anomalies
                (detected_at, topic, minute_bucket, current_volume, rolling_avg, multiplier)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                datetime.now(timezone.utc), topic, bucket,
                current_count, rolling_avg, current_count / rolling_avg,
            ])

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