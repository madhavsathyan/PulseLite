"""
PulseLite - Day 1-2 (Simulated Data Version)

Why simulated data instead of live Reddit?
Reddit's public .json endpoint started returning 403 Blocked errors for
this project, even with browser-style headers, and the official API
requires an app-review process that isn't instant. Rather than lose days
waiting on Reddit's review queue, we generate realistic synthetic posts
locally. This is a common, legitimate pattern in streaming pipeline
development - it lets you build and test the whole pipeline (Kafka,
processing, dashboard) without depending on an external service's
availability. See ADR-01 in the README for the full reasoning.

This script produces posts in the exact same shape a real Reddit post
would have, so when we plug in Kafka in Day 3+, nothing downstream needs
to change - we can swap this for a real API later with zero refactoring.
"""

import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# --- Config ---
SUBREDDIT = "india"
POSTS_PER_BATCH = (1, 5)     # random number of "new" posts each tick
POLL_INTERVAL_SECONDS = 10
import os
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "reddit-posts"

TOPICS = [
    "cricket", "elections", "monsoon", "budget", "startup", "IPL",
    "traffic", "metro", "weather", "movies", "cricket team", "economy",
]

POSITIVE_PHRASES = [
    "This is amazing news, really happy about {topic}!",
    "Great update on {topic} today, feeling optimistic.",
    "Loving how {topic} is progressing this year.",
    "Big win for {topic} fans today!",
]

NEGATIVE_PHRASES = [
    "Really disappointed with how {topic} is being handled.",
    "This {topic} situation is getting worse, not good.",
    "Frustrated with the lack of progress on {topic}.",
    "Not happy about the recent {topic} decision at all.",
]

NEUTRAL_PHRASES = [
    "Does anyone have updates on {topic}?",
    "Sharing a quick summary of today's {topic} news.",
    "Here's what's happening with {topic} right now.",
    "Discussion thread for {topic} - thoughts?",
]

USERNAMES = [
    "arjun_k", "priya_sharma", "the_wanderer", "code_ninja99",
    "chai_lover", "mumbai_diaries", "quiet_observer", "night_owl_21",
]

_post_counter = 0


def _make_hashtags(topic: str) -> str:
    tag = "#" + topic.replace(" ", "")
    extra = random.choice(["#India", "#Trending", "#Live", ""])
    return f"{tag} {extra}".strip()


def generate_fake_post() -> dict:
    """Builds one fake post with the same fields a real Reddit post has."""
    global _post_counter
    _post_counter += 1

    topic = random.choice(TOPICS)
    sentiment_bucket = random.choices(
        ["positive", "negative", "neutral"], weights=[0.4, 0.3, 0.3]
    )[0]

    phrase_pool = {
        "positive": POSITIVE_PHRASES,
        "negative": NEGATIVE_PHRASES,
        "neutral": NEUTRAL_PHRASES,
    }[sentiment_bucket]

    title = random.choice(phrase_pool).format(topic=topic)
    title = f"{title} {_make_hashtags(topic)}"

    return {
        "id": f"sim_{_post_counter}_{int(time.time() * 1000)}",
        "title": title,
        "author": random.choice(USERNAMES),
        "score": random.randint(0, 500),
        "num_comments": random.randint(0, 100),
        "created_utc": datetime.now(timezone.utc).timestamp(),
        "subreddit": SUBREDDIT,
        "url": f"https://reddit.com/r/{SUBREDDIT}/comments/sim_{_post_counter}",
    }


def print_post(post: dict):
    ts = datetime.fromtimestamp(post["created_utc"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] r/{post['subreddit']} | score={post['score']:<5} | "
          f"comments={post['num_comments']:<4} | u/{post['author']}")
    print(f"    {post['title']}")
    print(f"    {post['url']}")
    print("-" * 80)


def main():
    print(f"Starting PulseLite SIMULATED fetcher for r/{SUBREDDIT}")
    print(f"Connecting to Kafka at {KAFKA_BROKER}, topic '{KAFKA_TOPIC}'...")

    # value_serializer converts our Python dict into JSON bytes automatically,
    # since Kafka only knows how to move raw bytes around, not Python objects.
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Connected. Polling every {POLL_INTERVAL_SECONDS} seconds. Press Ctrl+C to stop.\n")

    while True:
        batch_size = random.randint(*POSTS_PER_BATCH)
        new_posts = [generate_fake_post() for _ in range(batch_size)]

        print(f"--- {len(new_posts)} new post(s) at {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC ---\n")
        for post in new_posts:
            print_post(post)
            producer.send(KAFKA_TOPIC, value=post)

        producer.flush()  # make sure everything is actually sent before we sleep
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
