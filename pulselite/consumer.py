"""
PulseLite - Day 4
A Kafka consumer that watches the 'reddit-posts' topic and prints
each post as it arrives, in real time.

This is the seed of our processing pipeline. Right now it just prints -
in later days we'll add sentiment scoring, entity extraction, and
database writes right where the "# PROCESS HERE" comment is.
"""

import json

from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "reddit-posts"
CONSUMER_GROUP = "pulselite-processors"  # see note below on consumer groups


def main():
    print(f"Connecting to Kafka at {KAFKA_BROKER}, topic '{KAFKA_TOPIC}'...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",  # if no offset saved yet, start from the oldest message
    )

    print("Connected. Listening for posts... (Ctrl+C to stop)\n")

    for message in consumer:
        post = message.value  # already a Python dict, thanks to value_deserializer

        # --- PROCESS HERE (Day 5+: sentiment, entities, etc.) ---
        print(f"[partition {message.partition} | offset {message.offset}] "
              f"u/{post['author']}: {post['title']}")


if __name__ == "__main__":
    main()