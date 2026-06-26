import os
import time
import json
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "social_media_posts")

# Reddit API environment variables
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "python:pulselite:v1.0 (by /u/anonymous)")
REDDIT_SUBREDDITS = os.getenv("REDDIT_SUBREDDITS", "india,cricket,technology,gaming,movies,news")
REDDIT_STREAM_TYPE = os.getenv("REDDIT_STREAM_TYPE", "comments").lower()

print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
# Retry connection to Kafka as it might take time to start up in Docker Compose
producer = None
for i in range(15):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Connected to Kafka successfully!")
        break
    except Exception as e:
        print(f"Attempt {i+1}/15 failed to connect to Kafka: {e}")
        time.sleep(3)

if not producer:
    print("Could not connect to Kafka. Exiting.")
    exit(1)

# Sample pools for simulated posts
TOPICS = ["r/technology", "r/cricket", "r/movies", "r/gaming", "r/news"]

SENTIMENT_PHRASES = {
    "positive": [
        "is absolutely amazing! Highly recommend it.",
        "works flawlessly, so happy with the performance.",
        "is a game changer. Absolutely love the new updates!",
        "wins the match! What a stunning performance!",
        "is the best thing that happened this year.",
        "shows incredible potential. Super excited for what's next!"
    ],
    "neutral": [
        "released a new update today. Let's see how it goes.",
        "is being discussed globally at the current event.",
        "announced their quarterly results yesterday.",
        "is playing against them tomorrow evening.",
        "is a topic of debate among tech experts.",
        "just launched their website."
    ],
    "negative": [
        "is causing so many bugs and crashes. Extremely frustrating.",
        "disappointed with the lack of quality and support.",
        "lost the match. Horrible gameplay and poor coordination.",
        "is totally overrated and overpriced.",
        "is facing a severe outage right now.",
        "is a complete disaster. Avoid at all costs."
    ]
}

SUBJECTS = {
    "r/technology": ["#AI", "Google", "#Python", "OpenAI", "ChatGPT", "#Docker", "Apple", "Microsoft", "Linux"],
    "r/cricket": ["Virat Kohli", "IPL", "#Cricket", "BCCI", "Dhoni", "T20WorldCup", "TeamIndia", "Jasprit Bumrah"],
    "r/movies": ["Christopher Nolan", "Dune", "Oscars", "#Netflix", "Marvel", "Batman", "Cillian Murphy"],
    "r/gaming": ["PlayStation", "Xbox", "GTA6", "Cyberpunk", "#EldenRing", "Nintendo", "Steam"],
    "r/news": ["Inflation", "Elections", "NASA", "SpaceX", "#ClimateChange", "GlobalEconomy"]
}

# We can simulate topic drift by introducing a temporary trending topic/hashtag that changes every 2 minutes
TRENDING_SPIKES = [
    {"entity": "#SuperBowl", "topic": "r/news", "phrases": ["The halftime show was incredible! #SuperBowl", "Who will win the #SuperBowl this year?"]},
    {"entity": "#AppleEvent", "topic": "r/technology", "phrases": ["Apple just announced the new iPhone! #AppleEvent", "The new M4 chip looks crazy fast #AppleEvent"]},
    {"entity": "#IndVsPak", "topic": "r/cricket", "phrases": ["What a nail-biting finish! #IndVsPak", "India wins against Pakistan! Absolute scenes! #IndVsPak"]}
]

def generate_post():
    # Determine if we should generate a trending spike to demonstrate topic drift
    # Drift changes based on the current timestamp (e.g. cycle through spikes every 2 minutes)
    minute_cycle = int(time.time() / 120) % (len(TRENDING_SPIKES) + 1)
    
    if minute_cycle > 0:
        # We have an active trend spike!
        spike = TRENDING_SPIKES[minute_cycle - 1]
        # 40% chance of generating the trending post to simulate a massive volume spike
        if random.random() < 0.4:
            sentiment = random.choice(["positive", "neutral", "negative"])
            phrase = random.choice(SENTIMENT_PHRASES[sentiment])
            text = f"{spike['entity']} {phrase}"
            return {
                "id": str(uuid.uuid4()),
                "text": text,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "topic": spike["topic"]
            }

    # Otherwise generate a normal post
    topic = random.choice(TOPICS)
    subject = random.choice(SUBJECTS[topic])
    sentiment = random.choice(["positive", "neutral", "negative"])
    phrase = random.choice(SENTIMENT_PHRASES[sentiment])
    
    # Mix subject and phrase
    text = f"{subject} {phrase}"
    
    return {
        "id": str(uuid.uuid4()),
        "text": text,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "topic": topic
    }

# Try to initialize Reddit client if credentials are provided
reddit_client = None
if REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET:
    try:
        import praw
        reddit_client = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        # Quick validation
        reddit_client.read_only = True
        print("Reddit API client initialized successfully!")
    except Exception as e:
        print(f"Failed to initialize Reddit client: {e}. Falling back to simulator.")
        reddit_client = None

# Main event loop
try:
    if reddit_client:
        subreddits_list = [s.strip().lower() for s in REDDIT_SUBREDDITS.split(",") if s.strip()]
        subreddit_name = "+".join(subreddits_list)
        subreddit_group = reddit_client.subreddit(subreddit_name)
        
        print(f"Starting Reddit API ingestion. Subscribed to stream ({REDDIT_STREAM_TYPE}) for r/{subreddit_name}...")
        
        while True:
            try:
                if REDDIT_STREAM_TYPE == "comments":
                    stream = subreddit_group.stream.comments(skip_existing=True)
                else:
                    stream = subreddit_group.stream.submissions(skip_existing=True)
                
                for item in stream:
                    if REDDIT_STREAM_TYPE == "comments":
                        text = item.body
                        post_id = f"c_{item.id}"
                    else:
                        text = item.title
                        if item.selftext:
                            text += " " + item.selftext
                        post_id = f"s_{item.id}"
                    
                    if len(text) > 1000:
                        text = text[:1000] + "..."
                        
                    created_at = datetime.fromtimestamp(item.created_utc, timezone.utc).isoformat()
                    topic = f"r/{item.subreddit.display_name.lower()}"
                    
                    post = {
                        "id": post_id,
                        "text": text,
                        "created_at": created_at,
                        "topic": topic
                    }
                    
                    producer.send(KAFKA_TOPIC, value=post)
                    print(f"Emitted: {text[:80]}... (Topic: {topic})")
                    
            except Exception as e:
                print(f"Reddit stream disconnected ({e}). Reconnecting in 5 seconds...")
                time.sleep(5)
    else:
        print("Starting simulated post ingestion...")
        while True:
            post = generate_post()
            producer.send(KAFKA_TOPIC, value=post)
            print(f"Emitted: {post['text']} (Topic: {post['topic']})")
            # Sleep for a short interval (e.g. 0.5 to 1.5 seconds) to simulate continuous stream
            time.sleep(random.uniform(0.5, 1.2))
            
except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    producer.close()

