# PulseLite — Real-Time Hashtag Pulse

A miniature real-time social listening pipeline: simulated social posts flow through Kafka, get scored for sentiment and entities, tracked for volume and topic drift, checked for anomalies, and visualized on a live-updating dashboard.

Built as a hands-on introduction to streaming data engineering — Kafka, consumer groups, windowed aggregation, and anomaly detection — the foundation for a 3rd-year clickstream telemetry project.

## What it does

- **Ingests** simulated social posts as a live stream (Kafka topic `reddit-posts`)
- **Scores sentiment** per post using VADER (-1 to +1)
- **Extracts entities** (hashtags) using regex
- **Tracks volume** of posts per minute
- **Computes topic drift** — a rolling window of the top trending hashtags
- **Detects anomalies** — flags any topic whose volume spikes to 3x its 5-minute rolling average
- **Visualizes everything** on a Streamlit dashboard that auto-refreshes every 5 seconds

## Architecture
producer_fetch.py  --->  Kafka (reddit-posts topic)  --->  consumer.py
|
v
pulselite.db (DuckDB)
|
v
dashboard.py (Streamlit)

- **producer_fetch.py** — generates realistic simulated social posts and publishes them to Kafka
- **Kafka + Zookeeper** — the message broker, run via Docker Compose
- **consumer.py** — reads posts from Kafka, runs sentiment/entity/volume/drift/anomaly processing, writes results to DuckDB
- **pulselite.db** — a local DuckDB file storing all processed posts, topic drift snapshots, and anomaly events
- **dashboard.py** — a Streamlit app reading directly from DuckDB, auto-refreshing every 5 seconds

## How to run it

**Prerequisites:** Python 3.10+, Docker Desktop

```bash
# 1. Clone and set up
git clone https://github.com/YOUR_USERNAME/PulseLite.git
cd PulseLite
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Mac/Linux
pip install -r requirements.txt

# 2. Start Kafka
docker compose up -d

# 3. In separate terminals, run each part:
python producer_fetch.py
python consumer.py
streamlit run dashboard.py
```

The dashboard opens automatically at `http://localhost:8501`.

## Why simulated data instead of live Reddit?

Reddit's public API blocked automated requests during development (403 errors), and their official developer platform now requires manual application review. Rather than block progress on external approval, the pipeline uses a realistic simulated data generator that produces posts in the exact same schema a real Reddit post would have — meaning a real API can be swapped in later with zero changes to Kafka, processing, or the dashboard. Full reasoning in [ADR-01](./docs/ADR-01-data-source.md).

## Architecture Decision Records

- [ADR-01: Simulated data instead of live Reddit API](./docs/ADR-01-data-source.md)
- [ADR-02: DuckDB with per-write connections instead of a database server](./docs/ADR-02-duckdb-connections.md)
- [ADR-03: VADER instead of a transformer model for sentiment](./docs/ADR-03-vader-sentiment.md)

## What's next (3rd-year extension path)

- Exactly-once delivery semantics end-to-end
- Late-arrival handling with watermarks
- A second stream with stream-stream joins
- Migrating from Kafka consumer groups to Flink
- This becomes the foundation for a full **Clickstream Telemetry Pipeline**

## Author

Built by Madhav Sathyan as part of a Data Engineering learning track.