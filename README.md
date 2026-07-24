<div align="center">

# 📡 **PulseLite**

### Real-time sentiment & trend pulse — streamed, scored, and visualized live.

*Kafka → Consumer → DuckDB → Live Dashboard*

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Kafka-Streaming-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Deployed%20with-Docker%20Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat)](LICENSE)

**[🎥 Watch the Demo](#-demo)** · **[📐 Architecture](#-architecture)** · **[⚡ Quickstart](#-quickstart)** · **[🧠 ADRs](#-adrs)**

</div>

---

## 🌟 What is PulseLite?

Brands and communities want to know what people are saying about them **right now** — not in tomorrow's batch report.

**PulseLite** is a lightweight real-time pipeline that:
- 🔴 Streams live posts (simulated social feed, Reddit-compatible schema)
- 🧠 Scores sentiment per post using VADER (-1 to +1)
- 🏷️ Extracts trending entities (hashtags) using regex
- 📈 Tracks post volume per minute
- 🚨 Flags sudden spikes with a rolling 5-min anomaly detector
- 🖥️ Surfaces everything on a live, auto-refreshing Streamlit dashboard

Built as the **streaming foundation** project for the *Foundations of Data Engineering* track — and the seed for a future "Clickstream Telemetry Pipeline" extension.

---

## 🎬 Demo

| | |
|---|---|
| 🎥 **Loom walkthrough** | https://www.linkedin.com/posts/madhav-sathyan-944b3733a_dataengineering-kafka-streaming-ugcPost-7485765426162040832-rLUS/?utm_source=share&utm_medium=member_desktop&rcm=ACoAAFU9y4QBka-JdpAuX1sepDsBXYx8E4GRZk0 |
| 🌐 **Live URL** | https://pulselite-9j5srszefmrgmff3rx8dn3.streamlit.app/|

---
🏗️ Architecture
Simulated Social Posts (producer_fetch.py)
        │
        ▼
   Kafka Topic: reddit-posts
   (Zookeeper + Kafka, via Docker Compose)
        │
        ▼
   Stream Processor (consumer.py)
   ┌─────────────────────────────────────┐
   │  Sentiment scoring (VADER)          │
   │  Entity extraction (regex hashtags) │
   │  Volume-per-minute tracking          │
   │  Rolling topic drift (last 50 posts) │
   │  Anomaly detection (3x rolling avg) │
   └─────────────────────────────────────┘
        │
        ▼
   DuckDB (pulselite.db)
        │
        ▼
   Dashboard (Streamlit + Plotly)
   ┌─────────────────────────────────────┐
   │  Headline metrics (volume, mood)    │
   │  Volume-per-minute chart            │
   │  Sentiment-over-time chart          │
   │  Top entities + topic drift table   │
   │  Anomaly alert log                  │
   └─────────────────────────────────────┘
        │
        ▼
   Deployed on Streamlit Cloud (Free)

📄 Everything above — Kafka, Zookeeper, producer, consumer, dashboard — starts with a single command: docker compose up.

- **`producer_fetch.py`** — generates realistic simulated social posts and publishes them to Kafka
- **Kafka + Zookeeper** — the message broker, run via Docker Compose
- **`consumer.py`** — reads posts from Kafka, runs sentiment / entity / volume / drift / anomaly processing, writes results to DuckDB
- **`pulselite.db`** — a local DuckDB file storing all processed posts, topic drift snapshots, and anomaly events
- **`dashboard.py`** — a Streamlit app reading directly from DuckDB, auto-refreshing every 5 seconds

---

## 🛠️ Tech Stack

| Component | Choice | Why |
|---|---|---|
| 📥 Source | Simulated data generator | Free, realistic, no API approval bottleneck |
| 🟦 Broker | Kafka (Docker Compose) | Core streaming skill this project tests |
| 😊 Sentiment | VADER | Lightweight, no training needed, great for short social text |
| 🗄️ Sink | DuckDB | Zero-ops, file-based, fast to query from a dashboard |
| 📺 Dashboard | Streamlit | Quick to build, native auto-refresh support |
| 🐳 Orchestration | Docker Compose | One command brings up the whole stack |

---

## ⚡ Quickstart

### Prerequisites
- 🐳 Docker + Docker Compose
- 🐍 Python 3.10+

### Install
```bash
git clone https://github.com/madhavsathyan/PulseLite.git
cd PulseLite
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Mac/Linux
pip install -r requirements.txt
```

### Run (full live pipeline)
```bash
# 1. Start Kafka + Zookeeper
docker compose up -d

# 2. In separate terminals:
python producer_fetch.py       # streams posts into Kafka
python consumer.py             # processes & writes to DuckDB
streamlit run dashboard.py     # opens dashboard at http://localhost:8501
```

### Run (demo snapshot — no Docker needed)
```bash
pip install -r requirements.txt
streamlit run dashboard.py     # reads from bundled pulselite.db snapshot
```
📊 Data Source
Data	Source	Format
Social posts	Simulated generator	JSON via Kafka
Schema	Matches real Reddit post structure exactly	id, title, author, score, num_comments, created_utc, subreddit, url

Data is simulated but realistic — varied sentiment (positive/negative/neutral) and embedded hashtags, generated in the same schema a real Reddit post would have. This means a real API can be swapped in later with zero changes downstream. Full reasoning: ADR-01.
---

📁 Folder Structure
pulselite/
│
├── producer_fetch.py       # Generates simulated posts, publishes to Kafka
├── consumer.py              # Kafka consumer: sentiment, entities, volume, drift, anomalies, DuckDB writes
├── dashboard.py              # Streamlit live dashboard
├── docker-compose.yml       # Full 5-service stack (Zookeeper, Kafka, producer, consumer, dashboard)
├── Dockerfile                # Shared image for producer/consumer/dashboard
├── requirements.txt          # Python dependencies
├── README.md                  # This file
│
├── .streamlit/
│   └── config.toml            # Dashboard theme
│
├── data/                       # DuckDB file lives here (git-ignored, generated at runtime)
│
└── docs/
    ├── ADR-01-data-source.md
    ├── ADR-02-duckdb-connections.md
    ├── ADR-03-vader-sentiment.md
    └── ADR-04-duckdb-concurrency-fix.md
    
📈 Dashboard Features
Headline Metrics
Total posts processed, posts in the latest minute
Overall mood (average sentiment, color-coded)
Anomalies flagged so far
Volume & Sentiment
Volume-per-minute bar chart — the same signal the anomaly detector watches
Sentiment-over-time line chart, with a zero line marking positive vs negative
Topics & Drift
Top 10 all-time hashtags (bar chart)
Trending Now vs a Few Minutes Ago — a live comparison table with 🔺🔻➖ trend indicators, showing topic drift as it happens
Anomaly Alerts
Log of every topic whose volume spiked past 3x its 5-minute rolling average, with the exact multiplier

➕ Mini-Extension — Anomaly Detector

What it is: A rolling-average volume monitor per topic, running inside consumer.py.

What it does:

Tracks each hashtag's mention count per completed minute (last 5 minutes of history)
Compares the current minute's count against that rolling average
Flags and logs anything exceeding 3x normal — both to the console and to a dedicated anomalies table in DuckDB

Why it matters: This is the same core pattern behind real production alerting systems — the gateway concept from "here's what happened" to "here's what needs attention right now." It's a small addition (~40 lines) that demonstrates real stream-monitoring thinking, not just data plumbing.

📝 What I Learned

Week 1 — Ingestion:

Reddit's public API blocks automated scraping harder than expected — learned to pivot to a documented alternative (simulated data) rather than get stuck waiting on external approval
Kafka fundamentals: topics, producers, consumers, consumer groups
Docker Compose basics — multi-container networking, why localhost means something different inside vs outside a container

Week 2 — Processing:

VADER sentiment scoring and why rule-based tools beat transformer models for low-latency streaming
Rolling-window aggregation for topic drift and per-minute volume
Anomaly detection using a rolling average and threshold multiplier

Week 3 — Storage & Visualization:

DuckDB as an embedded, server-free database — and its single-writer concurrency model
Debugged a real lock-contention bug where a long-lived dashboard connection blocked the consumer's writes; fixed with explicit connection closing and retry-with-backoff logic
Streamlit auto-refresh patterns for building a genuinely live dashboard

Week 4 — Deployment & Polish:

Full Docker Compose containerization — Kafka, producer, consumer, and dashboard all starting from one command
Deploying to Streamlit Cloud, and why relative file paths behave differently in the cloud vs locally
Writing ADRs to document real engineering tradeoffs, not just decisions made in hindsight
📄 Documents
Document	Link
ADR-01: Simulated data instead of live Reddit	docs/ADR-01-data-source.md
ADR-02: DuckDB with per-write connections	docs/ADR-02-duckdb-connections.md
ADR-03: VADER instead of a transformer model	docs/ADR-03-vader-sentiment.md
ADR-04: Resolving DuckDB lock contention	docs/ADR-04-duckdb-concurrency-fix.md
🚀 3rd Year Extension Plan

This project is the seed of a 3rd year clickstream telemetry portfolio. Here's where it goes:

Timeline	What gets added
Next	Exactly-once delivery semantics end-to-end
Next	Late-arrival handling with watermarks
Later	A second stream with stream-stream joins
Later	Migrate from Kafka consumer groups to Apache Flink
3rd year internship	Full B2 Clickstream Telemetry Pipeline — same architecture, real scale

⚠️ Known Limitations
Data source is simulated, not a live external API (documented tradeoff — see ADR-01)
The hosted Streamlit Cloud demo is a static snapshot, since the cloud host can't run Kafka/Docker — the full live pipeline only runs locally via docker compose up
No exactly-once delivery guarantees — a message could theoretically be processed more than once
Topic drift uses a fixed post-count window (last 50 posts), not a strict time-based window
No authentication — this is a single-user demo tool, not a multi-tenant product

💼 Resume Bullets
- Built PulseLite, a real-time streaming pipeline processing social media
  posts through Kafka, with VADER sentiment scoring, regex-based entity
  extraction, rolling topic-drift detection, and volume anomaly alerts

- Designed and containerized a 5-service pipeline (Kafka, Zookeeper,
  producer, consumer, dashboard) with Docker Compose; diagnosed and
  resolved a DuckDB concurrency bug causing consumer crash-loops via
  connection-lifecycle management and retry-with-backoff logic

- Documented 4 Architecture Decision Records covering data-source
  tradeoffs, database concurrency, and model selection, and deployed a
  live public demo on Streamlit Cloud
👤 About

Name: Madhav Sathyan Track: 2nd Year Data Engineering Internship Project: H3 — Real-Time Hashtag Pulse (Streaming Starter)

📜 License

MIT License — see LICENSE

🙏 Acknowledgements
TrendWatch (fictional client scenario) — internship problem framework
Apache Kafka, DuckDB, Streamlit — open-source tools that made this possible
Internship mentor — for guidance through every debugging session, especially the DuckDB concurrency saga
