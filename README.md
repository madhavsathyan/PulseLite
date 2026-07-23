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

## 📐 Architecture

```
producer_fetch.py  ──▶  Kafka (reddit-posts topic)  ──▶  consumer.py
                                                              │
                                                              ▼
                                                    pulselite.db (DuckDB)
                                                              │
                                                              ▼
                                                    dashboard.py (Streamlit)
```

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

---

## 🚨 Anomaly Detector

> Computes a rolling **5-minute average** post volume per topic. If the current minute's volume exceeds **3× the rolling average**, the dashboard flags it — highlighted row + log entry.

**Why it matters:** spike detection is the simplest possible gateway into real-time monitoring & alerting — a core skill in production streaming systems.

---

## 🧠 ADRs

Architecture Decision Records explaining every major technical choice:

| ADR | Decision |
|---|---|
| [ADR-01](docs/ADR-01-data-source.md) | Simulated data instead of live Reddit API |
| [ADR-02](docs/ADR-02-duckdb-connections.md) | DuckDB with per-write connections instead of a database server |
| [ADR-03](docs/ADR-03-vader-sentiment.md) | VADER instead of a transformer model for sentiment |
| [ADR-04](docs/ADR-04-duckdb-concurrency-fix.md) | Resolving DuckDB lock contention between consumer and dashboard |

---

## ⚠️ Known Limitations

- No exactly-once delivery guarantees (at-least-once only)
- No handling of out-of-order / late-arriving events
- Single Kafka broker, no replication (fine for local/dev, not production-grade)
- Anomaly threshold (3×) is a fixed heuristic, not statistically tuned

---

## 🗺️ What I'd Do in 3rd Year

Planned extensions (see [`docs/`](docs/) for background):
- ✅ Exactly-once semantics end-to-end
- ✅ Watermark-based late-arrival handling
- ✅ A second joined stream (stream-stream join)
- ✅ Migration from Kafka consumer groups to Flink
- ✅ This becomes the foundation for a full **Clickstream Telemetry Pipeline**

---

## 📄 License & Acknowledgements

Released under the **MIT License**. Built by **Madhav Sathyan** as part of the *Foundations of Data Engineering* internship track — Problem **H3: Real-time Hashtag Pulse**.
