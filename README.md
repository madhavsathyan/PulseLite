<div align="center">

# 📡 <b>PulseLite</b>

### Real-time sentiment & trend pulse — streamed, scored, and visualized live.

*Kafka → PySpark Streaming → DuckDB → Live Dashboard*

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Kafka-Streaming-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/PySpark-Structured%20Streaming-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Deployed%20with-Docker%20Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat)](LICENSE)

**[🎥 Watch the Demo](#-demo)** · **[🚀 Live Dashboard](#-demo)** · **[📐 Architecture](#-architecture)** · **[🧠 ADRs](#-adrs)**

</div>

---

## 🌟 What is PulseLite?

Brands and communities want to know what people are saying about them **right now** — not in tomorrow's batch report.

**PulseLite** is a lightweight real-time pipeline that:
- 🔴 Streams live posts (Reddit API, with a simulated fallback generator)
- 🧠 Scores sentiment and extracts trending entities, in real time
- 📈 Tracks post volume per minute
- 🚨 Flags sudden spikes with a built-in anomaly detector
- 🖥️ Surfaces all of it on a live, auto-refreshing dashboard

Built as the **streaming foundation** project for the *Foundations of Data Engineering* track — and the seed for a future "Clickstream Telemetry Pipeline" extension in 3rd year.

---

## 🎬 Demo

| | |
|---|---|
| 🎥 **Loom walkthrough** | _[add link once recorded]_ |
| 🌐 **Live URL** | _[add deployed link once live]_ |

---

## 📐 Architecture

<p align="center">
  <img src="docs/architecture.png" alt="PulseLite Architecture Diagram" width="650"/>
</p>

```
  simulated post generator
        │  polls every N seconds
        ▼
   🟦 Kafka topic: raw_posts
        │
        ▼
🔥 PySpark Structured Streaming job
  ├─ 😊 sentiment scoring (VADER)
  ├─ 🏷️  entity extraction (regex)
  ├─ 📊 volume per minute
  └─ 🚨 rolling 5-min avg → anomaly flag
        │
        ▼
   🗄️  DuckDB (sink table)
        │
        ▼
📺 Streamlit dashboard (auto-refresh)
```

---

## 🛠️ Tech Stack

| Component | Choice | Why |
|---|---|---|
| 📥 Source | simulated data generator | Free, real data, no approval bottleneck as fallback |
| 🟦 Broker | Kafka (Docker Compose) | Core streaming skill this project is testing |
| 🔥 Processing | PySpark Structured Streaming | Python-first, more forgiving than raw Kafka Streams |
| 😊 Sentiment | VADER | Lightweight, no training needed, good for short social text |
| 🗄️ Sink | DuckDB | Zero-ops, file-based, fast to query from a dashboard |
| 📺 Dashboard | Streamlit | Quick to build, native auto-refresh support |
| 🐳 Orchestration | Docker Compose | One command brings up the whole stack |

---

## ⚡ Quickstart

### Prerequisites
- 🐳 Docker + Docker Compose
- 🐍 Python 3.11+
  

### Install
```bash
git clone https://github.com/<your-username>/PulseLite.git
cd PulseLite
pip install -r requirements.txt
```

### Run
```bash
docker compose up -d        # starts Kafka, Zookeeper, DuckDB volume
python producer.py          # streams posts into Kafka
python streaming_job.py     # runs the PySpark processing job
streamlit run dashboard.py  # opens the live dashboard
```

### Test
```bash
pytest tests/
```

---

## 🔌 Data Sources

| Source | Role |
|---|---|
| **Simulated generator** (`generator.py`) | Fallback — realistic fake posts (text, timestamp, subreddit, score) when Reddit is rate-limited |

---

## 🧠 ADRs

| ADR | Decision |
|---|---|
| [ADR-001](docs/adr/001-kafka-vs-direct-processing.md) | Kafka vs. direct processing |
| [ADR-002](docs/adr/002-duckdb-vs-postgres.md) | DuckDB vs. Postgres |
| [ADR-003](docs/adr/003-vader-vs-trained-model.md) | VADER vs. a trained sentiment model |

---

## 🚨 Mini-Extension: Anomaly Detector

> Computes a rolling **5-minute average** post volume per topic. If the current minute's volume exceeds **3× the rolling average**, the dashboard flags it — highlighted row + log entry.

**Why it matters:** spike detection is the simplest possible gateway into real-time monitoring & alerting — a core skill in production streaming systems.

---

## ⚠️ Known Limitations

- No exactly-once delivery guarantees (at-least-once only)
- No handling of out-of-order / late-arriving events
- Single Kafka broker, no replication (fine for local/dev, not production-grade)
- Anomaly threshold (3×) is a fixed heuristic, not statistically tuned

---

## 🗺️ What I'd Do in 3rd Year

See [`docs/roadmap_3rd_year.md`](docs/roadmap_3rd_year.md) — planned extensions:
- ✅ Exactly-once semantics
- ✅ Watermark-based late-arrival handling
- ✅ A second joined stream
- ✅ Migration to Flink

---

## 📄 License & Acknowledgements

Released under the **MIT License**. Built as part of the *Foundations of Data Engineering* internship track — Problem **H3: Real-time Hashtag Pulse**.

<div align="center">


</div>
