# PulseLite ⚡

PulseLite is a lightweight, real-time social media stream analytics platform. It consumes posts/comments in real time (from the live **Reddit public API** or a high-fidelity **Twitter/Reddit simulator**), publishes them to a Kafka topic, processes them in real time using a **PySpark Structured Streaming** job, stores the processed metrics in **PostgreSQL**, and visualizes the results on a live-updating, interactive **Streamlit dashboard**.

---

🌟 What is PulseLite?

Brands and communities want to know what people are saying about them right now — not in tomorrow's batch report.

PulseLite is a lightweight real-time pipeline that:


🔴 Streams live posts (Reddit API, with a simulated fallback generator)
🧠 Scores sentiment and extracts trending entities, in real time
📈 Tracks post volume per minute
🚨 Flags sudden spikes with a built-in anomaly detector
🖥️ Surfaces all of it on a live, auto-refreshing dashboard


Built as the streaming foundation project for the Foundations of Data Engineering track — and the seed for a future "Clickstream Telemetry Pipeline" extension in 3rd year.


🎬 Demo

🎥 Loom walkthrough[add link once recorded]🌐 Live URL[add deployed link once live]


📐 Architecture

<p align="center">
  <img src="docs/architecture.png" alt="PulseLite Architecture Diagram" width="650"/>
</p>
Reddit API (or simulated post generator)
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


🛠️ Tech Stack

ComponentChoiceWhy📥 SourceReddit API (PRAW) / simulated generatorFree, real data, no approval bottleneck as fallback🟦 BrokerKafka (Docker Compose)Core streaming skill this project is testing🔥 ProcessingPySpark Structured StreamingPython-first, more forgiving than raw Kafka Streams😊 SentimentVADERLightweight, no training needed, good for short social text🗄️ SinkDuckDBZero-ops, file-based, fast to query from a dashboard📺 DashboardStreamlitQuick to build, native auto-refresh support🐳 OrchestrationDocker ComposeOne command brings up the whole stack


⚡ Quickstart

Prerequisites


🐳 Docker + Docker Compose
🐍 Python 3.11+
🔑 (Optional) Reddit API credentials — falls back to a simulated generator if not provided


Install

bashgit clone https://github.com/<your-username>/PulseLite.git
cd PulseLite
pip install -r requirements.txt

Run

bashdocker compose up -d        # starts Kafka, Zookeeper, DuckDB volume
python producer.py          # streams posts into Kafka
python streaming_job.py     # runs the PySpark processing job
streamlit run dashboard.py  # opens the live dashboard

Test

bashpytest tests/


🔌 Data Sources

SourceRoleReddit API (PRAW)Primary — polls subreddits like r/india / r/cricketSimulated generator (generator.py)Fallback — realistic fake posts (text, timestamp, subreddit, score) when Reddit is rate-limited


🧠 ADRs

ADRDecisionADR-001Kafka vs. direct processingADR-002DuckDB vs. PostgresADR-003VADER vs. a trained sentiment model


🚨 Mini-Extension: Anomaly Detector


Computes a rolling 5-minute average post volume per topic. If the current minute's volume exceeds 3× the rolling average, the dashboard flags it — highlighted row + log entry.



Why it matters: spike detection is the simplest possible gateway into real-time monitoring & alerting — a core skill in production streaming systems.


⚠️ Known Limitations


No exactly-once delivery guarantees (at-least-once only)
No handling of out-of-order / late-arriving events
Single Kafka broker, no replication (fine for local/dev, not production-grade)
Anomaly threshold (3×) is a fixed heuristic, not statistically tuned



🗺️ What I'd Do in 3rd Year

See docs/roadmap_3rd_year.md — planned extensions:


✅ Exactly-once semantics
✅ Watermark-based late-arrival handling
✅ A second joined stream
✅ Migration to Flink



📄 License & Acknowledgements

Released under the MIT License. Built as part of the Foundations of Data Engineering internship track — Problem H3: Real-time Hashtag Pulse.
