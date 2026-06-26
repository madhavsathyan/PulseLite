# PulseLite ⚡

PulseLite is a lightweight, real-time social media stream analytics platform. It consumes posts/comments in real time (from the live **Reddit public API** or a high-fidelity **Twitter/Reddit simulator**), publishes them to a Kafka topic, processes them in real time using a **PySpark Structured Streaming** job, stores the processed metrics in **PostgreSQL**, and visualizes the results on a live-updating, interactive **Streamlit dashboard**.

---

## Architecture & Technology Stack

```
[Reddit / Simulator] -> (Kafka Topic: social_media_posts) -> [PySpark Processor] 
                                                                     |
                                                                     v
[Streamlit Dashboard] <--------------------------------------- [PostgreSQL DB]
```

- **Data Source / Producer**: A Python service that streams events. If configured with Reddit credentials, it streams live comments/submissions from configured subreddits (e.g. `r/india`, `r/cricket`). Otherwise, it falls back to a simulator that generates realistic social media posts with trending spikes (e.g. `#IndVsPak`, `#AppleEvent`, `#SuperBowl`) to demonstrate dynamic **topic drift**.
- **Ingestion**: Apache Kafka running in KRaft mode (Kafka Raft metadata mode) to minimize container footprint.
- **Processing Engine**: PySpark Structured Streaming. The processor consumes from Kafka in 5-second micro-batches, analyzes sentiment using the VADER Sentiment Analyzer, extracts entities (hashtags/mentions/capitalized nouns), and aggregates volume and topic words.
- **Storage / Sink**: PostgreSQL database containing tables optimized for real-time upserts and indexed for fast retrieval.
- **Visualization**: Streamlit web dashboard. It features an interactive layout with auto-refresh controls, filters to slice metrics by subreddit/topic, Plotly visualization charts, and a live post feed.

---

## Metrics Extracted

1. **Sentiment Analysis**: Tracks positive, negative, and neutral sentiments using the compound score computed by VADER.
2. **Top Entities**: Counts and visualizes top trending hashtags/mentions/capitalized nouns over a rolling 5-minute window.
3. **Ingestion Volume**: Computes post counts per minute to show pipeline throughput.
4. **Topic Word Drift**: Tracks rolling word frequencies over a sliding 5-minute window to show how topics drift in real time.

---

## Setup & Running the Stack

Make sure you have [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) installed on your machine.

### 1. Configure the Environment (Optional)
If you wish to use the real Reddit API, copy the environment template:
```bash
cp .env.example .env
```
Open `.env` in a text editor and fill in your Reddit API keys (register a script application at [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps/)).

*If you do not create a `.env` file or leave the credentials blank, PulseLite will automatically run in simulation mode. No setup is required for simulation!*

### 2. Start the Stack
Build and run all services in the background:
```bash
docker compose up --build -d
```

This will spin up five containers:
- `pulselite-postgres`: PostgreSQL database (port `5432` mapped to host)
- `pulselite-kafka`: Kafka single-node broker (port `9092` mapped to host)
- `pulselite-producer`: Python event generator (Reddit API wrapper or simulator)
- `pulselite-processor`: PySpark streaming processor (runs the spark-submit job)
- `pulselite-dashboard`: Streamlit web server (port `8501` mapped to host)

### 3. Open the Dashboard
Once the services are running, open your browser and navigate to:
```
http://localhost:8501
```
The dashboard will auto-refresh (default: every 3 seconds) to show live updates as events flow through the pipeline. Use the sidebar to adjust the refresh rate or filter metrics by subreddit/topic!

### 4. Verification & Diagnostics
To inspect logs and ensure that services are running correctly:
```bash
# Check if all containers are healthy
docker compose ps

# Monitor live producer output
docker compose logs -f producer

# Monitor PySpark stream processor output
docker compose logs -f processor

# Query the PostgreSQL database directly
docker exec -it pulselite-postgres psql -U postgres -d pulselite -c "SELECT * FROM processed_posts LIMIT 5;"
```

### 5. Stopping the Stack
To stop and clean up all resources:
```bash
docker compose down
```

---

## Folder Structure

```
PulseLite/
├── dashboard/
│   ├── dashboard.py         # Streamlit dashboard script
│   ├── Dockerfile
│   └── requirements.txt
├── postgres/
│   └── schema.sql           # Database schema initialization script
├── processor/
│   ├── processor.py         # PySpark Structured Streaming script
│   ├── Dockerfile
│   ├── log4j.properties
│   └── requirements.txt
├── producer/
│   ├── producer.py          # Reddit API poller or post simulator
│   ├── Dockerfile
│   └── requirements.txt
├── .env.example             # Environment configuration template
├── .gitignore
├── docker-compose.yml
├── IMPLEMENTATION.md        # Technical implementation & design choices
└── README.md
```

