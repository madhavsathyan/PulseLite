# PulseLite: Implementation Details & Design Choices

This document provides a deep dive into the architecture, design choices, database schema, processing flow, and dashboard layout of the **PulseLite** real-time social media analytics platform.

---

## 1. Architecture Overview

PulseLite is designed as a modular, containerized, real-time streaming pipeline:

```
[Reddit API / Simulator] 
           |
           v (polling / stream)
[Python Producer Service] -> (Kafka Broker: social_media_posts)
                                         |
                                         v (micro-batches / 5s)
                           [PySpark Structured Streaming]
                                         |
                                         v (VADER, regex, counts)
                              [PostgreSQL Database]
                                         |
                                         v (auto-refresh / 3s)
                           [Streamlit Live Dashboard]
```

---

## 2. Ingestion (Producer) & Data Source Choice

### Choices & Rationale
We chose **Reddit Public API** via PRAW (Python Reddit API Wrapper) as the primary real-world data source, with a built-in fallback to **Simulated Social Media Posts** mimicking Twitter-like trends.

- **Real-Time Reddit Ingestion**: Subreddits like `r/india` and `r/cricket` offer active, text-heavy feeds that are ideal for sentiment analysis and entity extraction. We support streaming both **comments** (higher volume, updates every few seconds) and **submissions** (longer text, updates occasionally).
- **Deduplication & Error Recovery**: In Reddit mode, the producer connects to the Reddit API's live comment/submission stream. If the network drops, it catches the error and attempts reconnection after 5 seconds.
- **Robust Simulator Fallback**: If Reddit credentials are not supplied in the environment (which is the default out-of-the-box state), the producer launches a simulated chatter stream. It rotates through trending topics/events (e.g. `#IndVsPak`, `#AppleEvent`, `#SuperBowl`) to simulate realistic **topic drift** and **volume spikes**.

---

## 3. Stream Processing (PySpark Structured Streaming)

### Rationale
PySpark Structured Streaming provides scalable, fault-tolerant micro-batch processing. It reads raw JSON payloads from Kafka, deserializes them using a predefined schema, and passes them to a writer function (`foreachBatch`).

### Stream Calculations
Within the writer function, the Spark DataFrame is converted to Pandas for processing in the Python execution environment:
1. **Sentiment Per Post**: The VADER Sentiment Analyzer calculates polarity scores. Posts with a compound score $\ge 0.05$ are classified as `positive`, $\le -0.05$ as `negative`, and others as `neutral`.
2. **Entity Extraction**: Uses regex to extract:
   - Hashtags (`#word`)
   - Mentions (`@word`)
   - Capitalized words (e.g. `Virat Kohli`, `Google`) not in the stopword list.
3. **Volume Per Minute**: Plucks the `created_at` timestamp from the event, truncates it to the nearest minute, and computes frequencies.
4. **Topic Drift (Rolling Words)**: Cleans post text by tokenizing words (length $\ge 3$) and removing English and domain-specific stopwords. It counts word frequencies per minute window.

---

## 4. Storage Sink (PostgreSQL)

PostgreSQL serves as our real-time relational sink, optimized for concurrent updates and rapid dashboard queries.

### Schema Definitions
We partition all metrics by the `topic` (subreddit) dimension to allow per-community dashboard filtering.

```sql
-- 1. Table for individual processed posts and sentiment
CREATE TABLE IF NOT EXISTS processed_posts (
    id VARCHAR(50) PRIMARY KEY,
    text TEXT NOT NULL,
    sentiment_label VARCHAR(20) NOT NULL,
    sentiment_score REAL NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    topic VARCHAR(50) NOT NULL DEFAULT 'unknown'
);

-- 2. Table for windowed entity metrics
CREATE TABLE IF NOT EXISTS entity_metrics (
    entity VARCHAR(100) NOT NULL,
    topic VARCHAR(50) NOT NULL DEFAULT 'unknown',
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    post_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (entity, topic, window_start)
);

-- 3. Table for post volume tracking
CREATE TABLE IF NOT EXISTS volume_metrics (
    topic VARCHAR(50) NOT NULL DEFAULT 'unknown',
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    post_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, window_start)
);

-- 4. Table for windowed topic word frequencies
CREATE TABLE IF NOT EXISTS topic_metrics (
    word VARCHAR(100) NOT NULL,
    topic VARCHAR(50) NOT NULL DEFAULT 'unknown',
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    post_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (word, topic, window_start)
);
```

### Database Optimization & Upserts
We use PostgreSQL's `ON CONFLICT` feature for transactional upserts. This ensures that even if events arrive out of order or are processed multiple times (at-least-once delivery guarantees), the metrics aggregate correctly instead of throwing integrity errors:
- For `processed_posts`, conflicting primary keys are ignored (`DO NOTHING`).
- For aggregated tables, conflicting primary keys add the counts (`DO UPDATE SET post_count = metrics.post_count + excluded.post_count`).
- We index the tables on `topic` and `window_start` to guarantee fast retrieval as the database grows.

---

## 5. Dashboard Visualizations (Streamlit)

The dashboard is built on Streamlit with a premium dark theme and custom Plotly charts.

### Interface Layout & Widgets
1. **Sidebar Controls**:
   - **Auto-Refresh Toggle & Slider**: Allows users to enable/disable auto-refresh and customize the frequency (2s - 60s).
   - **Subreddit / Topic Filter**: A dynamic dropdown loaded directly from the database's distinct topics. Users can view the global dashboard ("All Topics") or drill down into individual subreddits.
2. **KPI Metrics Cards**:
   - **Total Ingested**: Total post/comment count for the selected filter.
   - **Positive Sentiment %**: Proportion of positive feedback.
   - **Average Sentiment Score**: Compound score scaled from `-1.00` to `+1.00`.
   - **Ingestion Rate**: Volume of records processed in the last active minute.
3. **Charts**:
   - **Sentiment Distribution**: Donut chart highlighting positive (emerald green), neutral (slate grey), and negative (red) shares.
   - **Post Ingestion Volume**: Shaded area timeline showing throughput over time.
   - **Top Trending Entities**: Horizontal bar chart reflecting key entities in the last 5 minutes.
   - **Topic Word Drift**: Vertical bar chart highlighting active keywords from a 5-minute rolling window.
4. **Live Feed Sample**:
   - Vertical scrolling list of the 6 most recent posts, colorcoded by sentiment (left-hand border indicators) and tagged with their specific subreddits.
