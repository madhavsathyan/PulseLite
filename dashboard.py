"""
PulseLite - Day 9 (polished)
A live-updating Streamlit dashboard that reads directly from pulselite.db
and shows sentiment, top entities, volume per minute, topic drift, and
anomaly markers - refreshing automatically every few seconds.

Run with: streamlit run dashboard.py
"""

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "pulselite.db"))
REFRESH_INTERVAL_MS = 3000

st.set_page_config(page_title="PulseLite", page_icon="📡", layout="wide")
st_autorefresh(interval=REFRESH_INTERVAL_MS, key="pulselite_auto_refresh")
if not os.environ.get("KAFKA_BROKER") and os.environ.get("PULSELITE_MODE") != "live":
    st.info("📸 This is a static snapshot for demo purposes. The full pipeline (Kafka + live streaming) runs locally via `docker compose up` — see the README for the live version.")

# ---------------------------------------------------------------
# Small CSS tweaks: rounded metric cards + tighter spacing.
# Purely cosmetic - doesn't touch any data logic.
# ---------------------------------------------------------------
st.markdown("""
<style>
[data-testid="stMetric"] {
    background-color: #1C1F26;
    border: 1px solid #2A2E37;
    border-radius: 12px;
    padding: 16px 16px 8px 16px;
}
[data-testid="stMetricLabel"] { font-size: 0.85rem; opacity: 0.75; }
[data-testid="stMetricValue"] { font-size: 1.6rem; white-space: nowrap; overflow: visible; }
div.block-container { padding-top: 2rem; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------
# Sidebar: plain-language explanation of the whole pipeline.
# This is what makes the dashboard self-explanatory to anyone
# watching a demo/Loom without you narrating every chart.
# ---------------------------------------------------------------
with st.sidebar:
    st.header("📡 About PulseLite")
    st.markdown("""
    A real-time social listening pipeline:

    1. **Producer** generates simulated posts
    2. **Kafka** streams them as events
    3. **Consumer** scores sentiment (VADER),
       extracts hashtags, and detects anomalies
    4. **DuckDB** stores the results
    5. **This dashboard** reads DuckDB live

    Everything below updates automatically
    every 3 seconds - no manual refresh needed.
    """)
    st.divider()
    st.caption("Built as part of the PulseLite streaming project")


def fetch_dashboard_data(db_path, max_attempts=5, base_delay=0.1):
    """Fetches all dashboard data from DuckDB within a short-lived connection
    and closes the connection immediately to prevent locking conflicts with the consumer.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            with duckdb.connect(db_path, read_only=True) as con:
                total_posts = con.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
                if total_posts == 0:
                    return {"total_posts": 0}

                posts_last_min = con.execute("""
                    SELECT COUNT(*) FROM posts
                    WHERE minute_bucket = (SELECT MAX(minute_bucket) FROM posts)
                """).fetchone()[0]

                avg_sentiment = con.execute("SELECT AVG(sentiment_score) FROM posts").fetchone()[0] or 0.0

                anomaly_count = con.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]

                volume_df = con.execute("""
                    SELECT minute_bucket, COUNT(*) AS post_count
                    FROM posts GROUP BY minute_bucket ORDER BY minute_bucket
                """).fetchdf()

                sentiment_df = con.execute("""
                    SELECT minute_bucket, AVG(sentiment_score) AS avg_sentiment
                    FROM posts GROUP BY minute_bucket ORDER BY minute_bucket
                """).fetchdf()

                entities_df = con.execute("""
                    SELECT topic, COUNT(*) AS mentions
                    FROM posts, UNNEST(entities) AS t(topic)
                    GROUP BY topic ORDER BY mentions DESC LIMIT 10
                """).fetchdf()

                drift_snapshots = con.execute("""
                    SELECT DISTINCT computed_at FROM topic_drift ORDER BY computed_at DESC LIMIT 2
                """).fetchdf()

                drift_compare_df = pd.DataFrame()
                if len(drift_snapshots) >= 2:
                    latest_time = drift_snapshots.iloc[0]["computed_at"]
                    previous_time = drift_snapshots.iloc[1]["computed_at"]

                    latest = con.execute(
                        "SELECT topic, mentions FROM topic_drift WHERE computed_at = ?", [latest_time]
                    ).fetchdf().set_index("topic")["mentions"].to_dict()
                    previous = con.execute(
                        "SELECT topic, mentions FROM topic_drift WHERE computed_at = ?", [previous_time]
                    ).fetchdf().set_index("topic")["mentions"].to_dict()

                    all_topics = set(latest) | set(previous)
                    rows = []
                    for topic in all_topics:
                        now_count = latest.get(topic, 0)
                        prev_count = previous.get(topic, 0)
                        trend = "🔺" if now_count > prev_count else ("🔻" if now_count < prev_count else "➖")
                        rows.append({"Topic": f"#{topic}", "Now": now_count, "Before": prev_count, "Trend": trend})

                    drift_compare_df = pd.DataFrame(rows).sort_values("Now", ascending=False)

                anomalies_df = con.execute("""
                    SELECT detected_at AS "Detected At", topic AS "Topic",
                           current_volume AS "Volume", ROUND(rolling_avg, 1) AS "Usual Avg",
                           ROUND(multiplier, 1) AS "x Normal"
                    FROM anomalies ORDER BY detected_at DESC LIMIT 10
                """).fetchdf()

                return {
                    "total_posts": total_posts,
                    "posts_last_min": posts_last_min,
                    "avg_sentiment": avg_sentiment,
                    "anomaly_count": anomaly_count,
                    "volume_df": volume_df,
                    "sentiment_df": sentiment_df,
                    "entities_df": entities_df,
                    "drift_compare_df": drift_compare_df,
                    "anomalies_df": anomalies_df,
                }
        except Exception as e:
            if attempt == max_attempts:
                raise e
            time.sleep(base_delay * attempt)


try:
    data = fetch_dashboard_data(DB_PATH)
    total_posts = data.get("total_posts", 0)
except Exception:
    st.warning("⏳ No data yet or database initializing. Make sure `producer_fetch.py` and `consumer.py` are both running.")
    st.stop()

if total_posts == 0:
    st.info("⏳ Waiting for the first posts to arrive...")
    st.stop()

# ============================================================
# HEADLINE METRICS
# ============================================================
posts_last_min = data["posts_last_min"]
avg_sentiment = data["avg_sentiment"]
anomaly_count = data["anomaly_count"]

mood = "🙂 Good" if avg_sentiment >= 0.05 else ("🙁 Bad" if avg_sentiment <= -0.05 else "😐 Neutral")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Posts Processed", f"{total_posts:,}")
col2.metric("Posts This Minute", posts_last_min)
col3.metric("Overall Mood", mood, f"{avg_sentiment:+.2f} score")
col4.metric("Anomalies Flagged", anomaly_count, delta_color="inverse")

st.divider()

# ============================================================
# VOLUME + SENTIMENT OVER TIME
# ============================================================
left, right = st.columns(2)

with left:
    st.subheader("📈 Volume per Minute")
    st.caption("How many posts arrived each minute — spikes here are what the anomaly detector watches for.")

    volume_df = data["volume_df"]
    if not volume_df.empty:
        fig = px.bar(volume_df, x="minute_bucket", y="post_count",
                      labels={"minute_bucket": "", "post_count": "Posts"},
                      color_discrete_sequence=["#FF4B4B"])
        fig.update_layout(margin=dict(t=10, b=10), height=300)
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("💬 Sentiment Over Time")
    st.caption("Average VADER score per minute. Above the line = positive mood, below = negative.")

    sentiment_df = data["sentiment_df"]
    if not sentiment_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=sentiment_df["minute_bucket"], y=sentiment_df["avg_sentiment"],
            mode="lines+markers", line=dict(color="#4BA3FF", width=3),
            marker=dict(size=6),
        ))
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        fig.update_layout(margin=dict(t=10, b=10), height=300,
                           yaxis_title="Sentiment (-1 to +1)", xaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ============================================================
# TOP ENTITIES + TOPIC DRIFT
# ============================================================
left2, right2 = st.columns(2)

with left2:
    st.subheader("🏷️ Top Topics (All Time)")
    st.caption("Most-mentioned hashtags across every post seen so far.")

    entities_df = data["entities_df"]
    if not entities_df.empty:
        fig = px.bar(entities_df, x="mentions", y="topic", orientation="h",
                      labels={"topic": "", "mentions": "Mentions"},
                      color_discrete_sequence=["#4BFF9E"])
        fig.update_layout(yaxis={"categoryorder": "total ascending"},
                           margin=dict(t=10, b=10), height=320)
        st.plotly_chart(fig, use_container_width=True)

with right2:
    st.subheader("🔄 Trending Now vs a Few Minutes Ago")
    st.caption("Compares the current top topics against the previous snapshot — this is 'topic drift'.")

    drift_compare_df = data["drift_compare_df"]
    if not drift_compare_df.empty:
        st.dataframe(drift_compare_df, use_container_width=True, hide_index=True)
    else:
        st.info("Building history — comparison appears automatically after two snapshots.")

st.divider()

# ============================================================
# ANOMALIES
# ============================================================
st.subheader("🚨 Anomaly Alerts")
st.caption("Topics whose volume this minute was more than 3x their recent rolling average.")

anomalies_df = data["anomalies_df"]
if anomalies_df.empty:
    st.success("✅ No anomalies right now — everything's within normal range.")
else:
    st.dataframe(anomalies_df, use_container_width=True, hide_index=True)

