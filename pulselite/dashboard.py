"""
PulseLite - Day 9
A live-updating Streamlit dashboard that reads directly from pulselite.db
and shows sentiment, top entities, volume per minute, topic drift, and
anomaly markers - refreshing automatically every few seconds.

Run with: streamlit run dashboard.py
"""

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

DB_PATH = "pulselite.db"
REFRESH_INTERVAL_MS = 5000  # auto-refresh every 5 seconds

st.set_page_config(page_title="PulseLite", layout="wide")

# This one line makes the whole page automatically rerun every N milliseconds,
# which is what makes the dashboard feel "live" instead of static.
st_autorefresh(interval=REFRESH_INTERVAL_MS, key="refresh")

st.title("📡 PulseLite — Real-Time Hashtag Pulse")
st.caption(f"Auto-refreshing every {REFRESH_INTERVAL_MS // 1000} seconds — reading from {DB_PATH}")


def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)


con = get_connection()


# --- Guard: if the DB has no data yet, tell the user instead of crashing ---
try:
    total_posts = con.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
except Exception:
    st.warning("No data yet. Make sure producer_fetch.py and consumer.py are both running.")
    st.stop()

if total_posts == 0:
    st.info("Waiting for the first posts to arrive...")
    st.stop()

# ============================================================
# TOP ROW: headline numbers
# ============================================================
col1, col2, col3, col4 = st.columns(4)

posts_last_min = con.execute("""
    SELECT COUNT(*) FROM posts
    WHERE minute_bucket = (SELECT MAX(minute_bucket) FROM posts)
""").fetchone()[0]

avg_sentiment = con.execute("SELECT AVG(sentiment_score) FROM posts").fetchone()[0]

anomaly_count = con.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]

positive_pct = con.execute("""
    SELECT ROUND(100.0 * SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) / COUNT(*), 1)
    FROM posts
""").fetchone()[0]

col1.metric("Total Posts", total_posts)
col2.metric("Posts (Latest Minute)", posts_last_min)
col3.metric("Avg Sentiment", f"{avg_sentiment:+.2f}")
col4.metric("Anomalies Flagged", anomaly_count)

st.divider()

# ============================================================
# ROW: Volume per minute + Sentiment over time
# ============================================================
left, right = st.columns(2)

with left:
    st.subheader("📈 Volume per Minute")
    volume_df = con.execute("""
        SELECT minute_bucket, COUNT(*) AS post_count
        FROM posts
        GROUP BY minute_bucket
        ORDER BY minute_bucket
    """).fetchdf()

    if not volume_df.empty:
        fig = px.bar(volume_df, x="minute_bucket", y="post_count",
                      labels={"minute_bucket": "Minute", "post_count": "Posts"})
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("💬 Sentiment Over Time")
    sentiment_df = con.execute("""
        SELECT minute_bucket, AVG(sentiment_score) AS avg_sentiment
        FROM posts
        GROUP BY minute_bucket
        ORDER BY minute_bucket
    """).fetchdf()

    if not sentiment_df.empty:
        fig = px.line(sentiment_df, x="minute_bucket", y="avg_sentiment",
                       labels={"minute_bucket": "Minute", "avg_sentiment": "Avg Sentiment"},
                       markers=True)
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ============================================================
# ROW: Top entities + Topic drift comparison
# ============================================================
left2, right2 = st.columns(2)

with left2:
    st.subheader("🏷️ Top Entities (All Time)")
    entities_df = con.execute("""
        SELECT topic, COUNT(*) AS mentions
        FROM posts, UNNEST(entities) AS t(topic)
        GROUP BY topic
        ORDER BY mentions DESC
        LIMIT 10
    """).fetchdf()

    if not entities_df.empty:
        fig = px.bar(entities_df, x="mentions", y="topic", orientation="h",
                      labels={"topic": "Hashtag", "mentions": "Mentions"})
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

with right2:
    st.subheader("🔄 Topic Drift — Latest Snapshot vs Previous")
    drift_snapshots = con.execute("""
        SELECT DISTINCT computed_at FROM topic_drift ORDER BY computed_at DESC LIMIT 2
    """).fetchdf()

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
            rows.append({"topic": topic, "now": now_count, "previous": prev_count,
                         "change": now_count - prev_count})
        drift_compare_df = pd.DataFrame(rows).sort_values("now", ascending=False)

        st.dataframe(
            drift_compare_df.rename(columns={
                "topic": "Topic", "now": "Now", "previous": "Previous", "change": "Change"
            }),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("Need at least two topic-drift snapshots to compare (happens automatically over time).")

st.divider()

# ============================================================
# ROW: Anomalies
# ============================================================
st.subheader("🚨 Recent Anomalies")
anomalies_df = con.execute("""
    SELECT detected_at, topic, minute_bucket, current_volume, rolling_avg, multiplier
    FROM anomalies
    ORDER BY detected_at DESC
    LIMIT 10
""").fetchdf()

if anomalies_df.empty:
    st.caption("No anomalies detected yet — this is normal early on, it needs a few minutes of history first.")
else:
    st.dataframe(anomalies_df, use_container_width=True, hide_index=True)