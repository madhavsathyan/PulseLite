import os
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import psycopg2

# Page Config
st.set_page_config(
    page_title="PulseLite | Real-time Social Media Stream Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS for custom styling (glassmorphism KPI cards, dark background enhancements)
st.markdown("""
<style>
    /* Main body background styling */
    .stApp {
        background-color: #0F172A;
        color: #F8FAFC;
    }
    
    /* Header style */
    .header-container {
        background: linear-gradient(135deg, #1E1B4B 0%, #0F172A 100%);
        padding: 2rem;
        border-radius: 12px;
        border-bottom: 2px solid #3B82F6;
        margin-bottom: 2rem;
        box-shadow: 0 4px 20px -2px rgba(0, 0, 0, 0.5);
    }
    .header-title {
        font-family: 'Inter', sans-serif;
        font-size: 2.5rem;
        font-weight: 800;
        background: linear-gradient(90deg, #60A5FA 0%, #A78BFA 50%, #EC4899 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }
    .header-subtitle {
        font-size: 1.1rem;
        color: #94A3B8;
        margin-top: 0.5rem;
    }

    /* KPI Cards styling */
    .kpi-container {
        display: flex;
        justify-content: space-between;
        gap: 1.5rem;
        margin-bottom: 2rem;
    }
    .kpi-card {
        background: rgba(30, 41, 59, 0.7);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-top: 3px solid #6366F1;
        padding: 1.5rem;
        border-radius: 10px;
        flex: 1;
        text-align: center;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #F1F5F9;
        margin: 0.5rem 0;
    }
    .kpi-label {
        font-size: 0.9rem;
        text-transform: uppercase;
        color: #94A3B8;
        letter-spacing: 0.05em;
    }

    /* badges */
    .badge {
        padding: 0.25rem 0.6rem;
        border-radius: 9999px;
        font-size: 0.8rem;
        font-weight: 600;
        display: inline-block;
        margin-right: 0.5rem;
    }
    .badge-positive {
        background-color: rgba(16, 185, 129, 0.2);
        color: #34D399;
        border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .badge-neutral {
        background-color: rgba(100, 116, 139, 0.2);
        color: #94A3B8;
        border: 1px solid rgba(100, 116, 139, 0.3);
    }
    .badge-negative {
        background-color: rgba(239, 68, 68, 0.2);
        color: #F87171;
        border: 1px solid rgba(239, 68, 68, 0.3);
    }
    .badge-topic {
        background-color: rgba(99, 102, 241, 0.2);
        color: #A5B4FC;
        border: 1px solid rgba(99, 102, 241, 0.3);
    }
</style>
""", unsafe_allow_html=True)

# Postgres Connection parameters
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_NAME = os.getenv("POSTGRES_DB", "pulselite")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )

# Sidebar Configuration
st.sidebar.title("⚡ PulseLite Controls")
st.sidebar.markdown("Configure filters and dashboard behaviors.")

# Auto-Refresh Control
enable_refresh = st.sidebar.checkbox("Enable Auto-Refresh", value=True)
refresh_interval = st.sidebar.slider("Refresh Interval (s)", min_value=2, max_value=60, value=3, step=1)

if enable_refresh:
    st_autorefresh(interval=refresh_interval * 1000, key="datarefresh")

# Fetch unique subreddits/topics for the filter
topics_list = []
try:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT topic FROM processed_posts ORDER BY topic;")
    topics_list = [row[0] for row in cur.fetchall() if row[0]]
    cur.close()
    conn.close()
except Exception as e:
    # Default list if database isn't initialized yet
    topics_list = ["r/technology", "r/cricket", "r/movies", "r/gaming", "r/news"]

selected_topic = st.sidebar.selectbox("Filter by Subreddit/Topic", ["All Topics"] + topics_list)

# Header Section
st.markdown("""
<div class="header-container">
    <h1 class="header-title">⚡ PulseLite Stream Dashboard</h1>
    <p class="header-subtitle">Real-time sentiment and topic insights processing via Kafka and Spark Structured Streaming</p>
</div>
""", unsafe_allow_html=True)

# Fetch Data
try:
    conn = get_db_connection()
    
    # 1. Total processed posts & average sentiment
    if selected_topic == "All Topics":
        query_kpis = """
        SELECT 
            COUNT(*) as total_posts,
            SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) as pos_posts,
            SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) as neg_posts,
            SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neu_posts,
            AVG(sentiment_score) as avg_score
        FROM processed_posts;
        """
        df_kpi = pd.read_sql(query_kpis, conn)
    else:
        query_kpis = """
        SELECT 
            COUNT(*) as total_posts,
            SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) as pos_posts,
            SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) as neg_posts,
            SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neu_posts,
            AVG(sentiment_score) as avg_score
        FROM processed_posts
        WHERE topic = %s;
        """
        df_kpi = pd.read_sql(query_kpis, conn, params=(selected_topic,))
    
    # 2. Sentiment distribution
    if selected_topic == "All Topics":
        query_sentiment = """
        SELECT sentiment_label, COUNT(*) as count 
        FROM processed_posts 
        GROUP BY sentiment_label;
        """
        df_sentiment = pd.read_sql(query_sentiment, conn)
    else:
        query_sentiment = """
        SELECT sentiment_label, COUNT(*) as count 
        FROM processed_posts 
        WHERE topic = %s
        GROUP BY sentiment_label;
        """
        df_sentiment = pd.read_sql(query_sentiment, conn, params=(selected_topic,))
    
    # 3. Volume over time
    if selected_topic == "All Topics":
        query_volume = """
        SELECT window_start, SUM(post_count) as post_count 
        FROM volume_metrics 
        GROUP BY window_start
        ORDER BY window_start DESC 
        LIMIT 20;
        """
        df_volume = pd.read_sql(query_volume, conn)
    else:
        query_volume = """
        SELECT window_start, post_count 
        FROM volume_metrics 
        WHERE topic = %s
        ORDER BY window_start DESC 
        LIMIT 20;
        """
        df_volume = pd.read_sql(query_volume, conn, params=(selected_topic,))
        
    if not df_volume.empty:
        df_volume = df_volume.sort_values(by="window_start")
        
    # 4. Top Entities (last 5 minutes of data)
    if selected_topic == "All Topics":
        query_entities = """
        SELECT entity, SUM(post_count) as total_count 
        FROM entity_metrics 
        WHERE window_start >= (SELECT COALESCE(MAX(window_start), NOW()) - INTERVAL '5 minutes' FROM entity_metrics)
        GROUP BY entity 
        ORDER BY total_count DESC 
        LIMIT 10;
        """
        df_entities = pd.read_sql(query_entities, conn)
    else:
        query_entities = """
        SELECT entity, SUM(post_count) as total_count 
        FROM entity_metrics 
        WHERE topic = %s AND window_start >= (SELECT COALESCE(MAX(window_start), NOW()) - INTERVAL '5 minutes' FROM entity_metrics WHERE topic = %s)
        GROUP BY entity 
        ORDER BY total_count DESC 
        LIMIT 10;
        """
        df_entities = pd.read_sql(query_entities, conn, params=(selected_topic, selected_topic))
    
    # 5. Topic Drift (last 5 minutes of data)
    if selected_topic == "All Topics":
        query_topics = """
        SELECT word, SUM(post_count) as total_count 
        FROM topic_metrics 
        WHERE window_start >= (SELECT COALESCE(MAX(window_start), NOW()) - INTERVAL '5 minutes' FROM topic_metrics)
        GROUP BY word 
        ORDER BY total_count DESC 
        LIMIT 10;
        """
        df_topics = pd.read_sql(query_topics, conn)
    else:
        query_topics = """
        SELECT word, SUM(post_count) as total_count 
        FROM topic_metrics 
        WHERE topic = %s AND window_start >= (SELECT COALESCE(MAX(window_start), NOW()) - INTERVAL '5 minutes' FROM topic_metrics WHERE topic = %s)
        GROUP BY word 
        ORDER BY total_count DESC 
        LIMIT 10;
        """
        df_topics = pd.read_sql(query_topics, conn, params=(selected_topic, selected_topic))
    
    # 6. Recent posts
    if selected_topic == "All Topics":
        query_recent = """
        SELECT text, sentiment_label, sentiment_score, created_at, topic 
        FROM processed_posts 
        ORDER BY created_at DESC 
        LIMIT 6;
        """
        df_recent = pd.read_sql(query_recent, conn)
    else:
        query_recent = """
        SELECT text, sentiment_label, sentiment_score, created_at, topic 
        FROM processed_posts 
        WHERE topic = %s
        ORDER BY created_at DESC 
        LIMIT 6;
        """
        df_recent = pd.read_sql(query_recent, conn, params=(selected_topic,))
    
    conn.close()
    
    # Render KPIs
    total_posts = df_kpi["total_posts"].iloc[0] or 0
    avg_score = df_kpi["avg_score"].iloc[0] or 0.0
    pos_pct = (df_kpi["pos_posts"].iloc[0] or 0) / (total_posts or 1) * 100
    
    # Setup KPI layouts
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
        <div class="kpi-card" style="border-top-color: #3B82F6;">
            <div class="kpi-label">Ingested ({selected_topic})</div>
            <div class="kpi-value">{total_posts:,}</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="kpi-card" style="border-top-color: #10B981;">
            <div class="kpi-label">Positive Posts</div>
            <div class="kpi-value">{pos_pct:.1f}%</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="kpi-card" style="border-top-color: #EC4899;">
            <div class="kpi-label">Avg Sentiment Score</div>
            <div class="kpi-value">{avg_score:+.2f}</div>
        </div>
        """, unsafe_allow_html=True)
    with col4:
        # Calculate active stream speed (posts in latest minute)
        latest_speed = df_volume["post_count"].iloc[-1] if not df_volume.empty else 0
        st.markdown(f"""
        <div class="kpi-card" style="border-top-color: #A78BFA;">
            <div class="kpi-label">Ingestion Rate (Min)</div>
            <div class="kpi-value">{latest_speed} /m</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Main Visualizations Row
    v_col1, v_col2 = st.columns(2)
    
    # 1. Sentiment Pie Chart
    with v_col1:
        st.subheader("Sentiment Distribution")
        if not df_sentiment.empty:
            # Theme colors matching our specs
            colors = {'positive': '#10B981', 'neutral': '#64748B', 'negative': '#EF4444'}
            fig_pie = px.pie(
                df_sentiment, 
                values='count', 
                names='sentiment_label',
                color='sentiment_label',
                color_discrete_map=colors,
                hole=0.4
            )
            fig_pie.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(t=10, b=10, l=10, r=10),
                legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("No sentiment data available yet.")

    # 2. Volume Timeline
    with v_col2:
        st.subheader("Post Ingestion Volume (per minute)")
        if not df_volume.empty:
            fig_line = px.area(
                df_volume, 
                x="window_start", 
                y="post_count",
                labels={"window_start": "Time", "post_count": "Posts"}
            )
            # Styling area chart with violet-blue accents
            fig_line.update_traces(
                line_color='#A78BFA',
                fillcolor='rgba(167, 139, 250, 0.2)',
                mode='lines+markers',
                marker=dict(size=6, color='#60A5FA')
            )
            fig_line.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(t=10, b=10, l=10, r=10),
                xaxis=dict(showgrid=False),
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)")
            )
            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.info("No volume data available yet.")

    # Entities and Topics Row
    e_col1, e_col2 = st.columns(2)
    
    # 3. Top Entities
    with e_col1:
        st.subheader("Top Trending Entities (Last 5 mins)")
        if not df_entities.empty:
            fig_ent = px.bar(
                df_entities.sort_values(by="total_count"), 
                x="total_count", 
                y="entity",
                orientation='h',
                color="total_count",
                color_continuous_scale="Purples",
                labels={"total_count": "Mentions", "entity": "Entity"}
            )
            fig_ent.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(t=10, b=10, l=10, r=10),
                coloraxis_showscale=False,
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                yaxis=dict(showgrid=False)
            )
            st.plotly_chart(fig_ent, use_container_width=True)
        else:
            st.info("No entity data available. Generating stream data...")

    # 4. Topic Drift
    with e_col2:
        st.subheader("Topic Word Drift (Last 5 mins)")
        if not df_topics.empty:
            fig_top = px.bar(
                df_topics, 
                x="word", 
                y="total_count",
                color="total_count",
                color_continuous_scale="Viridis",
                labels={"total_count": "Frequency", "word": "Keyword"}
            )
            fig_top.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(t=10, b=10, l=10, r=10),
                coloraxis_showscale=False,
                xaxis=dict(showgrid=False),
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)")
            )
            st.plotly_chart(fig_top, use_container_width=True)
        else:
            st.info("No topic drift data available yet.")

    # Recent Posts Section
    st.markdown("<hr style='border-color: rgba(255,255,255,0.1)'>", unsafe_allow_html=True)
    st.subheader("Live Feed Sample")
    if not df_recent.empty:
        # Create custom styled cards or display table
        for idx, row in df_recent.iterrows():
            lbl = row['sentiment_label']
            topic_lbl = row['topic']
            badge_class = f"badge badge-{lbl}"
            score_color = "#34D399" if lbl == "positive" else ("#F87171" if lbl == "negative" else "#94A3B8")
            
            st.markdown(f"""
            <div style="background: rgba(30, 41, 59, 0.4); padding: 1rem; border-radius: 8px; border-left: 4px solid {score_color}; margin-bottom: 0.8rem;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-weight: 500; color: #F1F5F9;">{row['text']}</span>
                    <div>
                        <span class="badge badge-topic">{topic_lbl}</span>
                        <span class="{badge_class}">{lbl.upper()}</span>
                        <span style="color: #64748B; font-size: 0.8rem; margin-left: 1rem;">{row['created_at'].strftime('%Y-%m-%d %H:%M:%S')}</span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Waiting for live feed stream...")

except Exception as e:
    st.warning("Connecting to database and awaiting incoming streaming tables...")
    st.error(f"Details: {e}")
    time.sleep(2)

