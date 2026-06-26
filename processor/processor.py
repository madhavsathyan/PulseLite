import os
import re
import sys
import time
from datetime import datetime, timezone
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import psycopg2
from psycopg2.extras import execute_values

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Environment Variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "social_media_posts")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "pulselite")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# Initialize VADER Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

# Stopwords for Topic Drift analysis
STOPWORDS = {
    'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', 'as', 'at', 
    'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'can', 'could', 
    'did', 'do', 'does', 'doing', 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'has', 
    'have', 'having', 'he', 'her', 'here', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 
    'in', 'into', 'is', 'it', 'its', 'itself', 'just', 'me', 'more', 'most', 'my', 'myself', 'no', 'nor', 
    'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'our', 'ours', 'ourselves', 'out', 'over', 
    'own', 'same', 'she', 'should', 'so', 'some', 'such', 'than', 'that', 'the', 'their', 'theirs', 'them', 
    'themselves', 'then', 'there', 'these', 'they', 'this', 'those', 'through', 'to', 'too', 'under', 
    'until', 'up', 'very', 'was', 'we', 'were', 'what', 'when', 'where', 'which', 'while', 'who', 'whom', 
    'why', 'with', 'you', 'your', 'yours', 'yourself', 'yourselves', 'is', 'has', 'will', 'new', 'released',
    'announced', 'latest', 'absolutely', 'highly', 'works', 'game', 'change', 'wins', 'happened', 'year',
    'shows', 'super', 'excited', 'yesterday', 'tomorrow', 'evening', 'causing', 'bugs', 'crashes', 
    'disappointed', 'lack', 'quality', 'lost', 'horrible', 'poor', 'overrated', 'overpriced', 'facing',
    'outage', 'complete', 'disaster', 'avoid', 'costs', 'flawlessly'
}

def clean_text_and_get_words(text):
    """Tokenize and return words that are not stopwords, numbers, or short symbols."""
    words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
    return [w for w in words if w not in STOPWORDS]

def extract_entities(text):
    """Extract hashtags, mentions and uppercase terms as entities."""
    entities = []
    
    # 1. Extract hashtags
    hashtags = re.findall(r'#\w+', text)
    entities.extend([h.lower() for h in hashtags])
    
    # 2. Extract mentions
    mentions = re.findall(r'@\w+', text)
    entities.extend([m.lower() for m in mentions])
    
    # 3. Extract Capitalized words (e.g. Google, Virat) that are not at start of sentence,
    # but since sentences here are short, let's extract words starting with Capital letter 
    # that are not in stopwords
    cap_words = re.findall(r'\b[A-Z][a-zA-Z]+\b', text)
    for word in cap_words:
        if word.lower() not in STOPWORDS and word.lower() not in ["the", "this", "that", "they", "we", "he", "she"]:
            entities.append(word) # Keep case for entity display (e.g. Virat Kohli)
            
    return list(set(entities))

def parse_iso_datetime(dt_str):
    """Parse ISO datetime string to python datetime object."""
    try:
        # Handles '2026-06-26T13:00:00.123456Z' or '2026-06-26T13:00:00Z'
        if dt_str.endswith('Z'):
            dt_str = dt_str[:-1] + '+00:00'
        return datetime.fromisoformat(dt_str)
    except Exception as e:
        print(f"Error parsing date {dt_str}: {e}")
        return datetime.now(timezone.utc)

def write_to_postgres(spark_df, batch_id):
    """Write processed Spark DataFrame to PostgreSQL database by converting it to Pandas."""
    pandas_df = spark_df.toPandas()
    print(f"Processing batch {batch_id} with {len(pandas_df)} records.")
    if pandas_df.empty:
        return
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT
        )
        cur = conn.cursor()
    except Exception as e:
        print(f"Database connection failed: {e}")
        return

    try:
        posts_data = []
        volume_counts = {}
        entity_counts = {}
        topic_counts = {}

        for _, row in pandas_df.iterrows():
            post_id = row.get("id")
            text = row.get("text", "")
            created_at_str = row.get("created_at")
            topic = row.get("topic", "unknown")
            
            if not post_id or not text or not created_at_str:
                continue

            created_at = parse_iso_datetime(created_at_str)
            # Truncate to the minute for window aggregations
            minute_window = created_at.replace(second=0, microsecond=0)

            # 1. Sentiment calculation
            sentiment_scores = analyzer.polarity_scores(text)
            compound = sentiment_scores["compound"]
            if compound >= 0.05:
                sentiment_label = "positive"
            elif compound <= -0.05:
                sentiment_label = "negative"
            else:
                sentiment_label = "neutral"

            posts_data.append((post_id, text, sentiment_label, compound, created_at, topic))

            # 2. Volume tracking
            volume_key = (topic, minute_window)
            volume_counts[volume_key] = volume_counts.get(volume_key, 0) + 1

            # 3. Entity tracking
            entities = extract_entities(text)
            for entity in entities:
                key = (entity, topic, minute_window)
                entity_counts[key] = entity_counts.get(key, 0) + 1

            # 4. Topic drift tracking
            words = clean_text_and_get_words(text)
            for word in words:
                # Skip words that match extracted entities (to avoid duplicates in charts)
                if word in entities or f"#{word}" in entities:
                    continue
                key = (word, topic, minute_window)
                topic_counts[key] = topic_counts.get(key, 0) + 1

        # Perform database updates inside a single transaction
        
        # 1. Insert processed posts
        if posts_data:
            insert_posts_query = """
            INSERT INTO processed_posts (id, text, sentiment_label, sentiment_score, created_at, topic)
            VALUES %s
            ON CONFLICT (id) DO NOTHING;
            """
            execute_values(cur, insert_posts_query, posts_data)

        # 2. Upsert volume metrics
        if volume_counts:
            volume_data = [(k[0], k[1], v) for k, v in volume_counts.items()]
            insert_volume_query = """
            INSERT INTO volume_metrics (topic, window_start, post_count)
            VALUES %s
            ON CONFLICT (topic, window_start) 
            DO UPDATE SET post_count = volume_metrics.post_count + excluded.post_count;
            """
            execute_values(cur, insert_volume_query, volume_data)

        # 3. Upsert entity metrics
        if entity_counts:
            entity_data = [(k[0], k[1], k[2], v) for k, v in entity_counts.items()]
            insert_entity_query = """
            INSERT INTO entity_metrics (entity, topic, window_start, post_count)
            VALUES %s
            ON CONFLICT (entity, topic, window_start) 
            DO UPDATE SET post_count = entity_metrics.post_count + excluded.post_count;
            """
            execute_values(cur, insert_entity_query, entity_data)

        # 4. Upsert topic metrics
        if topic_counts:
            topic_data = [(k[0], k[1], k[2], v) for k, v in topic_counts.items()]
            insert_topic_query = """
            INSERT INTO topic_metrics (word, topic, window_start, post_count)
            VALUES %s
            ON CONFLICT (word, topic, window_start) 
            DO UPDATE SET post_count = topic_metrics.post_count + excluded.post_count;
            """
            execute_values(cur, insert_topic_query, topic_data)

        conn.commit()
        print(f"Batch {batch_id} written to PostgreSQL successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error executing batch {batch_id}: {e}")
    finally:
        cur.close()
        conn.close()

def main():
    print("Starting Spark Session...")
    # Initialize Spark Session
    # Set appName and configure Kafka jar location if run via spark-submit,
    # or let Spark auto-download packages if run via python (using configuration options)
    spark = SparkSession.builder \
        .appName("PulseLiteProcessor") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Subscribing to Kafka topic '{KAFKA_TOPIC}' on broker '{KAFKA_BROKER}'...")
    
    # 1. Read Stream from Kafka
    # Include retry loop in case Kafka is still starting
    kafka_df = None
    for i in range(10):
        try:
            kafka_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BROKER) \
                .option("subscribe", KAFKA_TOPIC) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            break
        except Exception as e:
            print(f"Attempt {i+1}/10 to connect to Kafka stream failed: {e}")
            time.sleep(5)
            
    if kafka_df is None:
        print("Failed to initialize Kafka stream reader. Exiting.")
        sys.exit(1)

    # Define schema for the JSON payload
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("topic", StringType(), True)
    ])

    # 2. Extract and parse fields
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_val") \
        .select(from_json(col("json_val"), schema).alias("data")) \
        .select("data.*")

    # 3. Write stream using foreachBatch
    query = parsed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .trigger(processingTime="5 seconds") \
        .start()

    print("PySpark Structured Streaming Job Started. Waiting for termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()

