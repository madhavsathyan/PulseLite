FROM bitnami/spark:3.5.0

USER root

# Install build dependencies for psycopg2 and database connections
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processor.py .
COPY log4j.properties .

# Execute via spark-submit, fetching the spark-sql-kafka package at runtime
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "processor.py"]

