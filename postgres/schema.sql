-- Schema initialization for PulseLite

-- 1. Table for individual processed posts and sentiment
CREATE TABLE IF NOT EXISTS processed_posts (
    id VARCHAR(50) PRIMARY KEY,
    text TEXT NOT NULL,
    sentiment_label VARCHAR(20) NOT NULL,
    sentiment_score REAL NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    topic VARCHAR(50) NOT NULL DEFAULT 'unknown'
);

-- 2. Table for windowed entity metrics (hashtags/mentions)
CREATE TABLE IF NOT EXISTS entity_metrics (
    entity VARCHAR(100) NOT NULL,
    topic VARCHAR(50) NOT NULL DEFAULT 'unknown',
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    post_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (entity, topic, window_start)
);

-- 3. Table for post volume tracking (aggregated by minute)
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

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_processed_posts_topic ON processed_posts(topic);
CREATE INDEX IF NOT EXISTS idx_processed_posts_created_at ON processed_posts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_entity_metrics_lookup ON entity_metrics(topic, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_volume_metrics_lookup ON volume_metrics(topic, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_topic_metrics_lookup ON topic_metrics(topic, window_start DESC);

