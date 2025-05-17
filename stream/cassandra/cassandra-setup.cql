CREATE KEYSPACE gold_news WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
-- Table pour les news individuelles
CREATE TABLE gold_news.raw_news (
    id text PRIMARY KEY,
    title text,
    source text,
    published_at text,
    description text,
    url text,
    ingestion_time text
);

CREATE TABLE gold_news.impact_analysis (
    window_timestamp text PRIMARY KEY,
    impact int,
    confidence double,
    explanation text
);