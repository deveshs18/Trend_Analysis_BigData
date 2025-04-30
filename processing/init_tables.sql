-- Table for Bloom Filter results
CREATE TABLE IF NOT EXISTS bloom_filter (
    text TEXT,
    created_at TIMESTAMP,
    sentiment TEXT,
    is_sports INTEGER,
    batch_id INTEGER,
    processing_time TIMESTAMP
);

-- Table for CMS (Count-Min Sketch) estimates
CREATE TABLE IF NOT EXISTS cms_stream (
    batch_id INTEGER,
    timestamp TIMESTAMP,
    keyword TEXT,
    estimated_count INTEGER
);

-- Table for LSH results
CREATE TABLE IF NOT EXISTS lsh_stream (
    batch_id INTEGER,
    tweet_id TEXT,
    similar_to TEXT,
    found_at TIMESTAMP
);



-- Table for EDW (Exponential Decay Window) results
CREATE TABLE IF NOT EXISTS edw_stream (
    text TEXT,
    created_at TIMESTAMP,
    sentiment TEXT,
    entities JSONB,
    weight DOUBLE PRECISION,
    weighted_sentiment DOUBLE PRECISION,
    processing_time TIMESTAMP,
    batch_id BIGINT,
    hour_of_day INTEGER,
    day_of_week INTEGER,
    is_positive INTEGER,
    is_negative INTEGER,
    is_neutral INTEGER
);

-- Table for Flajolet-Martin results
CREATE TABLE IF NOT EXISTS flajolent_stream (
    batch_id INTEGER,
    timestamp TIMESTAMP,
    set_name TEXT,
    estimated_cardinality BIGINT
);

-- Table for processing errors
CREATE TABLE IF NOT EXISTS processing_errors (
    batch_id INTEGER,
    error TEXT,
    error_time TIMESTAMP
);

-- Table for raw Twitter sentiment data
CREATE TABLE IF NOT EXISTS twitter_sentiment (
    text TEXT,
    created_at TIMESTAMP,
    sentiment TEXT,
    entities JSONB
); 