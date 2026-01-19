-- Migration: Create OHLCV hypertable for 1-minute bars
-- TimescaleDB extension must be enabled first

-- Enable TimescaleDB extension (run as superuser if needed)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create the OHLCV table for 1-minute bars
CREATE TABLE IF NOT EXISTS ohlcv_1min (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    open        NUMERIC(12, 4) NOT NULL,
    high        NUMERIC(12, 4) NOT NULL,
    low         NUMERIC(12, 4) NOT NULL,
    close       NUMERIC(12, 4) NOT NULL,
    volume      BIGINT NOT NULL,
    vwap        NUMERIC(12, 4),  -- Volume-weighted average price (optional)
    trade_count INTEGER,          -- Number of trades in the bar (optional)

    -- Composite primary key
    PRIMARY KEY (symbol, time)
);

-- Convert to hypertable partitioned by time
-- chunk_time_interval of 1 day is good for 1-minute data
SELECT create_hypertable(
    'ohlcv_1min',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Create index for efficient symbol + time range queries
CREATE INDEX IF NOT EXISTS idx_ohlcv_1min_symbol_time
    ON ohlcv_1min (symbol, time DESC);

-- Enable compression for older chunks (after 7 days)
ALTER TABLE ohlcv_1min SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'time DESC'
);

-- Add compression policy: compress chunks older than 7 days
SELECT add_compression_policy('ohlcv_1min', INTERVAL '7 days', if_not_exists => TRUE);

-- Create continuous aggregate for 5-minute bars
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_5min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS time,
    symbol,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume) AS volume,
    sum(volume * vwap) / NULLIF(sum(volume), 0) AS vwap,
    sum(trade_count) AS trade_count
FROM ohlcv_1min
GROUP BY time_bucket('5 minutes', time), symbol
WITH NO DATA;

-- Create continuous aggregate for 1-hour bars
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1hour
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS time,
    symbol,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume) AS volume,
    sum(volume * vwap) / NULLIF(sum(volume), 0) AS vwap,
    sum(trade_count) AS trade_count
FROM ohlcv_1min
GROUP BY time_bucket('1 hour', time), symbol
WITH NO DATA;

-- Create continuous aggregate for daily bars
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1day
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS time,
    symbol,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume) AS volume,
    sum(volume * vwap) / NULLIF(sum(volume), 0) AS vwap,
    sum(trade_count) AS trade_count
FROM ohlcv_1min
GROUP BY time_bucket('1 day', time), symbol
WITH NO DATA;

-- Add refresh policies for continuous aggregates
-- Refresh 5-min aggregate every 5 minutes, covering last 1 hour
SELECT add_continuous_aggregate_policy('ohlcv_5min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

-- Refresh 1-hour aggregate every hour, covering last 24 hours
SELECT add_continuous_aggregate_policy('ohlcv_1hour',
    start_offset => INTERVAL '24 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Refresh daily aggregate once per day, covering last 7 days
SELECT add_continuous_aggregate_policy('ohlcv_1day',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Table to track symbols being monitored
CREATE TABLE IF NOT EXISTS monitored_symbols (
    symbol      TEXT PRIMARY KEY,
    name        TEXT,
    enabled     BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Table to track backfill progress
CREATE TABLE IF NOT EXISTS backfill_status (
    symbol          TEXT PRIMARY KEY,
    last_backfill   TIMESTAMPTZ,
    backfill_start  TIMESTAMPTZ,  -- Earliest date we have data for
    backfill_end    TIMESTAMPTZ,  -- Latest date we have data for
    status          TEXT DEFAULT 'pending',  -- pending, in_progress, completed, failed
    error_message   TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for finding symbols that need backfill
CREATE INDEX IF NOT EXISTS idx_backfill_status_pending
    ON backfill_status (status) WHERE status IN ('pending', 'failed');
