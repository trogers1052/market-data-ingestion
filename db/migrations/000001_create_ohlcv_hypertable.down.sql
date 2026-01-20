-- Down migration: Remove OHLCV tables and related objects

-- Remove continuous aggregate policies first
SELECT remove_continuous_aggregate_policy('ohlcv_1day', if_exists => TRUE);
SELECT remove_continuous_aggregate_policy('ohlcv_1hour', if_exists => TRUE);
SELECT remove_continuous_aggregate_policy('ohlcv_5min', if_exists => TRUE);

-- Drop continuous aggregates (materialized views)
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1day CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ohlcv_1hour CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ohlcv_5min CASCADE;

-- Remove compression policy
SELECT remove_compression_policy('ohlcv_1min', if_exists => TRUE);

-- Drop the main hypertable
DROP TABLE IF EXISTS ohlcv_1min CASCADE;

-- Drop tracking tables
DROP TABLE IF EXISTS backfill_status CASCADE;
DROP TABLE IF EXISTS monitored_symbols CASCADE;
