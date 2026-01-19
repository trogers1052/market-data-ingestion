# Market Data Ingestion Service

**Language:** Go
**Status:** Implemented
**Data Provider:** Polygon.io
**Storage:** TimescaleDB

## Purpose

Ingests historical and real-time 1-minute OHLCV (Open, High, Low, Close, Volume) data from Polygon.io and stores it in TimescaleDB for analytics and charting.

## Features

- **Historical Backfill**: Fetch up to 6 months of 1-minute bars for any symbol
- **Real-time Ingestion**: WebSocket connection to Polygon.io for live 1-minute bars
- **Market Hours Aware**: Only connects during market hours (configurable pre-market/after-hours)
- **TimescaleDB Storage**: Optimized time-series storage with automatic compression
- **Continuous Aggregates**: Auto-generated 5-min, 1-hour, and daily bars
- **Symbol Management**: Manual watchlist + auto-sync from Stock-Service positions
- **Rate Limiting**: Built-in rate limiter for Polygon API (configurable by tier)
- **Data Quality**: Gap detection, anomaly detection, coverage reports
- **Gap Filling**: Automatically detect and fill missing data

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    market-data-ingestion                        │
├─────────────────────────────────────────────────────────────────┤
│  Polygon.io ──→ Ingestor ──→ TimescaleDB                       │
│    │                              │                             │
│    │ WebSocket                    ├── ohlcv_1min (hypertable)   │
│    │ (real-time)                  ├── ohlcv_5min (continuous)   │
│    │                              ├── ohlcv_1hour (continuous)  │
│    └─ REST API                    └── ohlcv_1day (continuous)   │
│       (historical backfill)                                     │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Set up TimescaleDB

```bash
# Create database
createdb market_data

# Run migrations
psql market_data < migrations/001_create_ohlcv_hypertable.sql
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your Polygon.io API key
```

### 3. Add Symbols to Monitor

```bash
# Add individual symbols
go run ./cmd/ingestion -add-symbol AAPL
go run ./cmd/ingestion -add-symbol MSFT
go run ./cmd/ingestion -add-symbol GOOGL

# List monitored symbols
go run ./cmd/ingestion -list-symbols
```

### 4. Backfill Historical Data

```bash
# Backfill all monitored symbols (6 months by default)
go run ./cmd/ingestion -backfill

# Backfill specific symbols
go run ./cmd/ingestion -backfill -symbols AAPL,MSFT
```

### 5. Start Real-time Ingestion

```bash
# Run the service (connects during market hours)
go run ./cmd/ingestion
```

## Configuration

```env
# Polygon.io API
POLYGON_API_KEY=your_polygon_api_key_here

# TimescaleDB Database
DB_HOST=localhost
DB_PORT=5432
DB_USER=trader
DB_PASSWORD=REDACTED_PASSWORD
DB_NAME=market_data
DB_SSL_MODE=disable

# Ingestion Configuration
BACKFILL_MONTHS=6

# Market Hours (Eastern Time)
MARKET_OPEN_HOUR=4      # 4am for pre-market
MARKET_CLOSE_HOUR=20    # 8pm for after-hours
ENABLE_PRE_MARKET=true
ENABLE_AFTER_HOURS=true
```

## Database Schema

### ohlcv_1min (Hypertable)
```sql
CREATE TABLE ohlcv_1min (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    open        NUMERIC(12, 4) NOT NULL,
    high        NUMERIC(12, 4) NOT NULL,
    low         NUMERIC(12, 4) NOT NULL,
    close       NUMERIC(12, 4) NOT NULL,
    volume      BIGINT NOT NULL,
    vwap        NUMERIC(12, 4),
    trade_count INTEGER,
    PRIMARY KEY (symbol, time)
);
```

### Continuous Aggregates
- `ohlcv_5min` - 5-minute bars (auto-refreshed every 5 minutes)
- `ohlcv_1hour` - Hourly bars (auto-refreshed every hour)
- `ohlcv_1day` - Daily bars (auto-refreshed daily)

## CLI Commands

```bash
# Start the service (real-time + backfill check)
./market-data-ingestion

# Backfill only (then exit)
./market-data-ingestion -backfill

# Backfill specific symbols
./market-data-ingestion -backfill -symbols AAPL,MSFT,GOOGL

# Add a symbol to monitor
./market-data-ingestion -add-symbol NVDA

# Remove a symbol from monitoring
./market-data-ingestion -remove-symbol NVDA

# List all monitored symbols (with bar counts)
./market-data-ingestion -list-symbols

# Sync symbols from Stock-Service positions
./market-data-ingestion -sync-positions -stock-service-dsn "postgres://user:pass@host/db"

# Run data quality check (coverage, gaps, anomalies)
./market-data-ingestion -quality
./market-data-ingestion -quality -symbols AAPL -days 60

# Detect and fill data gaps
./market-data-ingestion -fill-gaps
./market-data-ingestion -fill-gaps -days 14

# Manually refresh continuous aggregates
./market-data-ingestion -refresh -days 7
```

## Storage Estimates

For 18 stocks:
- 6 months historical: ~5 MB (compressed)
- 1 year real-time: ~10 MB (compressed)
- 10 years: ~100 MB (compressed)

## Build & Deploy

```bash
# Build
go build -o bin/market-data-ingestion ./cmd/ingestion

# Docker
docker build -t market-data-ingestion .
docker run -d \
  --name market-data-ingestion \
  --env-file .env \
  --network trading-network \
  market-data-ingestion
```

## Querying Data

```sql
-- Get latest bars for a symbol
SELECT * FROM ohlcv_1min
WHERE symbol = 'AAPL'
ORDER BY time DESC
LIMIT 10;

-- Get daily bars from continuous aggregate
SELECT * FROM ohlcv_1day
WHERE symbol = 'AAPL'
  AND time >= NOW() - INTERVAL '30 days'
ORDER BY time;

-- Get VWAP and volume for last hour
SELECT
    time_bucket('5 minutes', time) AS bucket,
    symbol,
    SUM(volume) AS total_volume,
    SUM(volume * vwap) / SUM(volume) AS vwap
FROM ohlcv_1min
WHERE symbol = 'AAPL'
  AND time >= NOW() - INTERVAL '1 hour'
GROUP BY bucket, symbol
ORDER BY bucket;
```
