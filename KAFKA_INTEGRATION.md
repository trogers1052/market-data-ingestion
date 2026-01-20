# Kafka Integration - Market Data Ingestion

## Overview

The market-data-ingestion service now publishes quote events to Kafka, enabling real-time processing by analytics and alert services.

## Changes Made

### 1. Kafka Producer (`internal/kafka/producer.go`)
- Created new Kafka producer using IBM/sarama
- Publishes `QUOTE_UPDATE` events to `stock.quotes.realtime` topic
- Supports single and batch publishing
- Gracefully handles Kafka failures (logs but doesn't fail ingestion)

### 2. Configuration Updates
- Changed `KAFKA_OHLCV_TOPIC` to `KAFKA_QUOTES_TOPIC` (matches architecture)
- Default topic: `stock.quotes.realtime`
- Default enabled: `true` (can be disabled with `KAFKA_ENABLED=false`)

### 3. Backfill Improvements
- **Fixed duplicate ingestion**: Now checks existing data before fetching
- Only fetches gaps in data (backwards or forwards extension)
- Skips backfill if data is already up to date
- Publishes historical data to Kafka during backfill

### 4. Realtime Service Updates
- Integrated Kafka producer
- Publishes quote events in batches (every 5 seconds or 100 bars)
- Non-blocking: Kafka failures don't stop database writes

## Event Schema

```json
{
  "event_type": "QUOTE_UPDATE",
  "source": "polygon",
  "timestamp": "2026-01-17T15:30:00Z",
  "schema_version": "1.0",
  "data": {
    "symbol": "AAPL",
    "time": "2026-01-17T15:30:00Z",
    "open": "175.50",
    "high": "175.75",
    "low": "175.45",
    "close": "175.60",
    "volume": 1000000,
    "vwap": "175.55",
    "trade_count": 5000
  }
}
```

## Setup

### 1. Create Kafka Topic

```bash
# Using the provided script
./scripts/create-kafka-topics.sh

# Or manually
docker exec trading-redpanda rpk topic create stock.quotes.realtime \
  --brokers localhost:19092 \
  --partitions 3 \
  --replication-factor 1 \
  --topic-config retention.ms=3600000
```

### 2. Environment Variables

```bash
# Required
POLYGON_API_KEY=your_key_here

# Kafka (optional, defaults shown)
KAFKA_BROKERS=localhost:19092
KAFKA_QUOTES_TOPIC=stock.quotes.realtime
KAFKA_ENABLED=true

# Database
DB_HOST=localhost
DB_PORT=5432
DB_USER=trader
DB_PASSWORD=REDACTED_PASSWORD
DB_NAME=market_data
```

### 3. Verify Events

```bash
# View topic in Redpanda Console
# http://localhost:8080

# Or using rpk
docker exec trading-redpanda rpk topic consume stock.quotes.realtime \
  --brokers localhost:19092 \
  --num 10
```

## Backfill Behavior

### Before Fix
- Always fetched full date range (e.g., 6 months)
- Re-ingested all data on every run
- Wasted API calls and time

### After Fix
- Checks existing data range first
- Only fetches gaps:
  - **Backwards extension**: If data starts after target start date
  - **Forwards extension**: If data ends more than 1 day ago
- Skips if data is already up to date
- Example log output:
  ```
  Starting backfill for AAPL (6 months)
    Data already up to date (range: 2025-07-17 to 2026-01-17), skipping backfill
  ```

## Testing

### Test Backfill (No Duplicates)
```bash
# First run - fetches data
./bin/market-data-ingestion -backfill -symbols AAPL

# Second run - skips (data exists)
./bin/market-data-ingestion -backfill -symbols AAPL
# Should see: "Data already up to date, skipping backfill"
```

### Test Kafka Publishing
```bash
# Start service
./bin/market-data-ingestion

# In another terminal, consume events
docker exec trading-redpanda rpk topic consume stock.quotes.realtime \
  --brokers localhost:19092
```

## Troubleshooting

### Kafka Producer Not Publishing
1. Check `KAFKA_ENABLED=true` in environment
2. Verify Kafka broker is accessible: `docker exec trading-redpanda rpk cluster info`
3. Check topic exists: `docker exec trading-redpanda rpk topic list`
4. Check logs for Kafka errors

### Backfill Still Fetching Duplicates
1. Verify database connection is working
2. Check `GetDataRange` query is returning correct values
3. Look for "Data already up to date" message in logs

### Events Not Appearing in Kafka
1. Verify topic exists and is accessible
2. Check producer logs for errors
3. Verify Kafka broker address is correct
4. Check network connectivity (if using Docker network)

## Next Steps

With quote events flowing to Kafka, you can now:
1. **Build Analytics Service** - Consume `stock.quotes.realtime` and calculate indicators
2. **Build Alert Service** - Consume quotes + indicators and evaluate alert rules
3. **Build Stock Persistence Service** - Consume quotes and update `stocks` table in trading_platform DB

## Architecture Flow

```
Polygon.io WebSocket
    │
    ▼
market-data-ingestion (Go)
    │
    ├─► TimescaleDB (ohlcv_1min table)
    │
    └─► Kafka (stock.quotes.realtime topic)
            │
            ├─► Analytics Service (future)
            ├─► Alert Service (future)
            └─► Stock Persistence Service (future)
```
