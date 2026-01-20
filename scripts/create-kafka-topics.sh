#!/bin/bash
# Create Kafka topics for market data ingestion

set -e

KAFKA_BROKER="${KAFKA_BROKERS:-localhost:19092}"

echo "Creating Kafka topics for market data ingestion..."
echo "Broker: $KAFKA_BROKER"

# Create stock.quotes.realtime topic
echo ""
echo "Creating topic: stock.quotes.realtime"
docker exec trading-redpanda rpk topic create stock.quotes.realtime \
  --brokers "$KAFKA_BROKER" \
  --partitions 3 \
  --replication-factor 1 \
  --topic-config retention.ms=3600000 || echo "Topic may already exist"

echo ""
echo "Topics created successfully!"
echo ""
echo "To verify, run:"
echo "  docker exec trading-redpanda rpk topic list --brokers $KAFKA_BROKER"
echo ""
echo "To view topic details:"
echo "  docker exec trading-redpanda rpk topic describe stock.quotes.realtime --brokers $KAFKA_BROKER"
