package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/trogers1052/market-data-ingestion/internal/metrics"
	"github.com/trogers1052/market-data-ingestion/internal/models"
	commonskafka "github.com/trogers1052/trading-go-commons/kafka"
)

// eventSource identifies the upstream market-data provider stamped on every
// published quote event (QuoteEvent.Source and the "source" message header).
const eventSource = "alpaca"

// eventType is the QUOTE_UPDATE constant stamped on every published quote event
// (QuoteEvent.EventType and the "event_type" message header).
const eventType = "QUOTE_UPDATE"

// publishTimeout bounds the broker ack wait (mirrors the old sarama
// Producer.Timeout / Net.{Dial,Read,Write}Timeout of 10s).
const publishTimeout = 10 * time.Second

// quoteEventClientID is the logical Kafka client identifier for the producer.
const quoteEventClientID = "market-data-ingestion"

// QuoteEvent represents a quote event published to Kafka
type QuoteEvent struct {
	EventType     string         `json:"event_type"`
	Source        string         `json:"source"`
	Timestamp     string         `json:"timestamp"`
	SchemaVersion string         `json:"schema_version"`
	IsBackfill    bool           `json:"is_backfill"`
	Data          QuoteEventData `json:"data"`
}

// QuoteEventData holds the quote data
type QuoteEventData struct {
	Symbol     string    `json:"symbol"`
	Time       time.Time `json:"time"`
	Open       string    `json:"open"` // Decimal as string for JSON
	High       string    `json:"high"`
	Low        string    `json:"low"`
	Close      string    `json:"close"`
	Volume     int64     `json:"volume"`
	VWAP       string    `json:"vwap,omitempty"`
	TradeCount int       `json:"trade_count,omitempty"`
}

// quoteProducer is the minimal subset of *commonskafka.Producer the Producer
// depends on. It lets tests inject a fake in place of a live broker writer while
// the production path uses the real shared kafka-go producer.
type quoteProducer interface {
	Publish(ctx context.Context, topic string, key, value []byte, headers ...commonskafka.Header) error
	PublishBatch(ctx context.Context, topic string, msgs []commonskafka.Message) error
	Close() error
}

// Producer handles publishing quote events to Kafka
type Producer struct {
	producer quoteProducer
	topic    string
	enabled  bool
}

// NewProducer creates a new Kafka producer for quote events backed by the shared
// trading-go-commons kafka.Producer (kafka-go). It preserves the previous sarama
// semantics: leader-only acks (WaitForLocal == RequireOne), Snappy compression,
// and a 10s broker timeout.
func NewProducer(brokers []string, topic string, enabled bool) (*Producer, error) {
	if !enabled {
		log.Println("Kafka producer disabled, quote events will not be published")
		return &Producer{enabled: false}, nil
	}

	producer, err := commonskafka.NewProducer(
		brokers,
		commonskafka.WithRequiredAcks(commonskafka.RequireOne), // == sarama WaitForLocal
		commonskafka.WithProducerCompression(commonskafka.CompressionSnappy),
		commonskafka.WithProducerTimeout(publishTimeout),
		commonskafka.WithClientID(quoteEventClientID),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	log.Printf("Kafka producer initialized (brokers: %v, topic: %s)", brokers, topic)
	return &Producer{
		producer: producer,
		topic:    topic,
		enabled:  true,
	}, nil
}

// quoteHeaders returns the two Kafka record headers stamped on every published
// quote message: event_type=QUOTE_UPDATE and source=alpaca.
func quoteHeaders() []commonskafka.Header {
	return []commonskafka.Header{
		{Key: "event_type", Value: []byte(eventType)},
		{Key: "source", Value: []byte(eventSource)},
	}
}

// quoteHeaderMap returns the same two record headers as a map, for the batch
// path (commonskafka.Message carries headers as a map).
func quoteHeaderMap() map[string][]byte {
	return map[string][]byte{
		"event_type": []byte(eventType),
		"source":     []byte(eventSource),
	}
}

// buildQuoteEvent constructs the QuoteEvent envelope (the JSON message body) for
// a bar. It is the single source of the value-marshaling shape shared by
// PublishQuote and PublishQuotesBatch.
func buildQuoteEvent(bar models.OHLCV, isBackfill bool) QuoteEvent {
	event := QuoteEvent{
		EventType:     eventType,
		Source:        eventSource,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		SchemaVersion: "1.0",
		IsBackfill:    isBackfill,
		Data: QuoteEventData{
			Symbol:     bar.Symbol,
			Time:       bar.Time,
			Open:       bar.Open.String(),
			High:       bar.High.String(),
			Low:        bar.Low.String(),
			Close:      bar.Close.String(),
			Volume:     bar.Volume,
			TradeCount: bar.TradeCount,
		},
	}
	if !bar.VWAP.IsZero() {
		event.Data.VWAP = bar.VWAP.String()
	}
	return event
}

// PublishQuote publishes a single quote event to Kafka
func (p *Producer) PublishQuote(ctx context.Context, bar models.OHLCV, isBackfill bool) error {
	if !p.enabled {
		return nil // Silently skip if disabled
	}

	event := buildQuoteEvent(bar, isBackfill)

	// Marshal to JSON
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal quote event: %w", err)
	}

	// Publish with symbol as key for partitioning and the two record headers
	// (event_type=QUOTE_UPDATE, source=alpaca) stamped on the message.
	if err := p.producer.Publish(ctx, p.topic, []byte(bar.Symbol), value, quoteHeaders()...); err != nil {
		metrics.KafkaPublishErrors.Inc()
		return fmt.Errorf("failed to send message to Kafka: %w", err)
	}

	metrics.QuotesPublished.Inc()

	// Per-quote publish log intentionally omitted to avoid flooding during market hours.
	// Batch publishes are logged in PublishQuotesBatch.

	return nil
}

// PublishQuotesBatch publishes multiple quote events in a single batched
// WriteMessages round-trip (all-or-nothing, mirroring the old sarama
// SendMessages call). Each message carries the symbol key, the QuoteEvent JSON
// body, and the two record headers (event_type=QUOTE_UPDATE, source=alpaca).
func (p *Producer) PublishQuotesBatch(ctx context.Context, bars []models.OHLCV, isBackfill bool) error {
	if !p.enabled {
		return nil // Silently skip if disabled
	}

	if len(bars) == 0 {
		return nil
	}

	messages := make([]commonskafka.Message, 0, len(bars))
	for _, bar := range bars {
		event := buildQuoteEvent(bar, isBackfill)

		value, err := json.Marshal(event)
		if err != nil {
			log.Printf("Warning: failed to marshal quote event for %s: %v", bar.Symbol, err)
			continue
		}

		messages = append(messages, commonskafka.Message{
			Topic:   p.topic,
			Key:     []byte(bar.Symbol),
			Value:   value,
			Headers: quoteHeaderMap(),
		})
	}

	if len(messages) == 0 {
		return nil
	}

	// Send batch in one all-or-nothing call.
	if err := p.producer.PublishBatch(ctx, p.topic, messages); err != nil {
		metrics.KafkaPublishErrors.Add(float64(len(messages)))
		return fmt.Errorf("failed to send batch to Kafka: %w", err)
	}

	metrics.QuotesPublished.Add(float64(len(messages)))

	log.Printf("Published %d quote events to %s", len(messages), p.topic)
	return nil
}

// Close closes the Kafka producer
func (p *Producer) Close() error {
	if !p.enabled || p.producer == nil {
		return nil
	}
	return p.producer.Close()
}
