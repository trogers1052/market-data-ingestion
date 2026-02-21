package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/trogers1052/market-data-ingestion/internal/models"
)

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

// Producer handles publishing quote events to Kafka
type Producer struct {
	producer sarama.SyncProducer
	topic    string
	enabled  bool
}

// NewProducer creates a new Kafka producer for quote events
func NewProducer(brokers []string, topic string, enabled bool) (*Producer, error) {
	if !enabled {
		log.Println("Kafka producer disabled, quote events will not be published")
		return &Producer{enabled: false}, nil
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal // Wait for leader acknowledgment
	config.Producer.Retry.Max = 3
	config.Producer.Compression = sarama.CompressionSnappy
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second
	config.Producer.Timeout = 10 * time.Second

	producer, err := sarama.NewSyncProducer(brokers, config)
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

// PublishQuote publishes a single quote event to Kafka
func (p *Producer) PublishQuote(ctx context.Context, bar models.OHLCV, isBackfill bool) error {
	if !p.enabled {
		return nil // Silently skip if disabled
	}

	event := QuoteEvent{
		EventType:     "QUOTE_UPDATE",
		Source:        "polygon",
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

	// Marshal to JSON
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal quote event: %w", err)
	}

	// Create message with symbol as key for partitioning
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(bar.Symbol),
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("event_type"),
				Value: []byte("QUOTE_UPDATE"),
			},
			{
				Key:   []byte("source"),
				Value: []byte("polygon"),
			},
		},
	}

	// Send message
	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message to Kafka: %w", err)
	}

	log.Printf("Published quote event: %s @ %s to %s[%d:%d]",
		bar.Symbol, bar.Time.Format("15:04:05"), p.topic, partition, offset)

	return nil
}

// PublishQuotesBatch publishes multiple quote events in a batch
func (p *Producer) PublishQuotesBatch(ctx context.Context, bars []models.OHLCV, isBackfill bool) error {
	if !p.enabled {
		return nil // Silently skip if disabled
	}

	if len(bars) == 0 {
		return nil
	}

	messages := make([]*sarama.ProducerMessage, 0, len(bars))
	for _, bar := range bars {
		event := QuoteEvent{
			EventType:     "QUOTE_UPDATE",
			Source:        "polygon",
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

		value, err := json.Marshal(event)
		if err != nil {
			log.Printf("Warning: failed to marshal quote event for %s: %v", bar.Symbol, err)
			continue
		}

		msg := &sarama.ProducerMessage{
			Topic: p.topic,
			Key:   sarama.StringEncoder(bar.Symbol),
			Value: sarama.ByteEncoder(value),
			Headers: []sarama.RecordHeader{
				{
					Key:   []byte("event_type"),
					Value: []byte("QUOTE_UPDATE"),
				},
				{
					Key:   []byte("source"),
					Value: []byte("polygon"),
				},
			},
		}
		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return nil
	}

	// Send batch
	err := p.producer.SendMessages(messages)
	if err != nil {
		return fmt.Errorf("failed to send batch to Kafka: %w", err)
	}

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
