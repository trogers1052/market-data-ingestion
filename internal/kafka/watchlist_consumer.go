package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/IBM/sarama"
)

// WatchlistEvent represents a watchlist event from Kafka
type WatchlistEvent struct {
	EventType string              `json:"event_type"`
	Source    string              `json:"source"`
	Timestamp string              `json:"timestamp"`
	Data      WatchlistEventData  `json:"data"`
}

// WatchlistEventData holds the data for different watchlist event types
type WatchlistEventData struct {
	// For WATCHLIST_UPDATED events
	AddedSymbols   []string        `json:"added_symbols,omitempty"`
	RemovedSymbols []string        `json:"removed_symbols,omitempty"`
	AllSymbols     []string        `json:"all_symbols,omitempty"`
	TotalCount     int             `json:"total_count,omitempty"`
	Stocks         []WatchlistStock `json:"stocks,omitempty"`

	// For WATCHLIST_SYMBOL_ADDED/REMOVED events
	Symbol string `json:"symbol,omitempty"`
	Name   string `json:"name,omitempty"`
}

// WatchlistStock represents stock details in the event
type WatchlistStock struct {
	Symbol        string `json:"symbol"`
	Name          string `json:"name"`
	InstrumentURL string `json:"instrument_url"`
	AddedAt       string `json:"added_at"`
}

// SymbolHandler is a callback for handling symbol changes
type SymbolHandler interface {
	OnSymbolAdded(ctx context.Context, symbol, name string) error
	OnSymbolRemoved(ctx context.Context, symbol string) error
}

// WatchlistConsumer consumes watchlist events from Kafka
type WatchlistConsumer struct {
	consumer sarama.ConsumerGroup
	topic    string
	handler  SymbolHandler
	ready    chan bool
}

// NewWatchlistConsumer creates a new watchlist consumer
func NewWatchlistConsumer(brokers []string, topic, groupID string, handler SymbolHandler) (*WatchlistConsumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	return &WatchlistConsumer{
		consumer: consumer,
		topic:    topic,
		handler:  handler,
		ready:    make(chan bool),
	}, nil
}

// Start begins consuming messages
func (c *WatchlistConsumer) Start(ctx context.Context) error {
	log.Printf("Starting watchlist consumer for topic: %s", c.topic)

	for {
		// Consume handles reconnection automatically
		if err := c.consumer.Consume(ctx, []string{c.topic}, c); err != nil {
			if ctx.Err() != nil {
				return nil // Context cancelled, clean shutdown
			}
			log.Printf("Consumer error: %v, will retry...", err)
		}

		// Check if context was cancelled
		if ctx.Err() != nil {
			return nil
		}

		c.ready = make(chan bool)
	}
}

// Close closes the consumer
func (c *WatchlistConsumer) Close() error {
	return c.consumer.Close()
}

// Setup is run at the beginning of a new session
func (c *WatchlistConsumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session
func (c *WatchlistConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim processes messages from a claim
func (c *WatchlistConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			if err := c.processMessage(session.Context(), message); err != nil {
				log.Printf("Error processing message: %v", err)
				// Continue processing other messages
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// processMessage handles a single watchlist event message
func (c *WatchlistConsumer) processMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var event WatchlistEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	log.Printf("Received watchlist event: %s", event.EventType)

	switch event.EventType {
	case "WATCHLIST_UPDATED":
		// Process added symbols
		for _, symbol := range event.Data.AddedSymbols {
			name := symbol
			// Find the name from stocks list
			for _, stock := range event.Data.Stocks {
				if stock.Symbol == symbol {
					name = stock.Name
					break
				}
			}
			if err := c.handler.OnSymbolAdded(ctx, strings.ToUpper(symbol), name); err != nil {
				log.Printf("Error adding symbol %s: %v", symbol, err)
			}
		}

		// Process removed symbols
		for _, symbol := range event.Data.RemovedSymbols {
			if err := c.handler.OnSymbolRemoved(ctx, strings.ToUpper(symbol)); err != nil {
				log.Printf("Error removing symbol %s: %v", symbol, err)
			}
		}

	case "WATCHLIST_SYMBOL_ADDED":
		symbol := strings.ToUpper(event.Data.Symbol)
		name := event.Data.Name
		if name == "" {
			name = symbol
		}
		if err := c.handler.OnSymbolAdded(ctx, symbol, name); err != nil {
			return fmt.Errorf("failed to add symbol %s: %w", symbol, err)
		}

	case "WATCHLIST_SYMBOL_REMOVED":
		symbol := strings.ToUpper(event.Data.Symbol)
		if err := c.handler.OnSymbolRemoved(ctx, symbol); err != nil {
			return fmt.Errorf("failed to remove symbol %s: %w", symbol, err)
		}

	default:
		log.Printf("Unknown event type: %s", event.EventType)
	}

	return nil
}
