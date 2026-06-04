package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	commonskafka "github.com/trogers1052/trading-go-commons/kafka"
)

// WatchlistEvent represents a watchlist event from Kafka
type WatchlistEvent struct {
	EventType string             `json:"event_type"`
	Source    string             `json:"source"`
	Timestamp string             `json:"timestamp"`
	Data      WatchlistEventData `json:"data"`
}

// WatchlistEventData holds the data for different watchlist event types
type WatchlistEventData struct {
	// For WATCHLIST_UPDATED events
	AddedSymbols   []string         `json:"added_symbols,omitempty"`
	RemovedSymbols []string         `json:"removed_symbols,omitempty"`
	AllSymbols     []string         `json:"all_symbols,omitempty"`
	TotalCount     int              `json:"total_count,omitempty"`
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

// WatchlistConsumer consumes watchlist events from Kafka via the shared
// trading-go-commons kafka.ConsumerGroup (kafka-go). It preserves the previous
// sarama semantics: the configured topic/group, OffsetOldest for a fresh group,
// and mark-and-continue on handler error (the offset is committed even when the
// handler errors, so a poison message does not wedge the consumer).
type WatchlistConsumer struct {
	group   *commonskafka.ConsumerGroup
	topic   string
	handler SymbolHandler
}

// NewWatchlistConsumer creates a new watchlist consumer backed by the shared
// kafka.ConsumerGroup.
func NewWatchlistConsumer(brokers []string, topic, groupID string, handler SymbolHandler) (*WatchlistConsumer, error) {
	c := &WatchlistConsumer{
		topic:   topic,
		handler: handler,
	}

	group, err := commonskafka.NewConsumerGroup(
		brokers,
		groupID,
		[]string{topic},
		c.handle,
		commonskafka.WithInitialOffset(commonskafka.OffsetOldest),
		commonskafka.WithOnError(commonskafka.MarkAndContinue),
		commonskafka.WithConsumerClientID("market-data-ingestion"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}
	c.group = group

	return c, nil
}

// Start begins consuming messages. It blocks until ctx is cancelled, then
// returns nil (graceful shutdown).
func (c *WatchlistConsumer) Start(ctx context.Context) error {
	log.Printf("Starting watchlist consumer for topic: %s", c.topic)
	return c.group.Run(ctx)
}

// Close closes the consumer. The shared ConsumerGroup ties its lifecycle to the
// Run context, so this is a no-op for the runner; the field guard keeps it safe
// when the consumer was never fully constructed.
func (c *WatchlistConsumer) Close() error {
	if c.group == nil {
		return nil
	}
	return c.group.Close()
}

// handle adapts a shared kafka.Message into the existing processMessage logic.
// It is the Handler passed to the ConsumerGroup. Returning an error triggers the
// group's MarkAndContinue policy (log + commit + continue), which mirrors the old
// sarama ConsumeClaim behaviour of marking the message even when processing fails.
func (c *WatchlistConsumer) handle(ctx context.Context, msg *commonskafka.Message) error {
	return c.processMessage(ctx, msg.Value)
}

// processMessage handles a single watchlist event message body
func (c *WatchlistConsumer) processMessage(ctx context.Context, value []byte) error {
	var event WatchlistEvent
	if err := json.Unmarshal(value, &event); err != nil {
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
