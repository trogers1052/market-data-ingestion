package kafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	schemas "github.com/trogers1052/trading-event-schemas"

	"github.com/trogers1052/market-data-ingestion/internal/models"
)

// buildQuoteEvent mirrors the exact QuoteEvent construction performed by
// Producer.PublishQuote / PublishQuotesBatch in producer.go, so the schema
// contract test validates the real producer marshaling path (same field
// formatting, same eventSource const, same "1.0"/"QUOTE_UPDATE" literals).
func buildQuoteEvent(bar models.OHLCV, isBackfill bool) QuoteEvent {
	event := QuoteEvent{
		EventType:     "QUOTE_UPDATE",
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

// TestQuoteEvent_SchemaContract asserts that a QuoteEvent built and marshaled
// exactly as the producer does validates against the canonical quote_event
// JSON Schema shipped by trading-event-schemas (the single source of truth
// shared with Python consumers). This catches producer/contract drift.
func TestQuoteEvent_SchemaContract(t *testing.T) {
	bar := models.OHLCV{
		Time:       time.Date(2026, 3, 10, 14, 30, 0, 0, time.UTC),
		Symbol:     "AAPL",
		Open:       decimal.RequireFromString("150.25"),
		High:       decimal.RequireFromString("150.50"),
		Low:        decimal.RequireFromString("150.00"),
		Close:      decimal.RequireFromString("150.40"),
		Volume:     10000,
		VWAP:       decimal.RequireFromString("150.30"),
		TradeCount: 450,
	}
	require.NoError(t, bar.Validate(), "test fixture bar must be a valid OHLCV")

	event := buildQuoteEvent(bar, false)

	value, err := json.Marshal(event)
	require.NoError(t, err, "producer-marshaled QuoteEvent must serialize")

	err = schemas.Validate("quote_event", value)
	require.NoError(t, err, "producer QuoteEvent must conform to quote_event schema:\n%s", value)
}

// TestQuoteEvent_SchemaContract_Backfill exercises the is_backfill=true path
// and a bar without VWAP / trade_count (the omitempty optional fields), which
// is the realistic minimal quote the producer can emit.
func TestQuoteEvent_SchemaContract_Backfill(t *testing.T) {
	bar := models.OHLCV{
		Time:   time.Date(2026, 3, 9, 21, 0, 0, 0, time.UTC),
		Symbol: "MSFT",
		Open:   decimal.RequireFromString("415.10"),
		High:   decimal.RequireFromString("417.00"),
		Low:    decimal.RequireFromString("414.80"),
		Close:  decimal.RequireFromString("416.25"),
		Volume: 0, // schema permits volume >= 0
	}
	require.NoError(t, bar.Validate(), "test fixture bar must be a valid OHLCV")

	event := buildQuoteEvent(bar, true)

	value, err := json.Marshal(event)
	require.NoError(t, err, "producer-marshaled backfill QuoteEvent must serialize")

	err = schemas.Validate("quote_event", value)
	require.NoError(t, err, "producer backfill QuoteEvent must conform to quote_event schema:\n%s", value)
}
