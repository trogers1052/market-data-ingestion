package kafka

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testkit "github.com/trogers1052/trading-testkit"
)

func TestContract_QuoteEvent(t *testing.T) {
	data := testkit.LoadContract("quote_event.json")

	var event QuoteEvent
	err := json.Unmarshal(data, &event)
	require.NoError(t, err, "QuoteEvent must unmarshal from contract fixture")

	assert.Equal(t, "QUOTE_UPDATE", event.EventType)
	assert.Equal(t, "alpaca", event.Source)
	assert.Equal(t, "1.0", event.SchemaVersion)
	assert.Equal(t, "AAPL", event.Data.Symbol)
	assert.Equal(t, "150.25", event.Data.Open)
	assert.Equal(t, int64(10000), event.Data.Volume)
}

func TestContract_WatchlistEventAdded(t *testing.T) {
	data := testkit.LoadContract("watchlist_event_added.json")

	var event WatchlistEvent
	err := json.Unmarshal(data, &event)
	require.NoError(t, err, "WatchlistEvent must unmarshal from contract fixture")

	assert.Equal(t, "WATCHLIST_SYMBOL_ADDED", event.EventType)
	assert.Equal(t, "NVDA", event.Data.Symbol)
	assert.Equal(t, "NVIDIA Corporation", event.Data.Name)
}

func TestContract_WatchlistEventUpdated(t *testing.T) {
	data := testkit.LoadContract("watchlist_event_updated.json")

	var event WatchlistEvent
	err := json.Unmarshal(data, &event)
	require.NoError(t, err, "WatchlistEvent must unmarshal from contract fixture")

	assert.Equal(t, "WATCHLIST_UPDATED", event.EventType)
	assert.Len(t, event.Data.AddedSymbols, 2)
	assert.Len(t, event.Data.RemovedSymbols, 1)
	assert.Equal(t, 4, event.Data.TotalCount)
	assert.Len(t, event.Data.Stocks, 2)
}

func TestContract_QuoteEvent_Roundtrip(t *testing.T) {
	data := testkit.LoadContract("quote_event.json")

	var original QuoteEvent
	err := json.Unmarshal(data, &original)
	require.NoError(t, err, "first unmarshal must succeed")

	remarshaled, err := json.Marshal(original)
	require.NoError(t, err, "re-marshal must succeed")

	var roundtripped QuoteEvent
	err = json.Unmarshal(remarshaled, &roundtripped)
	require.NoError(t, err, "second unmarshal must succeed")

	assert.Equal(t, original.EventType, roundtripped.EventType)
	assert.Equal(t, original.Source, roundtripped.Source)
	assert.Equal(t, original.SchemaVersion, roundtripped.SchemaVersion)
	assert.Equal(t, original.IsBackfill, roundtripped.IsBackfill)
	assert.Equal(t, original.Data.Symbol, roundtripped.Data.Symbol)
	assert.Equal(t, original.Data.Open, roundtripped.Data.Open)
	assert.Equal(t, original.Data.High, roundtripped.Data.High)
	assert.Equal(t, original.Data.Low, roundtripped.Data.Low)
	assert.Equal(t, original.Data.Close, roundtripped.Data.Close)
	assert.Equal(t, original.Data.Volume, roundtripped.Data.Volume)
	assert.Equal(t, original.Data.VWAP, roundtripped.Data.VWAP)
	assert.Equal(t, original.Data.TradeCount, roundtripped.Data.TradeCount)
	assert.Equal(t, original.Data.Time, roundtripped.Data.Time)
}
