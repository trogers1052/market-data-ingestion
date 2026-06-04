package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kgo "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testkit "github.com/trogers1052/trading-testkit"

	"github.com/trogers1052/market-data-ingestion/internal/models"
)

// readRecords reads exactly n records from topic starting at the earliest
// offset, returning them in offset order. It bounds the wait so a hung broker
// fails the test instead of blocking forever.
func readRecords(t *testing.T, brokers, topic string, n int) []kgo.Message {
	t.Helper()

	reader := kgo.NewReader(kgo.ReaderConfig{
		Brokers:     []string{brokers},
		Topic:       topic,
		Partition:   0,
		StartOffset: kgo.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	t.Cleanup(func() { _ = reader.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out := make([]kgo.Message, 0, n)
	for len(out) < n {
		m, err := reader.ReadMessage(ctx)
		require.NoError(t, err, "reading record %d/%d", len(out)+1, n)
		out = append(out, m)
	}
	return out
}

// headerMap collapses a kafka-go record's headers into a map for assertion.
func headerMap(m kgo.Message) map[string]string {
	got := map[string]string{}
	for _, h := range m.Headers {
		got[h.Key] = string(h.Value)
	}
	return got
}

// TestProducer_Integration_SingleHeadersRoundTrip publishes a single quote
// through the real shared kafka-go producer against a live Redpanda broker and
// reads it back, asserting the symbol key, the QuoteEvent JSON body, and the two
// record headers (event_type=QUOTE_UPDATE, source=alpaca) all round-trip — the
// behaviour the old sarama TestQuoteEventMessageShape locked in, now verified
// against a real broker.
func TestProducer_Integration_SingleHeadersRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	rp := testkit.NewRedpandaContainer(t)
	const topic = "stock.quotes.realtime.itest.single"
	rp.CreateTopic(t, topic, 1)

	p, err := NewProducer([]string{rp.Brokers}, topic, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	bar := sampleBar("AAPL")
	require.NoError(t, p.PublishQuote(context.Background(), bar, false))

	recs := readRecords(t, rp.Brokers, topic, 1)
	require.Len(t, recs, 1)

	rec := recs[0]
	assert.Equal(t, "AAPL", string(rec.Key), "message key must be the symbol")

	// Headers must round-trip through the real broker.
	hdrs := headerMap(rec)
	assert.Equal(t, "QUOTE_UPDATE", hdrs["event_type"], "event_type header must round-trip")
	assert.Equal(t, "alpaca", hdrs["source"], "source header must round-trip")

	// Body envelope must be intact.
	var event QuoteEvent
	require.NoError(t, json.Unmarshal(rec.Value, &event))
	assert.Equal(t, "QUOTE_UPDATE", event.EventType)
	assert.Equal(t, "alpaca", event.Source)
	assert.Equal(t, "1.0", event.SchemaVersion)
	assert.Equal(t, "AAPL", event.Data.Symbol)
}

// TestProducer_Integration_BatchDeliversAll publishes a batch through the real
// shared kafka-go producer against a live Redpanda broker and reads every record
// back, asserting the whole slice is delivered (the all-or-nothing batch path)
// and that EACH record carries the event_type/source headers.
func TestProducer_Integration_BatchDeliversAll(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	rp := testkit.NewRedpandaContainer(t)
	const topic = "stock.quotes.realtime.itest.batch"
	rp.CreateTopic(t, topic, 1)

	p, err := NewProducer([]string{rp.Brokers}, topic, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	symbols := []string{"AAPL", "MSFT", "NVDA", "TSLA"}
	bars := make([]models.OHLCV, 0, len(symbols))
	for _, s := range symbols {
		bars = append(bars, sampleBar(s))
	}

	require.NoError(t, p.PublishQuotesBatch(context.Background(), bars, true))

	recs := readRecords(t, rp.Brokers, topic, len(symbols))
	require.Len(t, recs, len(symbols), "batch must deliver every message")

	gotKeys := make([]string, 0, len(recs))
	for _, rec := range recs {
		gotKeys = append(gotKeys, string(rec.Key))
		hdrs := headerMap(rec)
		assert.Equal(t, "QUOTE_UPDATE", hdrs["event_type"], "every batched record must carry event_type")
		assert.Equal(t, "alpaca", hdrs["source"], "every batched record must carry source")

		var event QuoteEvent
		require.NoError(t, json.Unmarshal(rec.Value, &event))
		assert.True(t, event.IsBackfill, "is_backfill must propagate through the batch")
	}
	// Partition 0, single producer: order is preserved.
	assert.Equal(t, symbols, gotKeys, "all batched symbols must be present in order")
}
