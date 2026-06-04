package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonskafka "github.com/trogers1052/trading-go-commons/kafka"

	"github.com/trogers1052/market-data-ingestion/internal/models"
)

func sampleBar(symbol string) models.OHLCV {
	return models.OHLCV{
		Time:       time.Date(2026, 1, 2, 15, 0, 0, 0, time.UTC),
		Symbol:     symbol,
		Open:       decimal.NewFromFloat(100.5),
		High:       decimal.NewFromFloat(101),
		Low:        decimal.NewFromFloat(100),
		Close:      decimal.NewFromFloat(100.75),
		Volume:     1000,
		VWAP:       decimal.NewFromFloat(100.6),
		TradeCount: 10,
	}
}

// publishCall records a single Publish invocation.
type publishCall struct {
	topic   string
	key     []byte
	value   []byte
	headers []commonskafka.Header
}

// batchCall records a single PublishBatch invocation.
type batchCall struct {
	topic string
	msgs  []commonskafka.Message
}

// fakeQuoteProducer is an in-memory quoteProducer used to assert exactly what the
// Producer hands to the shared kafka.Producer (topic, key, value, and the
// event_type/source record headers) without needing a live broker.
type fakeQuoteProducer struct {
	publishes  []publishCall
	batches    []batchCall
	publishErr error
	batchErr   error
	closed     bool
}

func (f *fakeQuoteProducer) Publish(_ context.Context, topic string, key, value []byte, headers ...commonskafka.Header) error {
	if f.publishErr != nil {
		return f.publishErr
	}
	f.publishes = append(f.publishes, publishCall{topic: topic, key: key, value: value, headers: headers})
	return nil
}

func (f *fakeQuoteProducer) PublishBatch(_ context.Context, topic string, msgs []commonskafka.Message) error {
	if f.batchErr != nil {
		return f.batchErr
	}
	f.batches = append(f.batches, batchCall{topic: topic, msgs: msgs})
	return nil
}

func (f *fakeQuoteProducer) Close() error {
	f.closed = true
	return nil
}

// assertQuoteHeaders asserts the two required record headers are present with the
// expected values, regardless of slice order.
func assertQuoteHeaders(t *testing.T, headers []commonskafka.Header) {
	t.Helper()
	require.Len(t, headers, 2)
	got := map[string]string{}
	for _, h := range headers {
		got[h.Key] = string(h.Value)
	}
	assert.Equal(t, "QUOTE_UPDATE", got["event_type"], "event_type header must be QUOTE_UPDATE")
	assert.Equal(t, "alpaca", got["source"], "source header must be alpaca")
}

// assertQuoteHeaderMap asserts the same two headers on the map form used by the
// batch path.
func assertQuoteHeaderMap(t *testing.T, headers map[string][]byte) {
	t.Helper()
	require.Len(t, headers, 2)
	assert.Equal(t, "QUOTE_UPDATE", string(headers["event_type"]), "event_type header must be QUOTE_UPDATE")
	assert.Equal(t, "alpaca", string(headers["source"]), "source header must be alpaca")
}

func TestNewProducer_Disabled(t *testing.T) {
	p, err := NewProducer([]string{"b:9092"}, "topic", false)
	require.NoError(t, err)
	assert.False(t, p.enabled)

	// All publish ops are no-ops when disabled
	require.NoError(t, p.PublishQuote(context.Background(), sampleBar("AAPL"), false))
	require.NoError(t, p.PublishQuotesBatch(context.Background(), []models.OHLCV{sampleBar("AAPL")}, false))
	require.NoError(t, p.Close())
}

func TestNewProducer_Enabled(t *testing.T) {
	// The shared kafka-go producer connects lazily, so construction with a
	// well-formed broker list succeeds without a live broker.
	p, err := NewProducer([]string{"127.0.0.1:9092"}, "topic", true)
	require.NoError(t, err)
	assert.True(t, p.enabled)
	require.NoError(t, p.Close())
}

func TestNewProducer_NoBrokers(t *testing.T) {
	// The shared producer rejects an empty broker list at construction.
	_, err := NewProducer(nil, "topic", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create Kafka producer")
}

func TestPublishQuote(t *testing.T) {
	fp := &fakeQuoteProducer{}
	p := &Producer{producer: fp, topic: "quotes", enabled: true}

	require.NoError(t, p.PublishQuote(context.Background(), sampleBar("AAPL"), false))
	require.NoError(t, p.Close())

	require.Len(t, fp.publishes, 1)
	assert.Equal(t, "quotes", fp.publishes[0].topic)
	assert.Equal(t, "AAPL", string(fp.publishes[0].key))
	assertQuoteHeaders(t, fp.publishes[0].headers)
	assert.True(t, fp.closed)
}

func TestPublishQuote_NoVWAP(t *testing.T) {
	fp := &fakeQuoteProducer{}
	p := &Producer{producer: fp, topic: "quotes", enabled: true}

	bar := sampleBar("AAPL")
	bar.VWAP = decimal.Zero
	require.NoError(t, p.PublishQuote(context.Background(), bar, true))
	require.NoError(t, p.Close())

	require.Len(t, fp.publishes, 1)
	assertQuoteHeaders(t, fp.publishes[0].headers)
}

func TestPublishQuote_SendError(t *testing.T) {
	fp := &fakeQuoteProducer{publishErr: errors.New("kafka down")}
	p := &Producer{producer: fp, topic: "quotes", enabled: true}

	err := p.PublishQuote(context.Background(), sampleBar("AAPL"), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send message")
}

func TestPublishQuotesBatch(t *testing.T) {
	fp := &fakeQuoteProducer{}
	p := &Producer{producer: fp, topic: "quotes", enabled: true}

	bars := []models.OHLCV{sampleBar("AAPL"), sampleBar("MSFT")}
	require.NoError(t, p.PublishQuotesBatch(context.Background(), bars, true))
	require.NoError(t, p.Close())

	// All messages must go out in a SINGLE batched call (all-or-nothing).
	require.Len(t, fp.batches, 1, "batch must be a single PublishBatch call")
	require.Len(t, fp.batches[0].msgs, 2, "batch must carry every message")
	assert.Equal(t, "quotes", fp.batches[0].topic)
	assert.Equal(t, "AAPL", string(fp.batches[0].msgs[0].Key))
	assert.Equal(t, "MSFT", string(fp.batches[0].msgs[1].Key))
	for _, m := range fp.batches[0].msgs {
		assertQuoteHeaderMap(t, m.Headers)
	}
}

func TestPublishQuotesBatch_Empty(t *testing.T) {
	fp := &fakeQuoteProducer{}
	p := &Producer{producer: fp, topic: "quotes", enabled: true}
	require.NoError(t, p.PublishQuotesBatch(context.Background(), nil, false))
	require.NoError(t, p.Close())
	assert.Empty(t, fp.batches, "empty batch must not call PublishBatch")
}

func TestPublishQuotesBatch_SendError(t *testing.T) {
	fp := &fakeQuoteProducer{batchErr: errors.New("kafka down")}
	p := &Producer{producer: fp, topic: "quotes", enabled: true}

	err := p.PublishQuotesBatch(context.Background(), []models.OHLCV{sampleBar("AAPL")}, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send batch")
}

func TestProducer_Close_NilProducer(t *testing.T) {
	p := &Producer{enabled: true, producer: nil}
	require.NoError(t, p.Close())
}

// TestQuoteEventMessageShape preserves the old test's intent: every published
// quote carries the symbol key and the two record headers
// (event_type=QUOTE_UPDATE, source=alpaca) on the SINGLE-message path.
func TestQuoteEventMessageShape(t *testing.T) {
	fp := &fakeQuoteProducer{}
	p := &Producer{producer: fp, topic: "quotes", enabled: true}
	require.NoError(t, p.PublishQuote(context.Background(), sampleBar("AAPL"), false))
	require.NoError(t, p.Close())

	require.Len(t, fp.publishes, 1)
	call := fp.publishes[0]
	assert.Equal(t, "quotes", call.topic)
	assert.Equal(t, "AAPL", string(call.key))
	assertQuoteHeaders(t, call.headers)
}
