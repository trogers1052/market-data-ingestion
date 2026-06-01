package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestNewProducer_Disabled(t *testing.T) {
	p, err := NewProducer([]string{"b:9092"}, "topic", false)
	require.NoError(t, err)
	assert.False(t, p.enabled)

	// All publish ops are no-ops when disabled
	require.NoError(t, p.PublishQuote(context.Background(), sampleBar("AAPL"), false))
	require.NoError(t, p.PublishQuotesBatch(context.Background(), []models.OHLCV{sampleBar("AAPL")}, false))
	require.NoError(t, p.Close())
}

func TestNewProducer_Enabled_ConnectError(t *testing.T) {
	// No broker is listening on this port, so creating a sync producer
	// (which requires fetching cluster metadata) must fail.
	_, err := NewProducer([]string{"127.0.0.1:1"}, "topic", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create Kafka producer")
}

func TestPublishQuote(t *testing.T) {
	mp := mocks.NewSyncProducer(t, nil)
	mp.ExpectSendMessageAndSucceed()
	p := &Producer{producer: mp, topic: "quotes", enabled: true}

	require.NoError(t, p.PublishQuote(context.Background(), sampleBar("AAPL"), false))
	require.NoError(t, p.Close())
}

func TestPublishQuote_NoVWAP(t *testing.T) {
	mp := mocks.NewSyncProducer(t, nil)
	mp.ExpectSendMessageAndSucceed()
	p := &Producer{producer: mp, topic: "quotes", enabled: true}

	bar := sampleBar("AAPL")
	bar.VWAP = decimal.Zero
	require.NoError(t, p.PublishQuote(context.Background(), bar, true))
	require.NoError(t, p.Close())
}

func TestPublishQuote_SendError(t *testing.T) {
	mp := mocks.NewSyncProducer(t, nil)
	mp.ExpectSendMessageAndFail(errors.New("kafka down"))
	p := &Producer{producer: mp, topic: "quotes", enabled: true}

	err := p.PublishQuote(context.Background(), sampleBar("AAPL"), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send message")
	mp.Close()
}

func TestPublishQuotesBatch(t *testing.T) {
	mp := mocks.NewSyncProducer(t, nil)
	mp.ExpectSendMessageAndSucceed()
	mp.ExpectSendMessageAndSucceed()
	p := &Producer{producer: mp, topic: "quotes", enabled: true}

	bars := []models.OHLCV{sampleBar("AAPL"), sampleBar("MSFT")}
	require.NoError(t, p.PublishQuotesBatch(context.Background(), bars, true))
	require.NoError(t, p.Close())
}

func TestPublishQuotesBatch_Empty(t *testing.T) {
	mp := mocks.NewSyncProducer(t, nil)
	p := &Producer{producer: mp, topic: "quotes", enabled: true}
	require.NoError(t, p.PublishQuotesBatch(context.Background(), nil, false))
	require.NoError(t, p.Close())
}

func TestPublishQuotesBatch_SendError(t *testing.T) {
	mp := mocks.NewSyncProducer(t, nil)
	mp.ExpectSendMessageAndFail(errors.New("kafka down"))
	p := &Producer{producer: mp, topic: "quotes", enabled: true}

	err := p.PublishQuotesBatch(context.Background(), []models.OHLCV{sampleBar("AAPL")}, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send batch")
	mp.Close()
}

func TestProducer_Close_NilProducer(t *testing.T) {
	p := &Producer{enabled: true, producer: nil}
	require.NoError(t, p.Close())
}

func TestQuoteEventMessageShape(t *testing.T) {
	mp := mocks.NewSyncProducer(t, nil)
	mp.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		assert.Equal(t, "quotes", msg.Topic)
		key, _ := msg.Key.Encode()
		assert.Equal(t, "AAPL", string(key))
		require.Len(t, msg.Headers, 2)
		assert.Equal(t, "event_type", string(msg.Headers[0].Key))
		return nil
	})
	p := &Producer{producer: mp, topic: "quotes", enabled: true}
	require.NoError(t, p.PublishQuote(context.Background(), sampleBar("AAPL"), false))
	require.NoError(t, p.Close())
}
