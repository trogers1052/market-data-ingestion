package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonskafka "github.com/trogers1052/trading-go-commons/kafka"
	testkit "github.com/trogers1052/trading-testkit"
)

// recordingHandler is a concurrency-safe SymbolHandler for the consumer
// integration test.
type recordingHandler struct {
	mu      sync.Mutex
	added   []string
	removed []string
}

func (h *recordingHandler) OnSymbolAdded(_ context.Context, symbol, _ string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.added = append(h.added, symbol)
	return nil
}

func (h *recordingHandler) OnSymbolRemoved(_ context.Context, symbol string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removed = append(h.removed, symbol)
	return nil
}

func (h *recordingHandler) snapshot() ([]string, []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	a := append([]string(nil), h.added...)
	r := append([]string(nil), h.removed...)
	return a, r
}

// TestWatchlistConsumer_Integration_RoundTrip produces watchlist events to a live
// Redpanda broker and consumes them through the migrated WatchlistConsumer
// (backed by the shared kafka.ConsumerGroup), asserting the events are dispatched
// to the SymbolHandler — verifying the consumer mapping (topic/group/OffsetOldest)
// against a real broker.
func TestWatchlistConsumer_Integration_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	rp := testkit.NewRedpandaContainer(t)
	const topic = "trading.watchlist.itest"
	rp.CreateTopic(t, topic, 1)

	// Produce two watchlist events using the shared producer (the same library
	// the consumer reads from).
	producer, err := commonskafka.NewProducer([]string{rp.Brokers})
	require.NoError(t, err)
	defer producer.Close()

	addedEvt := []byte(`{"event_type":"WATCHLIST_SYMBOL_ADDED","data":{"symbol":"nvda","name":"NVIDIA"}}`)
	removedEvt := []byte(`{"event_type":"WATCHLIST_SYMBOL_REMOVED","data":{"symbol":"tsla"}}`)
	require.NoError(t, producer.Publish(context.Background(), topic, []byte("nvda"), addedEvt))
	require.NoError(t, producer.Publish(context.Background(), topic, []byte("tsla"), removedEvt))

	h := &recordingHandler{}
	c, err := NewWatchlistConsumer([]string{rp.Brokers}, topic, "market-data-ingestion-itest", h)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()

	require.Eventually(t, func() bool {
		added, removed := h.snapshot()
		return len(added) == 1 && len(removed) == 1
	}, 30*time.Second, 250*time.Millisecond, "consumer did not dispatch both watchlist events")

	added, removed := h.snapshot()
	assert.Equal(t, []string{"NVDA"}, added)
	assert.Equal(t, []string{"TSLA"}, removed)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "Start should return nil on context cancellation")
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not shut down after context cancellation")
	}
	require.NoError(t, c.Close())
}
