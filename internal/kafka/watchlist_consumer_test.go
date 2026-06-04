package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonskafka "github.com/trogers1052/trading-go-commons/kafka"
)

type fakeHandler struct {
	added   []string
	names   []string
	removed []string
	failAdd bool
	failRem bool
}

func (h *fakeHandler) OnSymbolAdded(ctx context.Context, symbol, name string) error {
	if h.failAdd {
		return errors.New("add failed")
	}
	h.added = append(h.added, symbol)
	h.names = append(h.names, name)
	return nil
}

func (h *fakeHandler) OnSymbolRemoved(ctx context.Context, symbol string) error {
	if h.failRem {
		return errors.New("remove failed")
	}
	h.removed = append(h.removed, symbol)
	return nil
}

func consumer(h SymbolHandler) *WatchlistConsumer {
	return &WatchlistConsumer{topic: "wl", handler: h}
}

func TestProcessMessage_SymbolAdded(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	err := c.processMessage(context.Background(), []byte(`{
		"event_type":"WATCHLIST_SYMBOL_ADDED",
		"data":{"symbol":"aapl","name":"Apple Inc"}
	}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"AAPL"}, h.added)
	assert.Equal(t, []string{"Apple Inc"}, h.names)
}

func TestProcessMessage_SymbolAdded_NoName(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	err := c.processMessage(context.Background(), []byte(`{
		"event_type":"WATCHLIST_SYMBOL_ADDED",
		"data":{"symbol":"msft"}
	}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"MSFT"}, h.added)
	assert.Equal(t, []string{"MSFT"}, h.names) // falls back to symbol
}

func TestProcessMessage_SymbolAdded_HandlerError(t *testing.T) {
	h := &fakeHandler{failAdd: true}
	c := consumer(h)
	err := c.processMessage(context.Background(), []byte(`{
		"event_type":"WATCHLIST_SYMBOL_ADDED","data":{"symbol":"aapl"}
	}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to add symbol")
}

func TestProcessMessage_SymbolRemoved(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	err := c.processMessage(context.Background(), []byte(`{
		"event_type":"WATCHLIST_SYMBOL_REMOVED","data":{"symbol":"tsla"}
	}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"TSLA"}, h.removed)
}

func TestProcessMessage_SymbolRemoved_HandlerError(t *testing.T) {
	h := &fakeHandler{failRem: true}
	c := consumer(h)
	err := c.processMessage(context.Background(), []byte(`{
		"event_type":"WATCHLIST_SYMBOL_REMOVED","data":{"symbol":"tsla"}
	}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to remove symbol")
}

func TestProcessMessage_WatchlistUpdated(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	err := c.processMessage(context.Background(), []byte(`{
		"event_type":"WATCHLIST_UPDATED",
		"data":{
			"added_symbols":["aapl","nvda"],
			"removed_symbols":["tsla"],
			"stocks":[{"symbol":"nvda","name":"NVIDIA"}]
		}
	}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"AAPL", "NVDA"}, h.added)
	// AAPL not in stocks list -> name defaults to the raw (lowercase) symbol;
	// NVDA name resolved from the stocks list.
	assert.Equal(t, []string{"aapl", "NVIDIA"}, h.names)
	assert.Equal(t, []string{"TSLA"}, h.removed)
}

func TestProcessMessage_WatchlistUpdated_HandlerErrorsContinue(t *testing.T) {
	// Errors inside WATCHLIST_UPDATED are logged but not returned.
	h := &fakeHandler{failAdd: true, failRem: true}
	c := consumer(h)
	err := c.processMessage(context.Background(), []byte(`{
		"event_type":"WATCHLIST_UPDATED",
		"data":{"added_symbols":["aapl"],"removed_symbols":["tsla"]}
	}`))
	require.NoError(t, err)
}

func TestProcessMessage_UnknownEvent(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	err := c.processMessage(context.Background(), []byte(`{"event_type":"SOMETHING_ELSE"}`))
	require.NoError(t, err)
	assert.Empty(t, h.added)
}

func TestProcessMessage_BadJSON(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	err := c.processMessage(context.Background(), []byte(`{not json`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

// TestHandle_RoutesMessageValue confirms the ConsumerGroup Handler adapter routes
// a shared kafka.Message's Value into processMessage and the existing dispatch.
func TestHandle_RoutesMessageValue(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	msg := &commonskafka.Message{
		Topic: "wl",
		Value: []byte(`{"event_type":"WATCHLIST_SYMBOL_ADDED","data":{"symbol":"aapl","name":"Apple Inc"}}`),
	}
	require.NoError(t, c.handle(context.Background(), msg))
	assert.Equal(t, []string{"AAPL"}, h.added)
	assert.Equal(t, []string{"Apple Inc"}, h.names)
}

// TestHandle_BadJSONReturnsError confirms the handler returns an error on a bad
// payload. Under the shared ConsumerGroup's MarkAndContinue policy this is logged
// and committed (mirroring the old ConsumeClaim mark-on-error behaviour).
func TestHandle_BadJSONReturnsError(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	msg := &commonskafka.Message{Topic: "wl", Value: []byte(`{bad json`)}
	err := c.handle(context.Background(), msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestNewWatchlistConsumer_NoBrokers(t *testing.T) {
	// The shared consumer group rejects an empty broker list at construction.
	_, err := NewWatchlistConsumer(nil, "wl", "grp", &fakeHandler{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create consumer group")
}

func TestWatchlistConsumer_Close_NilGroup(t *testing.T) {
	c := &WatchlistConsumer{topic: "wl"}
	require.NoError(t, c.Close())
}
