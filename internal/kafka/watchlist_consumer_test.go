package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	return &WatchlistConsumer{topic: "wl", handler: h, ready: make(chan bool)}
}

func msg(value string) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{Value: []byte(value)}
}

func TestProcessMessage_SymbolAdded(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	err := c.processMessage(context.Background(), msg(`{
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
	err := c.processMessage(context.Background(), msg(`{
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
	err := c.processMessage(context.Background(), msg(`{
		"event_type":"WATCHLIST_SYMBOL_ADDED","data":{"symbol":"aapl"}
	}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to add symbol")
}

func TestProcessMessage_SymbolRemoved(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	err := c.processMessage(context.Background(), msg(`{
		"event_type":"WATCHLIST_SYMBOL_REMOVED","data":{"symbol":"tsla"}
	}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"TSLA"}, h.removed)
}

func TestProcessMessage_SymbolRemoved_HandlerError(t *testing.T) {
	h := &fakeHandler{failRem: true}
	c := consumer(h)
	err := c.processMessage(context.Background(), msg(`{
		"event_type":"WATCHLIST_SYMBOL_REMOVED","data":{"symbol":"tsla"}
	}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to remove symbol")
}

func TestProcessMessage_WatchlistUpdated(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	err := c.processMessage(context.Background(), msg(`{
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
	err := c.processMessage(context.Background(), msg(`{
		"event_type":"WATCHLIST_UPDATED",
		"data":{"added_symbols":["aapl"],"removed_symbols":["tsla"]}
	}`))
	require.NoError(t, err)
}

func TestProcessMessage_UnknownEvent(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	err := c.processMessage(context.Background(), msg(`{"event_type":"SOMETHING_ELSE"}`))
	require.NoError(t, err)
	assert.Empty(t, h.added)
}

// --- fakes for ConsumerGroupSession / ConsumerGroupClaim ---

type fakeSession struct {
	ctx    context.Context
	marked int
}

func (f *fakeSession) Claims() map[string][]int32                        { return nil }
func (f *fakeSession) MemberID() string                                  { return "m" }
func (f *fakeSession) GenerationID() int32                               { return 1 }
func (f *fakeSession) MarkOffset(string, int32, int64, string)           {}
func (f *fakeSession) Commit()                                           {}
func (f *fakeSession) ResetOffset(string, int32, int64, string)          {}
func (f *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) { f.marked++ }
func (f *fakeSession) Context() context.Context                          { return f.ctx }

type fakeClaim struct {
	ch chan *sarama.ConsumerMessage
}

func (f *fakeClaim) Topic() string                            { return "wl" }
func (f *fakeClaim) Partition() int32                         { return 0 }
func (f *fakeClaim) InitialOffset() int64                     { return 0 }
func (f *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (f *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return f.ch }

func TestSetupClosesReady(t *testing.T) {
	c := consumer(&fakeHandler{})
	require.NoError(t, c.Setup(nil))
	// ready channel should now be closed
	select {
	case <-c.ready:
	default:
		t.Fatal("ready channel was not closed by Setup")
	}
}

func TestCleanup(t *testing.T) {
	c := consumer(&fakeHandler{})
	require.NoError(t, c.Cleanup(nil))
}

func TestConsumeClaim_ProcessesAndMarks(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	ch := make(chan *sarama.ConsumerMessage, 2)
	ch <- msg(`{"event_type":"WATCHLIST_SYMBOL_ADDED","data":{"symbol":"aapl"}}`)
	ch <- msg(`{bad json`) // processed (error logged) and still marked
	close(ch)

	sess := &fakeSession{ctx: context.Background()}
	err := c.ConsumeClaim(sess, &fakeClaim{ch: ch})
	require.NoError(t, err)
	assert.Equal(t, []string{"AAPL"}, h.added)
	assert.Equal(t, 2, sess.marked)
}

func TestConsumeClaim_ContextDone(t *testing.T) {
	c := consumer(&fakeHandler{})
	ch := make(chan *sarama.ConsumerMessage) // never sends
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sess := &fakeSession{ctx: ctx}
	err := c.ConsumeClaim(sess, &fakeClaim{ch: ch})
	require.NoError(t, err)
}

func TestNewWatchlistConsumer_ConnectError(t *testing.T) {
	// No Kafka broker listening -> consumer group creation fails.
	_, err := NewWatchlistConsumer([]string{"127.0.0.1:1"}, "wl", "grp", &fakeHandler{})
	require.Error(t, err)
}

func TestProcessMessage_BadJSON(t *testing.T) {
	h := &fakeHandler{}
	c := consumer(h)
	err := c.processMessage(context.Background(), msg(`{not json`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}
