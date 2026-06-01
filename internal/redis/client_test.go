package redis

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestClient(t *testing.T) (*Client, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })
	return &Client{client: rc}, mr
}

func ctx() context.Context { return context.Background() }

func TestNewClient_ConnectionError(t *testing.T) {
	// Nothing listening on this port -> Ping fails
	_, err := NewClient("127.0.0.1:1", "", 0)
	require.Error(t, err)
}

func TestNewClient_Success(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	c, err := NewClient(mr.Addr(), "", 0)
	require.NoError(t, err)
	require.NoError(t, c.Close())
}

func TestWatchlistSymbols(t *testing.T) {
	c, mr := newTestClient(t)
	mr.SAdd(WatchlistKey, "AAPL", "MSFT")

	syms, err := c.GetWatchlistSymbols(ctx())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"AAPL", "MSFT"}, syms)

	count, err := c.WatchlistCount(ctx())
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	exists, err := c.SymbolExists(ctx(), "AAPL")
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = c.SymbolExists(ctx(), "TSLA")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestWatchlistDetails(t *testing.T) {
	c, mr := newTestClient(t)
	stock := WatchlistStock{Symbol: "AAPL", Name: "Apple Inc"}
	data, _ := json.Marshal(stock)
	mr.HSet(WatchlistDetailsKey, "AAPL", string(data))
	mr.HSet(WatchlistDetailsKey, "BAD", "{not json") // skipped with warning

	details, err := c.GetWatchlistDetails(ctx())
	require.NoError(t, err)
	require.Contains(t, details, "AAPL")
	assert.Equal(t, "Apple Inc", details["AAPL"].Name)
	assert.NotContains(t, details, "BAD")
}

func TestGetStockDetails(t *testing.T) {
	c, mr := newTestClient(t)
	stock := WatchlistStock{Symbol: "AAPL", Name: "Apple"}
	data, _ := json.Marshal(stock)
	mr.HSet(WatchlistDetailsKey, "AAPL", string(data))

	got, err := c.GetStockDetails(ctx(), "AAPL")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "Apple", got.Name)

	// Missing symbol -> nil, no error
	got, err = c.GetStockDetails(ctx(), "MISSING")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Bad JSON -> error
	mr.HSet(WatchlistDetailsKey, "BAD", "{not json")
	_, err = c.GetStockDetails(ctx(), "BAD")
	require.Error(t, err)
}

func TestIngestionStatusRoundTrip(t *testing.T) {
	c, _ := newTestClient(t)

	// No status set -> nil
	got, err := c.GetIngestionStatus(ctx())
	require.NoError(t, err)
	assert.Nil(t, got)

	status := &IngestionStatus{Status: "running", SymbolsTracked: 5, BarsReceived: 100}
	require.NoError(t, c.UpdateIngestionStatus(ctx(), status))

	got, err = c.GetIngestionStatus(ctx())
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "running", got.Status)
	assert.Equal(t, 5, got.SymbolsTracked)
}

func TestGetIngestionStatus_BadJSON(t *testing.T) {
	c, mr := newTestClient(t)
	mr.Set(IngestionStatusKey, "{not json")
	_, err := c.GetIngestionStatus(ctx())
	require.Error(t, err)
}

func TestSymbolFreshnessRoundTrip(t *testing.T) {
	c, _ := newTestClient(t)

	got, err := c.GetSymbolFreshness(ctx(), "AAPL")
	require.NoError(t, err)
	assert.Nil(t, got)

	fresh := &SymbolFreshness{Symbol: "AAPL", Status: "current", IsReady: true, BarCount: 300}
	require.NoError(t, c.UpdateSymbolFreshness(ctx(), fresh))

	got, err = c.GetSymbolFreshness(ctx(), "AAPL")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "current", got.Status)
	assert.True(t, got.IsReady)
}

func TestGetSymbolFreshness_BadJSON(t *testing.T) {
	c, mr := newTestClient(t)
	mr.Set(SymbolFreshnessKeyPrefix+"AAPL:freshness", "{bad")
	_, err := c.GetSymbolFreshness(ctx(), "AAPL")
	require.Error(t, err)
}

func TestGetAllSymbolFreshness(t *testing.T) {
	c, mr := newTestClient(t)
	require.NoError(t, c.UpdateSymbolFreshness(ctx(), &SymbolFreshness{Symbol: "AAPL", Status: "current"}))
	require.NoError(t, c.UpdateSymbolFreshness(ctx(), &SymbolFreshness{Symbol: "MSFT", Status: "stale"}))
	// add an unparseable entry; should be skipped
	mr.Set(SymbolFreshnessKeyPrefix+"BAD:freshness", "{bad")

	all, err := c.GetAllSymbolFreshness(ctx())
	require.NoError(t, err)
	assert.Len(t, all, 2)
	assert.Contains(t, all, "AAPL")
	assert.Contains(t, all, "MSFT")
}

func TestIsSymbolDataReady(t *testing.T) {
	c, _ := newTestClient(t)

	// No freshness data
	ready, reason, err := c.IsSymbolDataReady(ctx(), "AAPL")
	require.NoError(t, err)
	assert.False(t, ready)
	assert.Contains(t, reason, "no freshness data")

	// Not ready
	require.NoError(t, c.UpdateSymbolFreshness(ctx(), &SymbolFreshness{
		Symbol: "AAPL", Status: "stale", BackfillStatus: "pending", IsReady: false}))
	ready, reason, err = c.IsSymbolDataReady(ctx(), "AAPL")
	require.NoError(t, err)
	assert.False(t, ready)
	assert.Contains(t, reason, "data not ready")

	// Ready
	require.NoError(t, c.UpdateSymbolFreshness(ctx(), &SymbolFreshness{
		Symbol: "AAPL", Status: "current", BackfillStatus: "completed", IsReady: true}))
	ready, reason, err = c.IsSymbolDataReady(ctx(), "AAPL")
	require.NoError(t, err)
	assert.True(t, ready)
	assert.Empty(t, reason)
}

func TestBackfillCompletionLifecycle(t *testing.T) {
	c, _ := newTestClient(t)

	// Not complete initially
	done, err := c.IsBackfillComplete(ctx(), "AAPL")
	require.NoError(t, err)
	assert.False(t, done)

	completion := &BackfillCompletion{Symbol: "AAPL", TotalBars: 1000, Months: 60}
	require.NoError(t, c.MarkBackfillComplete(ctx(), completion))

	done, err = c.IsBackfillComplete(ctx(), "AAPL")
	require.NoError(t, err)
	assert.True(t, done)

	got, err := c.GetBackfillCompletion(ctx(), "AAPL")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(1000), got.TotalBars)

	// Not-completed symbol
	got, err = c.GetBackfillCompletion(ctx(), "MISSING")
	require.NoError(t, err)
	assert.Nil(t, got)

	syms, err := c.GetCompletedBackfills(ctx())
	require.NoError(t, err)
	assert.Equal(t, []string{"AAPL"}, syms)

	all, err := c.GetAllBackfillCompletions(ctx())
	require.NoError(t, err)
	assert.Len(t, all, 1)
	assert.Contains(t, all, "AAPL")

	// Clear single
	require.NoError(t, c.ClearBackfillComplete(ctx(), "AAPL"))
	done, err = c.IsBackfillComplete(ctx(), "AAPL")
	require.NoError(t, err)
	assert.False(t, done)
}

func TestGetBackfillCompletion_BadJSON(t *testing.T) {
	c, mr := newTestClient(t)
	mr.Set(BackfillCompletedKeyPrefix+"AAPL", "{bad")
	_, err := c.GetBackfillCompletion(ctx(), "AAPL")
	require.Error(t, err)
}

// closedClient returns a Client whose underlying connection is already closed,
// so every Redis command returns an error. This exercises the error-handling
// branch of each method.
func closedClient(t *testing.T) *Client {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	c := NewClientWithRedis(rc)
	require.NoError(t, rc.Close())
	mr.Close()
	return c
}

func TestErrorPaths(t *testing.T) {
	c := closedClient(t)
	ctx := ctx()

	_, err := c.GetWatchlistSymbols(ctx)
	require.Error(t, err)
	_, err = c.GetWatchlistDetails(ctx)
	require.Error(t, err)
	_, err = c.GetStockDetails(ctx, "AAPL")
	require.Error(t, err)
	_, err = c.SymbolExists(ctx, "AAPL")
	require.Error(t, err)
	_, err = c.WatchlistCount(ctx)
	require.Error(t, err)
	require.Error(t, c.UpdateIngestionStatus(ctx, &IngestionStatus{}))
	_, err = c.GetIngestionStatus(ctx)
	require.Error(t, err)
	require.Error(t, c.UpdateSymbolFreshness(ctx, &SymbolFreshness{Symbol: "AAPL"}))
	_, err = c.GetSymbolFreshness(ctx, "AAPL")
	require.Error(t, err)
	_, err = c.GetAllSymbolFreshness(ctx)
	require.Error(t, err)
	_, _, err = c.IsSymbolDataReady(ctx, "AAPL")
	require.Error(t, err)
	require.Error(t, c.MarkBackfillComplete(ctx, &BackfillCompletion{Symbol: "AAPL"}))
	_, err = c.IsBackfillComplete(ctx, "AAPL")
	require.Error(t, err)
	_, err = c.GetBackfillCompletion(ctx, "AAPL")
	require.Error(t, err)
	_, err = c.GetCompletedBackfills(ctx)
	require.Error(t, err)
	_, err = c.GetAllBackfillCompletions(ctx)
	require.Error(t, err)
	require.Error(t, c.ClearBackfillComplete(ctx, "AAPL"))
	require.Error(t, c.ClearAllBackfillComplete(ctx))
}

func TestClearAllBackfillComplete(t *testing.T) {
	c, _ := newTestClient(t)
	require.NoError(t, c.MarkBackfillComplete(ctx(), &BackfillCompletion{Symbol: "AAPL"}))
	require.NoError(t, c.MarkBackfillComplete(ctx(), &BackfillCompletion{Symbol: "MSFT"}))

	require.NoError(t, c.ClearAllBackfillComplete(ctx()))

	syms, err := c.GetCompletedBackfills(ctx())
	require.NoError(t, err)
	assert.Empty(t, syms)
}
