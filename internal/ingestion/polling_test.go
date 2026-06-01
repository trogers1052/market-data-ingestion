package ingestion

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/marketdata"
	"github.com/trogers1052/market-data-ingestion/internal/models"
)

type stubMD struct {
	bars    []models.OHLCV
	barsErr error
	once    bool // when true, returns bars on first call only, then nil
	calls   int
}

func (m *stubMD) GetMinuteBars(ctx context.Context, symbol string, from, to time.Time) ([]models.OHLCV, error) {
	m.calls++
	if m.barsErr != nil {
		return nil, m.barsErr
	}
	if m.once && m.calls > 1 {
		return nil, nil
	}
	return m.bars, nil
}

func (m *stubMD) GetTickerDetails(ctx context.Context, symbol string) (*marketdata.TickerDetails, error) {
	return &marketdata.TickerDetails{Symbol: symbol}, nil
}

func mockRepo(t *testing.T) (*database.Repository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return database.NewRepositoryWithDB(db), mock
}

func validBar(symbol string, ts time.Time) models.OHLCV {
	return models.OHLCV{
		Time: ts, Symbol: symbol,
		Open: decimal.NewFromInt(100), High: decimal.NewFromInt(110),
		Low: decimal.NewFromInt(95), Close: decimal.NewFromInt(105), Volume: 100,
	}
}

func newPolling(md marketdata.Client, repo *database.Repository) *PollingService {
	sched := NewMarketScheduler(9, 16, false, false)
	return NewPollingService(md, repo, sched, nil, 60, 0)
}

func TestNewPollingService(t *testing.T) {
	repo, _ := mockRepo(t)
	p := newPolling(&stubMD{}, repo)
	assert.Equal(t, 60*time.Second, p.pollInterval)
	assert.NotNil(t, p.lastFetched)
}

func TestPollSymbol_NoBars(t *testing.T) {
	repo, _ := mockRepo(t)
	p := newPolling(&stubMD{bars: nil}, repo)
	fetched, inserted, err := p.pollSymbol(context.Background(), "AAPL")
	require.NoError(t, err)
	assert.Equal(t, 0, fetched)
	assert.Equal(t, 0, inserted)
}

func TestPollSymbol_FetchError(t *testing.T) {
	repo, _ := mockRepo(t)
	p := newPolling(&stubMD{barsErr: errors.New("api down")}, repo)
	_, _, err := p.pollSymbol(context.Background(), "AAPL")
	require.Error(t, err)
}

func TestPollSymbol_InsertsValidBars(t *testing.T) {
	repo, mock := mockRepo(t)
	// bar timestamp must be after windowStart; use a recent time
	bar := validBar("AAPL", time.Now().Add(-30*time.Second))
	p := newPolling(&stubMD{bars: []models.OHLCV{bar}}, repo)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	fetched, inserted, err := p.pollSymbol(context.Background(), "AAPL")
	require.NoError(t, err)
	assert.Equal(t, 1, fetched)
	assert.Equal(t, 1, inserted)
}

func TestPollSymbol_RejectsInvalidBars(t *testing.T) {
	repo, _ := mockRepo(t)
	bad := validBar("AAPL", time.Now().Add(-30*time.Second))
	bad.High = decimal.NewFromInt(1) // high < low -> invalid
	p := newPolling(&stubMD{bars: []models.OHLCV{bad}}, repo)

	fetched, inserted, err := p.pollSymbol(context.Background(), "AAPL")
	require.NoError(t, err)
	assert.Equal(t, 1, fetched)
	assert.Equal(t, 0, inserted) // all invalid -> nothing inserted, no DB call
}

func TestPollSymbol_InsertError(t *testing.T) {
	repo, mock := mockRepo(t)
	bar := validBar("AAPL", time.Now().Add(-30*time.Second))
	p := newPolling(&stubMD{bars: []models.OHLCV{bar}}, repo)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnError(errors.New("insert fail"))
	mock.ExpectRollback()

	_, _, err := p.pollSymbol(context.Background(), "AAPL")
	require.Error(t, err)
}

func TestPollSymbol_AlreadyFetched(t *testing.T) {
	repo, _ := mockRepo(t)
	p := newPolling(&stubMD{}, repo)
	// Set lastFetched to "now" so windowEnd (in the past) is not after it.
	p.lastFetched["AAPL"] = time.Now().Add(time.Hour)
	fetched, inserted, err := p.pollSymbol(context.Background(), "AAPL")
	require.NoError(t, err)
	assert.Equal(t, 0, fetched)
	assert.Equal(t, 0, inserted)
}

func TestPollSymbol_DuplicateBarsFiltered(t *testing.T) {
	repo, _ := mockRepo(t)
	old := time.Now().Add(-90 * time.Second)
	bar := validBar("AAPL", old)
	p := newPolling(&stubMD{bars: []models.OHLCV{bar}}, repo)
	// last fetched is after this bar -> bar filtered out as duplicate
	p.lastFetched["AAPL"] = old.Add(time.Second)

	fetched, inserted, err := p.pollSymbol(context.Background(), "AAPL")
	require.NoError(t, err)
	assert.Equal(t, 1, fetched)
	assert.Equal(t, 0, inserted)
}

func TestPollAllSymbols(t *testing.T) {
	repo, _ := mockRepo(t)
	p := newPolling(&stubMD{bars: nil}, repo)
	p.pollAllSymbols(context.Background(), []string{"AAPL", "MSFT"})
	stats := p.GetStats()
	assert.Equal(t, int64(1), stats["poll_count"])
}

func TestPollAllSymbols_ContextCancelled(t *testing.T) {
	repo, _ := mockRepo(t)
	p := newPolling(&stubMD{}, repo)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.pollAllSymbols(ctx, []string{"AAPL"}) // returns early
}

func TestGetStats(t *testing.T) {
	repo, _ := mockRepo(t)
	p := newPolling(&stubMD{}, repo)
	p.barsReceived = 10
	p.barsInserted = 8
	p.pollCount = 3
	stats := p.GetStats()
	assert.Equal(t, int64(10), stats["bars_received"])
	assert.Equal(t, int64(8), stats["bars_inserted"])
	assert.Equal(t, int64(3), stats["poll_count"])
}

func TestUpdateSymbols(t *testing.T) {
	repo, _ := mockRepo(t)
	p := newPolling(&stubMD{}, repo)
	p.lastFetched["AAPL"] = time.Now()
	p.lastFetched["OLD"] = time.Now()
	require.NoError(t, p.UpdateSymbols([]string{"AAPL"}))
	_, hasAAPL := p.lastFetched["AAPL"]
	_, hasOld := p.lastFetched["OLD"]
	assert.True(t, hasAAPL)
	assert.False(t, hasOld) // removed symbol cleared
}

func TestStopAndLogStats(t *testing.T) {
	repo, _ := mockRepo(t)
	p := newPolling(&stubMD{}, repo)
	p.Stop() // logs stats, no panic
}

func TestPollingStart_NoSymbols(t *testing.T) {
	repo, mock := mockRepo(t)
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(sqlmock.NewRows(cols))
	p := newPolling(&stubMD{}, repo)
	err := p.Start(context.Background())
	require.NoError(t, err)
}

func TestPollingStart_SymbolsError(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnError(errors.New("db"))
	p := newPolling(&stubMD{}, repo)
	err := p.Start(context.Background())
	require.Error(t, err)
}

func TestPollingStart_ContextCancelAfterInitialPoll(t *testing.T) {
	repo, mock := mockRepo(t)
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	mock.ExpectQuery("FROM monitored_symbols").
		WillReturnRows(sqlmock.NewRows(cols).AddRow("AAPL", "Apple", true, time.Now(), time.Now()))

	p := newPolling(&stubMD{bars: nil}, repo)
	// Short timeout: the initial GetMonitoredSymbols + initial poll succeed,
	// then the main loop blocks on the ticker until ctx expires and returns nil.
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	err := p.Start(ctx)
	require.NoError(t, err)
}
