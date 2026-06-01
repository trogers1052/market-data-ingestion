package status

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/ingestion"
	"github.com/trogers1052/market-data-ingestion/internal/models"
	redisclient "github.com/trogers1052/market-data-ingestion/internal/redis"
)

type fakeStats struct{ m map[string]int64 }

func (f fakeStats) GetStats() map[string]int64 { return f.m }

func deps(t *testing.T) (*database.Repository, sqlmock.Sqlmock, *redisclient.Client, *ingestion.MarketScheduler) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	// Market hours that will be closed (open=close) so IsMarketHours is false,
	// making status determination deterministic regardless of wall clock.
	sched := ingestion.NewMarketScheduler(0, 0, false, false)
	return database.NewRepositoryWithDB(db), mock, redisclient.NewClientWithRedis(rc), sched
}

func TestNewManager(t *testing.T) {
	repo, _, rc, sched := deps(t)
	m := NewManager(repo, rc, sched, fakeStats{})
	assert.NotNil(t, m)
	assert.Equal(t, 30*time.Second, m.updateInterval)
}

func TestDetermineSymbolStatus(t *testing.T) {
	repo, _, rc, sched := deps(t)
	m := NewManager(repo, rc, sched, nil)

	tests := []struct {
		name   string
		bs     *models.BackfillStatus
		latest time.Time
		count  int64
		want   string
	}{
		{"nil status", nil, time.Time{}, 0, "no_data"},
		{"empty status", &models.BackfillStatus{Status: ""}, time.Time{}, 0, "no_data"},
		{"in progress", &models.BackfillStatus{Status: models.BackfillStatusInProgress}, time.Time{}, 0, "backfilling"},
		{"failed", &models.BackfillStatus{Status: "failed"}, time.Time{}, 0, "error"},
		{"completed no bars", &models.BackfillStatus{Status: models.BackfillStatusCompleted}, time.Now(), 0, "no_data"},
		{"completed with bars, market closed", &models.BackfillStatus{Status: models.BackfillStatusCompleted}, time.Now(), 100, "current"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, m.determineSymbolStatus(tt.bs, tt.latest, tt.count))
		})
	}
}

func TestIsDataReady(t *testing.T) {
	repo, _, rc, sched := deps(t)
	m := NewManager(repo, rc, sched, nil)

	completed := &models.BackfillStatus{Status: models.BackfillStatusCompleted}

	assert.False(t, m.isDataReady(nil, 300, 0))
	assert.False(t, m.isDataReady(&models.BackfillStatus{Status: "pending"}, 300, 0))
	assert.False(t, m.isDataReady(completed, 100, 0)) // not enough bars
	assert.True(t, m.isDataReady(completed, 200, 0))
	assert.True(t, m.isDataReady(completed, 500, 5))
}

func TestHelperFormatters(t *testing.T) {
	assert.Equal(t, "", formatTime(time.Time{}))
	tm := time.Date(2026, 1, 2, 15, 0, 0, 0, time.UTC)
	assert.Equal(t, "2026-01-02T15:00:00Z", formatTime(tm))

	assert.Equal(t, "", formatTimePtr(nil))
	zero := time.Time{}
	assert.Equal(t, "", formatTimePtr(&zero))
	assert.Equal(t, "2026-01-02T15:00:00Z", formatTimePtr(&tm))

	assert.Equal(t, "pending", getBackfillStatusString(nil))
	assert.Equal(t, "pending", getBackfillStatusString(&models.BackfillStatus{Status: ""}))
	assert.Equal(t, "completed", getBackfillStatusString(&models.BackfillStatus{Status: "completed"}))
}

func monitoredRows(symbols ...string) *sqlmock.Rows {
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	rows := sqlmock.NewRows(cols)
	for _, s := range symbols {
		rows.AddRow(s, s, true, time.Now(), time.Now())
	}
	return rows
}

func TestUpdateIngestionStatus(t *testing.T) {
	repo, mock, rc, sched := deps(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(monitoredRows("AAPL", "MSFT"))
	mock.ExpectQuery("LEFT JOIN backfill_status").
		WillReturnRows(sqlmock.NewRows([]string{"symbol"}).AddRow("AAPL"))

	m := NewManager(repo, rc, sched, fakeStats{m: map[string]int64{"bars_received": 10, "bars_inserted": 8}})
	m.updateIngestionStatus(context.Background())

	got, err := rc.GetIngestionStatus(context.Background())
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "running", got.Status)
	assert.Equal(t, 2, got.SymbolsTracked)
	assert.Equal(t, int64(10), got.BarsReceived)
	assert.Equal(t, int64(8), got.BarsInserted)
	assert.Equal(t, 1, got.BackfillPending)
}

func TestUpdateIngestionStatus_NilRedis(t *testing.T) {
	repo, _, _, sched := deps(t)
	m := NewManager(repo, nil, sched, fakeStats{})
	// Should return early without panicking or hitting the DB.
	m.updateIngestionStatus(context.Background())
}

func TestUpdateSingleSymbolFreshness(t *testing.T) {
	repo, mock, rc, sched := deps(t)

	// GetBackfillStatus
	bsCols := []string{"symbol", "last_backfill", "backfill_start", "backfill_end", "status", "error_message", "created_at", "updated_at"}
	now := time.Now()
	mock.ExpectQuery("FROM backfill_status").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows(bsCols).
			AddRow("AAPL", now, now, now, "completed", nil, now, now))
	// GetBarCount
	mock.ExpectQuery("COUNT").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(250)))
	// GetLatestBar
	barCols := []string{"time", "symbol", "open", "high", "low", "close", "volume", "vwap", "trade_count"}
	mock.ExpectQuery("ORDER BY time DESC").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows(barCols).
			AddRow(now, "AAPL", "100", "110", "95", "105", int64(1000), "102", int32(10)))

	m := NewManager(repo, rc, sched, nil)
	m.updateSingleSymbolFreshness(context.Background(), "AAPL")

	fresh, err := rc.GetSymbolFreshness(context.Background(), "AAPL")
	require.NoError(t, err)
	require.NotNil(t, fresh)
	assert.Equal(t, "AAPL", fresh.Symbol)
	assert.Equal(t, int64(250), fresh.BarCount)
	assert.Equal(t, "completed", fresh.BackfillStatus)
}

func TestUpdateSymbolFreshness_NilRedis(t *testing.T) {
	repo, _, _, sched := deps(t)
	m := NewManager(repo, nil, sched, nil)
	m.updateSymbolFreshness(context.Background()) // early return
}

func TestUpdateSymbolFreshness_MonitoredError(t *testing.T) {
	repo, mock, rc, sched := deps(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnError(errors.New("db"))
	m := NewManager(repo, rc, sched, nil)
	m.updateSymbolFreshness(context.Background()) // logs warning, returns
}

func TestUpdateSingleSymbolFreshness_BackfillStatusError(t *testing.T) {
	repo, mock, rc, sched := deps(t)
	// GetBackfillStatus errors -> falls back to a pending status.
	mock.ExpectQuery("FROM backfill_status").WithArgs("AAPL").
		WillReturnError(errors.New("db"))
	// GetBarCount errors -> count defaults to 0.
	mock.ExpectQuery("COUNT").WithArgs("AAPL").WillReturnError(errors.New("db"))
	// GetLatestBar errors -> latestBarTime stays zero.
	mock.ExpectQuery("ORDER BY time DESC").WithArgs("AAPL").WillReturnError(errors.New("db"))

	m := NewManager(repo, rc, sched, nil)
	m.updateSingleSymbolFreshness(context.Background(), "AAPL")

	fresh, err := rc.GetSymbolFreshness(context.Background(), "AAPL")
	require.NoError(t, err)
	require.NotNil(t, fresh)
	assert.Equal(t, "pending", fresh.BackfillStatus)
	assert.Equal(t, int64(0), fresh.BarCount)
}

func TestStartStop(t *testing.T) {
	repo, mock, rc, sched := deps(t)
	// The initial updateAllStatus call will query symbols (status + freshness).
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(monitoredRows())
	mock.ExpectQuery("LEFT JOIN backfill_status").WillReturnRows(sqlmock.NewRows([]string{"symbol"}))
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(monitoredRows())

	m := NewManager(repo, rc, sched, fakeStats{})
	m.updateInterval = time.Hour // avoid extra ticks
	ctx := context.Background()
	m.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	m.Stop()
}
