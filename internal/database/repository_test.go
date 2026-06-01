package database

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/market-data-ingestion/internal/models"
)

func newMockRepo(t *testing.T) (*Repository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return &Repository{db: db}, mock
}

func bar(symbol string) models.OHLCV {
	return models.OHLCV{
		Time:       time.Date(2026, 1, 2, 15, 0, 0, 0, time.UTC),
		Symbol:     symbol,
		Open:       decimal.NewFromInt(100),
		High:       decimal.NewFromInt(110),
		Low:        decimal.NewFromInt(95),
		Close:      decimal.NewFromInt(105),
		Volume:     1000,
		VWAP:       decimal.NewFromInt(102),
		TradeCount: 50,
	}
}

func TestClose(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectClose()
	require.NoError(t, repo.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertOHLCV(t *testing.T) {
	repo, mock := newMockRepo(t)
	b := bar("AAPL")

	mock.ExpectExec("INSERT INTO ohlcv_1min").
		WithArgs(b.Time, b.Symbol, b.Open, b.High, b.Low, b.Close, b.Volume, b.VWAP, b.TradeCount).
		WillReturnResult(sqlmock.NewResult(1, 1))

	require.NoError(t, repo.InsertOHLCV(context.Background(), &b))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertOHLCV_Error(t *testing.T) {
	repo, mock := newMockRepo(t)
	b := bar("AAPL")
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnError(errors.New("boom"))
	err := repo.InsertOHLCV(context.Background(), &b)
	require.Error(t, err)
}

func TestInsertOHLCVBatch_Empty(t *testing.T) {
	repo, _ := newMockRepo(t)
	require.NoError(t, repo.InsertOHLCVBatch(context.Background(), nil))
}

func TestInsertOHLCVBatch(t *testing.T) {
	repo, mock := newMockRepo(t)
	bars := []models.OHLCV{bar("AAPL"), bar("MSFT")}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnResult(sqlmock.NewResult(1, 2))
	mock.ExpectCommit()

	require.NoError(t, repo.InsertOHLCVBatch(context.Background(), bars))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertOHLCVBatch_BeginError(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectBegin().WillReturnError(errors.New("no tx"))
	err := repo.InsertOHLCVBatch(context.Background(), []models.OHLCV{bar("AAPL")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin transaction")
}

func TestInsertOHLCVBatch_ExecError(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnError(errors.New("exec fail"))
	mock.ExpectRollback()
	err := repo.InsertOHLCVBatch(context.Background(), []models.OHLCV{bar("AAPL")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insert batch")
}

func TestGetOHLCV(t *testing.T) {
	repo, mock := newMockRepo(t)
	cols := []string{"time", "symbol", "open", "high", "low", "close", "volume", "vwap", "trade_count"}
	rows := sqlmock.NewRows(cols).
		AddRow(time.Now(), "AAPL", "100", "110", "95", "105", int64(1000), "102.5", int32(50)).
		AddRow(time.Now(), "AAPL", "101", "111", "96", "106", int64(1100), nil, nil)

	mock.ExpectQuery("FROM ohlcv_1min").WithArgs("AAPL", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(rows)

	bars, err := repo.GetOHLCV(context.Background(), "AAPL", time.Now(), time.Now(), models.Timeframe1Min)
	require.NoError(t, err)
	require.Len(t, bars, 2)
	assert.Equal(t, 50, bars[0].TradeCount)
	assert.True(t, bars[0].VWAP.Equal(decimal.NewFromFloat(102.5)))
}

func TestGetOHLCV_TimeframeTables(t *testing.T) {
	for _, tf := range []models.Timeframe{models.Timeframe5Min, models.Timeframe1Hour, models.Timeframe1Day} {
		repo, mock := newMockRepo(t)
		cols := []string{"time", "symbol", "open", "high", "low", "close", "volume", "vwap", "trade_count"}
		mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(cols))
		_, err := repo.GetOHLCV(context.Background(), "AAPL", time.Now(), time.Now(), tf)
		require.NoError(t, err)
	}
}

func TestGetOHLCV_QueryError(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("FROM ohlcv_1min").WillReturnError(errors.New("query fail"))
	_, err := repo.GetOHLCV(context.Background(), "AAPL", time.Now(), time.Now(), models.Timeframe1Min)
	require.Error(t, err)
}

func TestGetLatestBar(t *testing.T) {
	repo, mock := newMockRepo(t)
	cols := []string{"time", "symbol", "open", "high", "low", "close", "volume", "vwap", "trade_count"}
	mock.ExpectQuery("ORDER BY time DESC").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow(time.Now(), "AAPL", "100", "110", "95", "105", int64(1000), "102.5", int32(50)))

	b, err := repo.GetLatestBar(context.Background(), "AAPL")
	require.NoError(t, err)
	require.NotNil(t, b)
	assert.Equal(t, "AAPL", b.Symbol)
}

func TestGetLatestBar_NoRows(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("ORDER BY time DESC").WithArgs("AAPL").WillReturnError(sql.ErrNoRows)
	b, err := repo.GetLatestBar(context.Background(), "AAPL")
	require.NoError(t, err)
	assert.Nil(t, b)
}

func TestGetLatestBar_Error(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("ORDER BY time DESC").WillReturnError(errors.New("db down"))
	_, err := repo.GetLatestBar(context.Background(), "AAPL")
	require.Error(t, err)
}

func TestGetDataRange(t *testing.T) {
	repo, mock := newMockRepo(t)
	now := time.Now()
	mock.ExpectQuery("SELECT MIN").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(now, now.Add(time.Hour)))
	mn, mx, err := repo.GetDataRange(context.Background(), "AAPL")
	require.NoError(t, err)
	require.NotNil(t, mn)
	require.NotNil(t, mx)
}

func TestGetDataRange_NullEmpty(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("SELECT MIN").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mn, mx, err := repo.GetDataRange(context.Background(), "AAPL")
	require.NoError(t, err)
	assert.Nil(t, mn)
	assert.Nil(t, mx)
}

func TestGetDataRange_Error(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("SELECT MIN").WillReturnError(errors.New("fail"))
	_, _, err := repo.GetDataRange(context.Background(), "AAPL")
	require.Error(t, err)
}

func TestGetMonitoredSymbols(t *testing.T) {
	repo, mock := newMockRepo(t)
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	mock.ExpectQuery("FROM monitored_symbols").
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow("AAPL", "Apple", true, time.Now(), time.Now()).
			AddRow("MSFT", nil, true, time.Now(), time.Now()))
	syms, err := repo.GetMonitoredSymbols(context.Background())
	require.NoError(t, err)
	require.Len(t, syms, 2)
	assert.Equal(t, "Apple", syms[0].Name)
	assert.Equal(t, "", syms[1].Name)
}

func TestGetMonitoredSymbols_Error(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnError(errors.New("fail"))
	_, err := repo.GetMonitoredSymbols(context.Background())
	require.Error(t, err)
}

func TestUpsertMonitoredSymbol(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").
		WithArgs("AAPL", "Apple", true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t, repo.UpsertMonitoredSymbol(context.Background(), "AAPL", "Apple", true))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetBackfillStatus(t *testing.T) {
	repo, mock := newMockRepo(t)
	cols := []string{"symbol", "last_backfill", "backfill_start", "backfill_end", "status", "error_message", "created_at", "updated_at"}
	now := time.Now()
	mock.ExpectQuery("FROM backfill_status").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow("AAPL", now, now, now, "completed", "some err", now, now))
	bs, err := repo.GetBackfillStatus(context.Background(), "AAPL")
	require.NoError(t, err)
	require.NotNil(t, bs)
	assert.Equal(t, "completed", bs.Status)
	assert.Equal(t, "some err", bs.ErrorMessage)
	assert.NotNil(t, bs.LastBackfill)
	assert.NotNil(t, bs.BackfillStart)
	assert.NotNil(t, bs.BackfillEnd)
}

func TestGetBackfillStatus_NullsAndNoRows(t *testing.T) {
	repo, mock := newMockRepo(t)
	cols := []string{"symbol", "last_backfill", "backfill_start", "backfill_end", "status", "error_message", "created_at", "updated_at"}
	mock.ExpectQuery("FROM backfill_status").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow("AAPL", nil, nil, nil, "pending", nil, time.Now(), time.Now()))
	bs, err := repo.GetBackfillStatus(context.Background(), "AAPL")
	require.NoError(t, err)
	require.NotNil(t, bs)
	assert.Nil(t, bs.LastBackfill)
	assert.Equal(t, "", bs.ErrorMessage)

	repo2, mock2 := newMockRepo(t)
	mock2.ExpectQuery("FROM backfill_status").WithArgs("MSFT").WillReturnError(sql.ErrNoRows)
	bs2, err := repo2.GetBackfillStatus(context.Background(), "MSFT")
	require.NoError(t, err)
	assert.Nil(t, bs2)
}

func TestGetBackfillStatus_Error(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("FROM backfill_status").WillReturnError(errors.New("fail"))
	_, err := repo.GetBackfillStatus(context.Background(), "AAPL")
	require.Error(t, err)
}

func TestUpdateBackfillStatus(t *testing.T) {
	repo, mock := newMockRepo(t)
	now := time.Now()
	mock.ExpectExec("INSERT INTO backfill_status").
		WithArgs("AAPL", "completed", &now, &now, "").
		WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t, repo.UpdateBackfillStatus(context.Background(), "AAPL", "completed", &now, &now, ""))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetSymbolsNeedingBackfill(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("LEFT JOIN backfill_status").
		WillReturnRows(sqlmock.NewRows([]string{"symbol"}).AddRow("AAPL").AddRow("MSFT"))
	syms, err := repo.GetSymbolsNeedingBackfill(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"AAPL", "MSFT"}, syms)
}

func TestGetSymbolsNeedingBackfill_Error(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("LEFT JOIN backfill_status").WillReturnError(errors.New("fail"))
	_, err := repo.GetSymbolsNeedingBackfill(context.Background())
	require.Error(t, err)
}

func TestGetBarCount(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("COUNT").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(42)))
	c, err := repo.GetBarCount(context.Background(), "AAPL")
	require.NoError(t, err)
	assert.Equal(t, int64(42), c)
}

func TestGetBarCountExact(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("COUNT").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(123)))
	c, err := repo.GetBarCountExact(context.Background(), "AAPL")
	require.NoError(t, err)
	assert.Equal(t, int64(123), c)
}

func TestRefreshContinuousAggregates(t *testing.T) {
	repo, mock := newMockRepo(t)
	// Three aggregates; one fails (logged, not fatal)
	mock.ExpectExec("refresh_continuous_aggregate").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("refresh_continuous_aggregate").WillReturnError(errors.New("oops"))
	mock.ExpectExec("refresh_continuous_aggregate").WillReturnResult(sqlmock.NewResult(0, 0))
	err := repo.RefreshContinuousAggregates(context.Background(), time.Now(), time.Now())
	require.NoError(t, err)
}

func TestQueryGaps(t *testing.T) {
	repo, mock := newMockRepo(t)
	now := time.Now()
	mock.ExpectQuery("SELECT").WithArgs("AAPL", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"time", "next_time"}).
			AddRow(now, now.Add(10*time.Minute)))
	gaps, err := repo.QueryGaps(context.Background(), "SELECT 1", "AAPL", time.Now(), time.Now())
	require.NoError(t, err)
	require.Len(t, gaps, 1)
	assert.Equal(t, "AAPL", gaps[0].Symbol)
}

func TestQueryGaps_Error(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("SELECT").WillReturnError(errors.New("fail"))
	_, err := repo.QueryGaps(context.Background(), "SELECT 1", "AAPL", time.Now(), time.Now())
	require.Error(t, err)
}

func TestCountZeroVolumeBars(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("volume = 0").WithArgs("AAPL", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	c, err := repo.CountZeroVolumeBars(context.Background(), "AAPL", time.Now(), time.Now())
	require.NoError(t, err)
	assert.Equal(t, 3, c)
}

func TestCountPriceSpikes(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("price_changes").WithArgs("AAPL", sqlmock.AnyArg(), sqlmock.AnyArg(), 0.10).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	c, err := repo.CountPriceSpikes(context.Background(), "AAPL", time.Now(), time.Now(), 0.10)
	require.NoError(t, err)
	assert.Equal(t, 2, c)
}

func TestGetDailyStats(t *testing.T) {
	repo, mock := newMockRepo(t)
	cols := []string{"day", "symbol", "open", "high", "low", "close", "volume", "vwap", "trade_count"}
	mock.ExpectQuery("time_bucket").WithArgs("AAPL", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow(time.Now(), "AAPL", "100", "110", "95", "105", int64(1000), 102.5, int32(50)))
	b, err := repo.GetDailyStats(context.Background(), "AAPL", time.Now())
	require.NoError(t, err)
	require.NotNil(t, b)
	assert.Equal(t, 50, b.TradeCount)
}

func TestGetDailyStats_NoRows(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("time_bucket").WillReturnError(sql.ErrNoRows)
	b, err := repo.GetDailyStats(context.Background(), "AAPL", time.Now())
	require.NoError(t, err)
	assert.Nil(t, b)
}

func TestGetDailyStats_Error(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("time_bucket").WillReturnError(errors.New("fail"))
	_, err := repo.GetDailyStats(context.Background(), "AAPL", time.Now())
	require.Error(t, err)
}

func TestGetOHLCV_ScanError(t *testing.T) {
	repo, mock := newMockRepo(t)
	// Only 2 columns but Scan expects 9 -> scan error path.
	mock.ExpectQuery("FROM ohlcv_1min").
		WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow("x", "y"))
	_, err := repo.GetOHLCV(context.Background(), "AAPL", time.Now(), time.Now(), models.Timeframe1Min)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scan")
}

func TestGetMonitoredSymbols_ScanError(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").
		WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("x"))
	_, err := repo.GetMonitoredSymbols(context.Background())
	require.Error(t, err)
}

func TestGetSymbolsNeedingBackfill_ScanError(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("LEFT JOIN backfill_status").
		WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow("x", "y"))
	_, err := repo.GetSymbolsNeedingBackfill(context.Background())
	require.Error(t, err)
}

func TestQueryGaps_ScanError(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow("x"))
	_, err := repo.QueryGaps(context.Background(), "SELECT 1", "AAPL", time.Now(), time.Now())
	require.Error(t, err)
}

func TestInsertOHLCVBatch_CommitError(t *testing.T) {
	repo, mock := newMockRepo(t)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit().WillReturnError(errors.New("commit fail"))
	err := repo.InsertOHLCVBatch(context.Background(), []models.OHLCV{bar("AAPL")})
	require.Error(t, err)
}

// ensure the SQL regexp matcher behavior is what we expect (defensive sanity).
func TestRegexpMatcherSanity(t *testing.T) {
	assert.True(t, regexp.MustCompile("INSERT INTO ohlcv_1min").MatchString("INSERT INTO ohlcv_1min (...)"))
}
