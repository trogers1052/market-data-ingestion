package ingestion

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/marketdata"
	"github.com/trogers1052/market-data-ingestion/internal/models"
)

func newBackfill(md marketdata.Client, repo *database.Repository, months, delayDays int) *BackfillService {
	return NewBackfillService(md, repo, months, nil, delayDays)
}

func TestNewBackfillService_NegativeDelayClamped(t *testing.T) {
	repo, _ := mockRepo(t)
	s := NewBackfillService(&stubMD{}, repo, 12, nil, -5)
	assert.Equal(t, 0, s.delayDays)
}

func TestBackfillSymbol_DataRangeError(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("SELECT MIN").WillReturnError(errors.New("db"))
	s := newBackfill(&stubMD{}, repo, 12, 0)
	err := s.BackfillSymbol(context.Background(), "AAPL")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot determine existing data range")
}

func TestBackfillSymbol_NoExistingData(t *testing.T) {
	repo, mock := mockRepo(t)
	// 1 month range keeps the chunk loop small
	bar := validBar("AAPL", time.Now().Add(-24*time.Hour))
	md := &stubMD{bars: []models.OHLCV{bar}, once: true}
	s := newBackfill(md, repo, 1, 0)

	// GetDataRange -> nil (no data)
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	// GetBarCount
	mock.ExpectQuery("COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))

	// fetchDateRange chunks: each chunk does UpdateBackfillStatus(in_progress),
	// then for each non-empty result: InsertOHLCVBatch + GetBarCount + GetDataRange.
	// We accept any number of these via AnyArg ordering disabled.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectExec("INSERT INTO backfill_status").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(time.Now(), time.Now()))
	// final GetDataRange in fetchDateRange + after-fetch GetDataRange + status update
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(time.Now(), time.Now()))
	mock.ExpectExec("INSERT INTO backfill_status").WillReturnResult(sqlmock.NewResult(1, 1))

	err := s.BackfillSymbol(context.Background(), "AAPL")
	require.NoError(t, err)
}

func TestBackfillSymbol_UpToDate(t *testing.T) {
	repo, mock := mockRepo(t)
	s := newBackfill(&stubMD{}, repo, 1, 0)

	// Existing data already covers the full target range:
	// minTime well before startDate, maxTime recent (after oneDayAgo).
	minT := time.Now().AddDate(0, -6, 0)
	maxT := time.Now()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(minT, maxT))
	mock.ExpectQuery("COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1000)))
	// After "up to date", GetDataRange again + UpdateBackfillStatus(completed)
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(minT, maxT))
	mock.ExpectExec("INSERT INTO backfill_status").WillReturnResult(sqlmock.NewResult(1, 1))

	err := s.BackfillSymbol(context.Background(), "AAPL")
	require.NoError(t, err)
}

func TestBackfillSymbol_ExtendBackwardsAndForwards(t *testing.T) {
	repo, mock := mockRepo(t)
	// Existing data is a narrow window in the middle of the target range, so
	// the service must extend both backwards and forwards.
	minT := time.Now().AddDate(0, 0, -10) // after startDate (1 month ago)
	maxT := time.Now().AddDate(0, 0, -5)  // before oneDayAgo
	md := &stubMD{bars: []models.OHLCV{validBar("AAPL", time.Now().Add(-48*time.Hour))}, once: true}
	s := newBackfill(md, repo, 1, 0)

	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(minT, maxT))
	mock.ExpectQuery("COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(100)))
	// Many UpdateBackfillStatus / GetDataRange / Insert / GetBarCount calls happen;
	// with unordered matching we register generous expectations.
	for i := 0; i < 12; i++ {
		mock.ExpectExec("INSERT INTO backfill_status").WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	for i := 0; i < 12; i++ {
		mock.ExpectQuery("COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	}
	for i := 0; i < 12; i++ {
		mock.ExpectQuery("SELECT MIN").
			WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(minT, maxT))
	}

	err := s.BackfillSymbol(context.Background(), "AAPL")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, md.calls, 2) // both directions fetched
}

func TestBackfillSymbol_FetchError(t *testing.T) {
	repo, mock := mockRepo(t)
	md := &stubMD{barsErr: errors.New("api down")}
	s := newBackfill(md, repo, 1, 0)
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mock.ExpectQuery("COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))
	for i := 0; i < 4; i++ {
		mock.ExpectExec("INSERT INTO backfill_status").WillReturnResult(sqlmock.NewResult(1, 1))
	}
	err := s.BackfillSymbol(context.Background(), "AAPL")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backfill failed")
}

func TestBackfillSymbol_InsertError(t *testing.T) {
	repo, mock := mockRepo(t)
	md := &stubMD{bars: []models.OHLCV{validBar("AAPL", time.Now().Add(-48*time.Hour))}}
	s := newBackfill(md, repo, 1, 0)
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mock.ExpectQuery("COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))
	for i := 0; i < 4; i++ {
		mock.ExpectExec("INSERT INTO backfill_status").WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnError(errors.New("insert fail"))
	mock.ExpectRollback()
	err := s.BackfillSymbol(context.Background(), "AAPL")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to insert bars")
}

func TestBackfillAll_WithSymbol(t *testing.T) {
	repo, mock := mockRepo(t)
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	minT := time.Now().AddDate(0, -6, 0)
	maxT := time.Now()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM monitored_symbols").
		WillReturnRows(sqlmock.NewRows(cols).AddRow("AAPL", "Apple", true, time.Now(), time.Now()))
	// BackfillSymbol up-to-date path
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(minT, maxT))
	mock.ExpectQuery("COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1000)))
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(minT, maxT))
	mock.ExpectExec("INSERT INTO backfill_status").WillReturnResult(sqlmock.NewResult(1, 1))

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	s := newBackfill(&stubMD{}, repo, 1, 0)
	err := s.BackfillAll(ctx)
	// Either completes or is cancelled during the inter-symbol delay.
	if err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}
}

func TestFillGaps_ExtendsBothDirections(t *testing.T) {
	repo, mock := mockRepo(t)
	minT := time.Now().AddDate(0, 0, -10) // after target start -> extend backwards
	maxT := time.Now().AddDate(0, 0, -5)  // before checkDate -> extend forwards
	md := &stubMD{bars: []models.OHLCV{validBar("AAPL", time.Now().Add(-72*time.Hour))}, once: true}
	s := newBackfill(md, repo, 1, 0)

	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(minT, maxT))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	require.NoError(t, s.FillGaps(context.Background(), "AAPL"))
}

func TestFillGaps_BackwardsFetchError(t *testing.T) {
	repo, mock := mockRepo(t)
	minT := time.Now().AddDate(0, 0, -10)
	maxT := time.Now().AddDate(0, 0, -5)
	md := &stubMD{barsErr: errors.New("fetch fail")}
	s := newBackfill(md, repo, 1, 0)
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(minT, maxT))
	err := s.FillGaps(context.Background(), "AAPL")
	require.Error(t, err)
}

func TestBackfillAll_NoSymbols(t *testing.T) {
	repo, mock := mockRepo(t)
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(sqlmock.NewRows(cols))
	s := newBackfill(&stubMD{}, repo, 1, 0)
	require.NoError(t, s.BackfillAll(context.Background()))
}

func TestBackfillAll_MonitoredError(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnError(errors.New("db"))
	s := newBackfill(&stubMD{}, repo, 1, 0)
	require.Error(t, s.BackfillAll(context.Background()))
}

func TestBackfillSymbols_Empty(t *testing.T) {
	repo, _ := mockRepo(t)
	s := newBackfill(&stubMD{}, repo, 1, 0)
	require.NoError(t, s.BackfillSymbols(context.Background(), nil))
}

func TestBackfillSymbols_FailureAggregated(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("SELECT MIN").WillReturnError(errors.New("db"))
	s := newBackfill(&stubMD{}, repo, 1, 0)
	err := s.BackfillSymbols(context.Background(), []string{"AAPL"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backfill failed for 1 symbols")
}

func TestBackfillSymbols_ContextCancelled(t *testing.T) {
	repo, mock := mockRepo(t)
	// Provide existing up-to-date data so BackfillSymbol succeeds quickly.
	minT := time.Now().AddDate(0, -6, 0)
	maxT := time.Now()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(minT, maxT))
	mock.ExpectQuery("COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1000)))
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(minT, maxT))
	mock.ExpectExec("INSERT INTO backfill_status").WillReturnResult(sqlmock.NewResult(1, 1))

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after first symbol processed via the inter-symbol delay select.
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	s := newBackfill(&stubMD{}, repo, 1, 0)
	err := s.BackfillSymbols(ctx, []string{"AAPL"})
	require.ErrorIs(t, err, context.Canceled)
}

func TestFillGaps_NoData(t *testing.T) {
	repo, mock := mockRepo(t)
	// GetDataRange -> nil triggers full BackfillSymbol path.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	// BackfillSymbol re-queries data range + count, then (1 month) no bars from md.
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mock.ExpectQuery("COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))
	// fetchDateRange: status in_progress, no bars returned, final GetDataRange
	mock.ExpectExec("INSERT INTO backfill_status").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	// completed status
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mock.ExpectExec("INSERT INTO backfill_status").WillReturnResult(sqlmock.NewResult(1, 1))

	s := newBackfill(&stubMD{bars: nil}, repo, 1, 0)
	require.NoError(t, s.FillGaps(context.Background(), "AAPL"))
}

func TestFillGaps_DataRangeError(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("SELECT MIN").WillReturnError(errors.New("db"))
	s := newBackfill(&stubMD{}, repo, 1, 0)
	require.Error(t, s.FillGaps(context.Background(), "AAPL"))
}

func TestGetBackfillProgress(t *testing.T) {
	repo, mock := mockRepo(t)
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	mock.ExpectQuery("FROM monitored_symbols").
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow("AAPL", "Apple", true, time.Now(), time.Now()).
			AddRow("MSFT", "Microsoft", true, time.Now(), time.Now()))

	bsCols := []string{"symbol", "last_backfill", "backfill_start", "backfill_end", "status", "error_message", "created_at", "updated_at"}
	now := time.Now()
	// AAPL has a status row
	mock.ExpectQuery("FROM backfill_status").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows(bsCols).
			AddRow("AAPL", now, now, now, "completed", nil, now, now))
	// MSFT has no row -> defaults to pending
	mock.ExpectQuery("FROM backfill_status").WithArgs("MSFT").
		WillReturnError(sql.ErrNoRows)

	s := newBackfill(&stubMD{}, repo, 1, 0)
	progress, err := s.GetBackfillProgress(context.Background())
	require.NoError(t, err)
	require.Len(t, progress, 2)
	assert.Equal(t, "completed", progress["AAPL"].Status)
	assert.Equal(t, models.BackfillStatusPending, progress["MSFT"].Status)
}

func TestGetBackfillProgress_MonitoredError(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnError(errors.New("db"))
	s := newBackfill(&stubMD{}, repo, 1, 0)
	_, err := s.GetBackfillProgress(context.Background())
	require.Error(t, err)
}
