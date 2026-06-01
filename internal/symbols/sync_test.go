package symbols

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/market-data-ingestion/internal/database"
)

type fakePositions struct {
	symbols []string
	err     error
}

func (f *fakePositions) GetPositionSymbols(ctx context.Context) ([]string, error) {
	return f.symbols, f.err
}

func mockRepo(t *testing.T) (*database.Repository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return database.NewRepositoryWithDB(db), mock
}

func monitoredRows(symbols ...string) *sqlmock.Rows {
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	rows := sqlmock.NewRows(cols)
	for _, s := range symbols {
		rows.AddRow(s, s, true, time.Now(), time.Now())
	}
	return rows
}

func TestSyncFromPositions_NilSource(t *testing.T) {
	repo, _ := mockRepo(t)
	svc := NewSymbolSyncService(repo, nil, time.Minute)
	added, err := svc.SyncFromPositions(context.Background())
	require.NoError(t, err)
	assert.Nil(t, added)
}

func TestSyncFromPositions_SourceError(t *testing.T) {
	repo, _ := mockRepo(t)
	svc := NewSymbolSyncService(repo, &fakePositions{err: errors.New("boom")}, time.Minute)
	_, err := svc.SyncFromPositions(context.Background())
	require.Error(t, err)
}

func TestSyncFromPositions_NoSymbols(t *testing.T) {
	repo, _ := mockRepo(t)
	svc := NewSymbolSyncService(repo, &fakePositions{symbols: nil}, time.Minute)
	added, err := svc.SyncFromPositions(context.Background())
	require.NoError(t, err)
	assert.Nil(t, added)
}

func TestSyncFromPositions_AddsNewSymbols(t *testing.T) {
	repo, mock := mockRepo(t)
	// position has AAPL (existing) and NVDA (new)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(monitoredRows("AAPL"))
	mock.ExpectExec("INSERT INTO monitored_symbols").
		WithArgs("NVDA", "", true).
		WillReturnResult(sqlmock.NewResult(1, 1))

	svc := NewSymbolSyncService(repo, &fakePositions{symbols: []string{"AAPL", "NVDA"}}, time.Minute)
	added, err := svc.SyncFromPositions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"NVDA"}, added)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSyncFromPositions_MonitoredQueryError(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnError(errors.New("db"))
	svc := NewSymbolSyncService(repo, &fakePositions{symbols: []string{"AAPL"}}, time.Minute)
	_, err := svc.SyncFromPositions(context.Background())
	require.Error(t, err)
}

func TestSyncFromPositions_UpsertErrorSkips(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(monitoredRows())
	mock.ExpectExec("INSERT INTO monitored_symbols").WillReturnError(errors.New("upsert fail"))
	svc := NewSymbolSyncService(repo, &fakePositions{symbols: []string{"NVDA"}}, time.Minute)
	added, err := svc.SyncFromPositions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, added) // upsert failed -> skipped
}

func TestGetAllSymbols(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(monitoredRows("AAPL", "MSFT"))
	svc := NewSymbolSyncService(repo, &fakePositions{symbols: []string{"MSFT", "NVDA"}}, time.Minute)

	syms, err := svc.GetAllSymbols(context.Background())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"AAPL", "MSFT", "NVDA"}, syms)
}

func TestGetAllSymbols_NoPositionsSource(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(monitoredRows("AAPL"))
	svc := NewSymbolSyncService(repo, nil, time.Minute)
	syms, err := svc.GetAllSymbols(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"AAPL"}, syms)
}

func TestGetAllSymbols_PositionsErrorIgnored(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(monitoredRows("AAPL"))
	svc := NewSymbolSyncService(repo, &fakePositions{err: errors.New("nope")}, time.Minute)
	syms, err := svc.GetAllSymbols(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"AAPL"}, syms)
}

func TestGetAllSymbols_MonitoredError(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnError(errors.New("db"))
	svc := NewSymbolSyncService(repo, nil, time.Minute)
	_, err := svc.GetAllSymbols(context.Background())
	require.Error(t, err)
}

func TestAddWatchlistSymbols(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("AAPL", "", true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("MSFT", "", true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	svc := NewSymbolSyncService(repo, nil, time.Minute)
	require.NoError(t, svc.AddWatchlistSymbols(context.Background(), []string{"AAPL", "MSFT"}))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAddWatchlistSymbols_Error(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WillReturnError(errors.New("fail"))
	svc := NewSymbolSyncService(repo, nil, time.Minute)
	require.Error(t, svc.AddWatchlistSymbols(context.Background(), []string{"AAPL"}))
}

func TestRemoveWatchlistSymbol(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("AAPL", "", false).
		WillReturnResult(sqlmock.NewResult(1, 1))
	svc := NewSymbolSyncService(repo, nil, time.Minute)
	require.NoError(t, svc.RemoveWatchlistSymbol(context.Background(), "AAPL"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStockServiceDB_GetPositionSymbols(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := NewStockServiceDBWithDB(db)

	mock.ExpectQuery("FROM positions").
		WillReturnRows(sqlmock.NewRows([]string{"symbol"}).AddRow("AAPL").AddRow("MSFT"))
	syms, err := s.GetPositionSymbols(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"AAPL", "MSFT"}, syms)
}

func TestStockServiceDB_GetPositionSymbols_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := NewStockServiceDBWithDB(db)
	mock.ExpectQuery("FROM positions").WillReturnError(errors.New("db"))
	_, err = s.GetPositionSymbols(context.Background())
	require.Error(t, err)
}

func TestStockServiceDB_GetPositionSymbols_ScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := NewStockServiceDBWithDB(db)
	// Two columns but Scan expects one -> scan error
	mock.ExpectQuery("FROM positions").
		WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).AddRow("x", "y"))
	_, err = s.GetPositionSymbols(context.Background())
	require.Error(t, err)
}

func TestStockServiceDB_Close(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	s := NewStockServiceDBWithDB(db)
	mock.ExpectClose()
	require.NoError(t, s.Close())
}

func TestNewStockServiceDB_BadDSN(t *testing.T) {
	// "postgres" driver Open succeeds lazily; Ping fails on an unreachable host.
	_, err := NewStockServiceDB("postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	require.Error(t, err)
}

func TestStartPeriodicSync_InitialThenCancel(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(monitoredRows())
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("NVDA", "", true).
		WillReturnResult(sqlmock.NewResult(1, 1))

	svc := NewSymbolSyncService(repo, &fakePositions{symbols: []string{"NVDA"}}, time.Hour)

	var changed []string
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		svc.StartPeriodicSync(ctx, func(s []string) { changed = s })
		close(done)
	}()
	// Give the initial sync a moment, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done
	assert.Equal(t, []string{"NVDA"}, changed)
}
