package main

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/ingestion"
	"github.com/trogers1052/market-data-ingestion/internal/marketdata"
	"github.com/trogers1052/market-data-ingestion/internal/models"
	"github.com/trogers1052/market-data-ingestion/internal/quality"
)

// stubMD implements marketdata.Client; GetTickerDetails returns a configurable
// name/error and GetMinuteBars is unused.
type stubMD struct {
	name string
	err  error
}

func (m stubMD) GetMinuteBars(ctx context.Context, symbol string, from, to time.Time) ([]models.OHLCV, error) {
	return nil, nil
}

func (m stubMD) GetTickerDetails(ctx context.Context, symbol string) (*marketdata.TickerDetails, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &marketdata.TickerDetails{Symbol: symbol, Name: m.name}, nil
}

func mockRepo(t *testing.T) (*database.Repository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return database.NewRepositoryWithDB(db), mock
}

func TestParseSymbolList(t *testing.T) {
	assert.Nil(t, parseSymbolList(""))
	assert.Nil(t, parseSymbolList("   "))
	assert.Equal(t, []string{"AAPL"}, parseSymbolList("aapl"))
	assert.Equal(t, []string{"AAPL", "MSFT", "NVDA"}, parseSymbolList(" aapl, msft ,nvda"))
	assert.Equal(t, []string{"AAPL"}, parseSymbolList("aapl,,"))
}

func TestParseAddSymbolArg(t *testing.T) {
	s, n := parseAddSymbolArg("aapl")
	assert.Equal(t, "AAPL", s)
	assert.Equal(t, "AAPL", n)

	s, n = parseAddSymbolArg("aapl:Apple Inc")
	assert.Equal(t, "AAPL", s)
	assert.Equal(t, "Apple Inc", n)

	s, n = parseAddSymbolArg("aapl:")
	assert.Equal(t, "AAPL", s)
	assert.Equal(t, "AAPL", n) // empty name falls back to symbol
}

func TestListSymbolsCmd(t *testing.T) {
	repo, mock := mockRepo(t)
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	mock.ExpectQuery("FROM monitored_symbols").
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow("AAPL", "Apple", true, time.Now(), time.Now()).
			AddRow("OLD", "Old Co", false, time.Now(), time.Now()))
	// GetBarCountExact for each
	mock.ExpectQuery("COUNT").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(int64(100)))
	mock.ExpectQuery("COUNT").WithArgs("OLD").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(int64(0)))

	require.NoError(t, listSymbolsCmd(context.Background(), repo))
}

func TestListSymbolsCmd_Error(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnError(errors.New("db"))
	require.Error(t, listSymbolsCmd(context.Background(), repo))
}

func TestAddSymbolCmd_WithMDName(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("AAPL", "Apple Inc", true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	md := stubMD{name: "Apple Inc"}
	sym, name, err := addSymbolCmd(context.Background(), repo, md, "aapl")
	require.NoError(t, err)
	assert.Equal(t, "AAPL", sym)
	assert.Equal(t, "Apple Inc", name)
}

func TestAddSymbolCmd_MDError(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("AAPL", "AAPL", true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	md := stubMD{err: errors.New("md down")}
	_, name, err := addSymbolCmd(context.Background(), repo, md, "aapl")
	require.NoError(t, err)
	assert.Equal(t, "AAPL", name) // falls back to symbol
}

func TestAddSymbolCmd_NilMD(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("AAPL", "Apple Inc", true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	_, name, err := addSymbolCmd(context.Background(), repo, nil, "aapl:Apple Inc")
	require.NoError(t, err)
	assert.Equal(t, "Apple Inc", name)
}

func TestAddSymbolCmd_UpsertError(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WillReturnError(errors.New("fail"))
	_, _, err := addSymbolCmd(context.Background(), repo, nil, "aapl:Apple")
	require.Error(t, err)
}

func TestRemoveSymbolCmd(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("AAPL", "", false).
		WillReturnResult(sqlmock.NewResult(1, 1))
	sym, err := removeSymbolCmd(context.Background(), repo, "aapl")
	require.NoError(t, err)
	assert.Equal(t, "AAPL", sym)
}

func TestRemoveSymbolCmd_Error(t *testing.T) {
	repo, mock := mockRepo(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WillReturnError(errors.New("fail"))
	_, err := removeSymbolCmd(context.Background(), repo, "aapl")
	require.Error(t, err)
}

func TestResolveStockServiceDSN(t *testing.T) {
	// Flag value wins.
	dsn, err := resolveStockServiceDSN("postgres://flag")
	require.NoError(t, err)
	assert.Equal(t, "postgres://flag", dsn)

	// Falls back to env var.
	t.Setenv("STOCK_SERVICE_DSN", "postgres://env")
	dsn, err = resolveStockServiceDSN("")
	require.NoError(t, err)
	assert.Equal(t, "postgres://env", dsn)

	// Neither set -> error.
	t.Setenv("STOCK_SERVICE_DSN", "")
	_, err = resolveStockServiceDSN("")
	require.Error(t, err)
}

type fakePositions struct {
	syms []string
	err  error
}

func (f fakePositions) GetPositionSymbols(ctx context.Context) ([]string, error) {
	return f.syms, f.err
}

func TestSyncPositionsCmd_AddsNew(t *testing.T) {
	repo, mock := mockRepo(t)
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(sqlmock.NewRows(cols))
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("NVDA", "", true).
		WillReturnResult(sqlmock.NewResult(1, 1))

	added, err := syncPositionsCmd(context.Background(), repo, fakePositions{syms: []string{"NVDA"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"NVDA"}, added)
}

func TestSyncPositionsCmd_None(t *testing.T) {
	repo, _ := mockRepo(t)
	added, err := syncPositionsCmd(context.Background(), repo, fakePositions{syms: nil})
	require.NoError(t, err)
	assert.Empty(t, added)
}

func TestSyncPositionsCmd_Error(t *testing.T) {
	repo, _ := mockRepo(t)
	_, err := syncPositionsCmd(context.Background(), repo, fakePositions{err: errors.New("src down")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sync failed")
}

func TestRunMigrationsFromSource_BadSource(t *testing.T) {
	// A non-existent source/driver causes migrate.New to fail.
	err := runMigrationsFromSource("nosuchscheme://x", "postgres://u:p@127.0.0.1:1/db")
	require.Error(t, err)
}

func TestRunMigrations_BadDatabaseURL(t *testing.T) {
	// migrate.New fails to construct a driver for an invalid database URL.
	err := runMigrations("://invalid")
	require.Error(t, err)
}

func newChecker(t *testing.T) (*quality.Checker, sqlmock.Sqlmock) {
	repo, mock := mockRepo(t)
	sched := ingestion.NewMarketScheduler(9, 16, false, false)
	return quality.NewChecker(repo, qualityMD{}, sched), mock
}

// qualityMD implements marketdata.Client for the quality checker.
type qualityMD struct{}

func (qualityMD) GetMinuteBars(ctx context.Context, symbol string, from, to time.Time) ([]models.OHLCV, error) {
	return nil, nil
}
func (qualityMD) GetTickerDetails(ctx context.Context, symbol string) (*marketdata.TickerDetails, error) {
	return &marketdata.TickerDetails{Symbol: symbol}, nil
}

func TestRunQualityCheck_SpecificSymbols(t *testing.T) {
	checker, mock := newChecker(t)
	// CheckSymbol("AAPL"): data range, count, gaps, zero vol, spikes
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mock.ExpectQuery("COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"time", "next_time"}))
	mock.ExpectQuery("volume = 0").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery("price_changes").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	err := runQualityCheck(context.Background(), checker, []string{"AAPL"}, 7)
	require.NoError(t, err)
}

func TestRunQualityCheck_SpecificSymbol_ErrorContinues(t *testing.T) {
	checker, mock := newChecker(t)
	mock.ExpectQuery("SELECT MIN").WillReturnError(errors.New("db"))
	// error is logged and skipped, so overall returns nil
	err := runQualityCheck(context.Background(), checker, []string{"AAPL"}, 7)
	require.NoError(t, err)
}

func TestRunQualityCheck_AllSymbols(t *testing.T) {
	checker, mock := newChecker(t)
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(sqlmock.NewRows(cols))
	err := runQualityCheck(context.Background(), checker, nil, 7)
	require.NoError(t, err)
}

func TestRunQualityCheck_AllSymbols_Error(t *testing.T) {
	checker, mock := newChecker(t)
	mock.ExpectQuery("FROM monitored_symbols").WillReturnError(errors.New("db"))
	err := runQualityCheck(context.Background(), checker, nil, 7)
	require.Error(t, err)
}
