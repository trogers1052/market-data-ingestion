package quality

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
	"github.com/trogers1052/market-data-ingestion/internal/ingestion"
	"github.com/trogers1052/market-data-ingestion/internal/marketdata"
	"github.com/trogers1052/market-data-ingestion/internal/models"
)

type stubMD struct {
	bars    []models.OHLCV
	barsErr error
}

func (m stubMD) GetMinuteBars(ctx context.Context, symbol string, from, to time.Time) ([]models.OHLCV, error) {
	return m.bars, m.barsErr
}

func (m stubMD) GetTickerDetails(ctx context.Context, symbol string) (*marketdata.TickerDetails, error) {
	return &marketdata.TickerDetails{Symbol: symbol}, nil
}

func setup(t *testing.T, md marketdata.Client) (*Checker, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	repo := database.NewRepositoryWithDB(db)
	sched := ingestion.NewMarketScheduler(9, 16, false, false)
	return NewChecker(repo, md, sched), mock
}

func TestNewChecker(t *testing.T) {
	c, _ := setup(t, stubMD{})
	assert.NotNil(t, c)
}

func TestCheckSymbol(t *testing.T) {
	c, mock := setup(t, stubMD{})
	now := time.Now()

	// GetDataRange
	mock.ExpectQuery("SELECT MIN").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(now.AddDate(0, 0, -10), now))
	// GetBarCountExact
	mock.ExpectQuery("COUNT").WithArgs("AAPL").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1000)))
	// detectGaps -> QueryGaps
	mock.ExpectQuery("SELECT").WithArgs("AAPL", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"time", "next_time"}).
			AddRow(now, now.Add(10*time.Minute)))
	// detectAnomalies -> CountZeroVolumeBars
	mock.ExpectQuery("volume = 0").WithArgs("AAPL", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	// detectAnomalies -> CountPriceSpikes
	mock.ExpectQuery("price_changes").WithArgs("AAPL", sqlmock.AnyArg(), sqlmock.AnyArg(), 0.10).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	from := now.AddDate(0, 0, -5)
	report, err := c.CheckSymbol(context.Background(), "AAPL", from, now)
	require.NoError(t, err)
	assert.Equal(t, "AAPL", report.Symbol)
	assert.Equal(t, int64(1000), report.TotalBars)
	assert.NotNil(t, report.DataRange)
	assert.Len(t, report.Gaps, 1)
	assert.Len(t, report.Anomalies, 2) // zero_volume + price_spike
	assert.Greater(t, report.ExpectedBars, int64(0))
}

func TestCheckSymbol_DataRangeError(t *testing.T) {
	c, mock := setup(t, stubMD{})
	mock.ExpectQuery("SELECT MIN").WillReturnError(errors.New("db"))
	_, err := c.CheckSymbol(context.Background(), "AAPL", time.Now().AddDate(0, 0, -1), time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "data range")
}

func TestCheckSymbol_BarCountError(t *testing.T) {
	c, mock := setup(t, stubMD{})
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mock.ExpectQuery("COUNT").WillReturnError(errors.New("count fail"))
	_, err := c.CheckSymbol(context.Background(), "AAPL", time.Now().AddDate(0, 0, -1), time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bar count")
}

func TestCheckSymbol_GapAndAnomalyErrorsTolerated(t *testing.T) {
	c, mock := setup(t, stubMD{})
	now := time.Now()
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mock.ExpectQuery("COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))
	mock.ExpectQuery("SELECT").WillReturnError(errors.New("gap fail")) // detectGaps fails -> logged
	mock.ExpectQuery("volume = 0").WillReturnError(errors.New("anom fail"))

	report, err := c.CheckSymbol(context.Background(), "AAPL", now.AddDate(0, 0, -1), now)
	require.NoError(t, err)
	assert.Empty(t, report.Gaps)
	assert.Empty(t, report.Anomalies)
}

func makeBar(symbol string) models.OHLCV {
	return models.OHLCV{
		Time: time.Now(), Symbol: symbol,
		Open: decimal.NewFromInt(100), High: decimal.NewFromInt(110),
		Low: decimal.NewFromInt(95), Close: decimal.NewFromInt(105), Volume: 100,
	}
}

func TestFillGaps_Empty(t *testing.T) {
	c, _ := setup(t, stubMD{})
	n, err := c.FillGaps(context.Background(), "AAPL", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestFillGaps_Success(t *testing.T) {
	md := stubMD{bars: []models.OHLCV{makeBar("AAPL"), makeBar("AAPL")}}
	c, mock := setup(t, md)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnResult(sqlmock.NewResult(1, 2))
	mock.ExpectCommit()

	gaps := []Gap{{Symbol: "AAPL", StartTime: time.Now().Add(-time.Hour), EndTime: time.Now()}}
	n, err := c.FillGaps(context.Background(), "AAPL", gaps)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

func TestFillGaps_FetchErrorContinues(t *testing.T) {
	md := stubMD{barsErr: errors.New("fetch fail")}
	c, _ := setup(t, md)
	gaps := []Gap{{Symbol: "AAPL", StartTime: time.Now().Add(-time.Hour), EndTime: time.Now()}}
	n, err := c.FillGaps(context.Background(), "AAPL", gaps)
	require.NoError(t, err)
	assert.Equal(t, 0, n) // fetch failed -> skipped
}

func TestFillGaps_InsertErrorContinues(t *testing.T) {
	md := stubMD{bars: []models.OHLCV{makeBar("AAPL")}}
	c, mock := setup(t, md)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnError(errors.New("insert fail"))
	mock.ExpectRollback()
	gaps := []Gap{{Symbol: "AAPL", StartTime: time.Now().Add(-time.Hour), EndTime: time.Now()}}
	n, err := c.FillGaps(context.Background(), "AAPL", gaps)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestCheckAllSymbols(t *testing.T) {
	c, mock := setup(t, stubMD{})
	now := time.Now()
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	mock.ExpectQuery("FROM monitored_symbols").
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow("AAPL", "Apple", true, now, now))

	// CheckSymbol("AAPL"): data range, count, gaps, zero vol, spikes
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mock.ExpectQuery("COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"time", "next_time"}))
	mock.ExpectQuery("volume = 0").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery("price_changes").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	reports, err := c.CheckAllSymbols(context.Background(), now.AddDate(0, 0, -1), now)
	require.NoError(t, err)
	assert.Len(t, reports, 1)
	assert.Contains(t, reports, "AAPL")
}

func TestCheckAllSymbols_MonitoredError(t *testing.T) {
	c, mock := setup(t, stubMD{})
	mock.ExpectQuery("FROM monitored_symbols").WillReturnError(errors.New("db"))
	_, err := c.CheckAllSymbols(context.Background(), time.Now().AddDate(0, 0, -1), time.Now())
	require.Error(t, err)
}

func TestAutoFill(t *testing.T) {
	md := stubMD{bars: []models.OHLCV{makeBar("AAPL")}}
	c, mock := setup(t, md)
	now := time.Now()
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	mock.ExpectQuery("FROM monitored_symbols").
		WillReturnRows(sqlmock.NewRows(cols).AddRow("AAPL", "Apple", true, now, now))

	// CheckSymbol for AAPL — return one gap
	mock.ExpectQuery("SELECT MIN").
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mock.ExpectQuery("COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows([]string{"time", "next_time"}).
			AddRow(now, now.Add(10*time.Minute)))
	mock.ExpectQuery("volume = 0").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery("price_changes").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	// FillGaps -> insert
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO ohlcv_1min").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := c.AutoFill(context.Background(), now.AddDate(0, 0, -1), now)
	require.NoError(t, err)
}

func TestAutoFill_CheckError(t *testing.T) {
	c, mock := setup(t, stubMD{})
	mock.ExpectQuery("FROM monitored_symbols").WillReturnError(errors.New("db"))
	err := c.AutoFill(context.Background(), time.Now().AddDate(0, 0, -1), time.Now())
	require.Error(t, err)
}
