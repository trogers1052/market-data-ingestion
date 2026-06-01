package watchlist

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/marketdata"
	"github.com/trogers1052/market-data-ingestion/internal/models"
	redisclient "github.com/trogers1052/market-data-ingestion/internal/redis"
)

// stubMD implements marketdata.Client. GetTickerDetails returns a name from
// the names map (if present); GetMinuteBars is unused by these tests.
type stubMD struct {
	names map[string]string
	err   error
}

func (m stubMD) GetMinuteBars(ctx context.Context, symbol string, from, to time.Time) ([]models.OHLCV, error) {
	return nil, nil
}

func (m stubMD) GetTickerDetails(ctx context.Context, symbol string) (*marketdata.TickerDetails, error) {
	if m.err != nil {
		return nil, m.err
	}
	name, ok := m.names[symbol]
	if !ok {
		return &marketdata.TickerDetails{Symbol: symbol}, nil
	}
	return &marketdata.TickerDetails{Symbol: symbol, Name: name}, nil
}

func newDeps(t *testing.T) (*database.Repository, sqlmock.Sqlmock, *redisclient.Client, *miniredis.Miniredis) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	return database.NewRepositoryWithDB(db), mock, redisclient.NewClientWithRedis(rc), mr
}

func monitoredRows(symbols ...string) *sqlmock.Rows {
	cols := []string{"symbol", "name", "enabled", "created_at", "updated_at"}
	rows := sqlmock.NewRows(cols)
	for _, s := range symbols {
		rows.AddRow(s, s, true, time.Now(), time.Now())
	}
	return rows
}

func TestSyncFromRedis_NoSymbols(t *testing.T) {
	repo, _, rc, _ := newDeps(t)
	svc := NewSyncService(repo, rc, stubMD{})
	added, err := svc.SyncFromRedis(context.Background())
	require.NoError(t, err)
	assert.Nil(t, added)
}

func TestSyncFromRedis_AddsNew(t *testing.T) {
	repo, mock, rc, mr := newDeps(t)
	mr.SAdd(redisclient.WatchlistKey, "AAPL", "NVDA")
	// details only for AAPL
	d, _ := json.Marshal(redisclient.WatchlistStock{Symbol: "AAPL", Name: "Apple Inc"})
	mr.HSet(redisclient.WatchlistDetailsKey, "AAPL", string(d))

	// currently monitoring nothing
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(monitoredRows())
	// AAPL added with the Redis-provided name
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("AAPL", "Apple Inc", true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// NVDA has no Redis name; mdClient supplies "NVIDIA"
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("NVDA", "NVIDIA", true).
		WillReturnResult(sqlmock.NewResult(1, 1))

	svc := NewSyncService(repo, rc, stubMD{names: map[string]string{"NVDA": "NVIDIA"}})
	added, err := svc.SyncFromRedis(context.Background())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"AAPL", "NVDA"}, added)

	// Both should be signalled for backfill
	got := drain(svc.BackfillChannel(), 2)
	assert.ElementsMatch(t, []string{"AAPL", "NVDA"}, got)
}

func TestSyncFromRedis_AllAlreadyMonitored(t *testing.T) {
	repo, mock, rc, mr := newDeps(t)
	mr.SAdd(redisclient.WatchlistKey, "AAPL")
	mock.ExpectQuery("FROM monitored_symbols").WillReturnRows(monitoredRows("AAPL"))

	svc := NewSyncService(repo, rc, stubMD{})
	added, err := svc.SyncFromRedis(context.Background())
	require.NoError(t, err)
	assert.Empty(t, added)
}

func TestSyncFromRedis_MonitoredQueryError(t *testing.T) {
	repo, mock, rc, mr := newDeps(t)
	mr.SAdd(redisclient.WatchlistKey, "AAPL")
	mock.ExpectQuery("FROM monitored_symbols").WillReturnError(errors.New("db"))
	svc := NewSyncService(repo, rc, stubMD{})
	_, err := svc.SyncFromRedis(context.Background())
	require.Error(t, err)
}

func TestOnSymbolAdded(t *testing.T) {
	repo, mock, rc, _ := newDeps(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("AAPL", "Apple Inc", true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	svc := NewSyncService(repo, rc, stubMD{})
	require.NoError(t, svc.OnSymbolAdded(context.Background(), "AAPL", "Apple Inc"))
	got := drain(svc.BackfillChannel(), 1)
	assert.Equal(t, []string{"AAPL"}, got)
}

func TestOnSymbolAdded_ResolvesNameFromMD(t *testing.T) {
	repo, mock, rc, _ := newDeps(t)
	// name == symbol triggers lookup
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("AAPL", "Apple Inc", true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	svc := NewSyncService(repo, rc, stubMD{names: map[string]string{"AAPL": "Apple Inc"}})
	require.NoError(t, svc.OnSymbolAdded(context.Background(), "AAPL", "AAPL"))
}

func TestOnSymbolAdded_UpsertError(t *testing.T) {
	repo, mock, rc, _ := newDeps(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WillReturnError(errors.New("fail"))
	svc := NewSyncService(repo, rc, stubMD{})
	require.Error(t, svc.OnSymbolAdded(context.Background(), "AAPL", "Apple"))
}

func TestOnSymbolRemoved(t *testing.T) {
	repo, mock, rc, _ := newDeps(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WithArgs("AAPL", "", false).
		WillReturnResult(sqlmock.NewResult(1, 1))
	svc := NewSyncService(repo, rc, stubMD{})
	require.NoError(t, svc.OnSymbolRemoved(context.Background(), "AAPL"))
}

func TestOnSymbolRemoved_Error(t *testing.T) {
	repo, mock, rc, _ := newDeps(t)
	mock.ExpectExec("INSERT INTO monitored_symbols").WillReturnError(errors.New("fail"))
	svc := NewSyncService(repo, rc, stubMD{})
	require.Error(t, svc.OnSymbolRemoved(context.Background(), "AAPL"))
}

func TestSetConsumer(t *testing.T) {
	repo, _, rc, _ := newDeps(t)
	svc := NewSyncService(repo, rc, stubMD{})
	// Passing nil exercises SetConsumer and leaves StartConsumer a no-op.
	svc.SetConsumer(nil)
	require.NoError(t, svc.StartConsumer(context.Background()))
	require.NoError(t, svc.Close())
}

func TestStartConsumer_NilConsumer(t *testing.T) {
	repo, _, rc, _ := newDeps(t)
	svc := NewSyncService(repo, rc, stubMD{})
	require.NoError(t, svc.StartConsumer(context.Background()))
}

func TestClose_NoConsumer(t *testing.T) {
	repo, _, rc, _ := newDeps(t)
	svc := NewSyncService(repo, rc, stubMD{})
	require.NoError(t, svc.Close())
}

// drain reads up to n items from a channel without blocking forever.
func drain(ch <-chan string, n int) []string {
	var out []string
	for i := 0; i < n; i++ {
		select {
		case s := <-ch:
			out = append(out, s)
		case <-time.After(time.Second):
			return out
		}
	}
	return out
}
