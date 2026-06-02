package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	testkit "github.com/trogers1052/trading-testkit"
)

// fakeAlpacaServer returns an httptest server that answers the two Alpaca
// endpoints the service touches during bootstrap (bars + assets) with empty but
// valid JSON, so no real network/API calls are made.
func fakeAlpacaServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	// Bars endpoint: data API. Return an empty bar set so backfill/polling
	// complete without error.
	mux.HandleFunc("/v2/stocks/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"bars":[],"next_page_token":""}`))
	})
	// Assets endpoint: broker API. Return a minimal asset.
	mux.HandleFunc("/v2/assets/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		sym := strings.TrimPrefix(r.URL.Path, "/v2/assets/")
		_, _ = w.Write([]byte(`{"symbol":"` + sym + `","name":"` + sym + `"}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// repoMigrationsURL returns the file:// source URL pointing at this repo's
// db/migrations directory (../../db/migrations relative to this test file).
func repoMigrationsURL(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller failed")
	// thisFile = <repo>/cmd/ingestion/run_integration_test.go
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	migrations := filepath.Join(repoRoot, "db", "migrations")
	return "file://" + migrations
}

// serviceEnv bundles the running backing containers for a test.
type serviceEnv struct {
	ts     *testkit.PostgresContainer
	redisC *testkit.RedisContainer
	rp     *testkit.RedpandaContainer
}

// setupServiceEnv stands up real TimescaleDB, Redis and Redpanda via
// testcontainers, fakes the Alpaca API, sets all the config env vars so the
// service points at the containers, and redirects the migration source at this
// repo's db/migrations directory. healthPort/metricsPort must be unique per test
// to avoid bind clashes with sibling tests in this package.
func setupServiceEnv(t *testing.T, healthPort, metricsPort string) *serviceEnv {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// --- Real backing services via testcontainers ---
	ts := testkit.NewTimescaleContainer(t)
	redisC := testkit.NewRedisContainer(t)
	rp := testkit.NewRedpandaContainer(t)

	// Pre-create the topics the service consumes/produces so Kafka wiring
	// succeeds immediately.
	rp.CreateTopic(t, "stock.quotes.realtime", 1)
	rp.CreateTopic(t, "trading.watchlist", 1)

	// --- Parse the Timescale connection string into host/port ---
	pgURL, err := url.Parse(ts.ConnStr)
	require.NoError(t, err)
	dbHost := pgURL.Hostname()
	dbPort := pgURL.Port()
	require.NotEmpty(t, dbHost)
	require.NotEmpty(t, dbPort)

	redisHost, redisPort, err := splitHostPort(redisC.Addr)
	require.NoError(t, err)

	// --- Fake Alpaca so no real API calls happen ---
	alpaca := fakeAlpacaServer(t)
	t.Setenv("ALPACA_DATA_URL", alpaca.URL)
	t.Setenv("ALPACA_BROKER_URL", alpaca.URL)
	t.Setenv("ALPACA_API_KEY_ID", "test-key")
	t.Setenv("ALPACA_API_SECRET_KEY", "test-secret")

	// --- Point config at the containers ---
	t.Setenv("DB_HOST", dbHost)
	t.Setenv("DB_PORT", dbPort)
	t.Setenv("DB_USER", "testuser")
	t.Setenv("DB_PASSWORD", "testpass")
	t.Setenv("DB_NAME", "testdb")
	t.Setenv("DB_SSL_MODE", "disable")

	t.Setenv("REDIS_HOST", redisHost)
	t.Setenv("REDIS_PORT", redisPort)
	t.Setenv("REDIS_PASSWORD", "")
	t.Setenv("REDIS_DB", "0")

	t.Setenv("KAFKA_BROKERS", rp.Brokers)
	t.Setenv("KAFKA_ENABLED", "true")
	t.Setenv("WATCHLIST_SYNC_ENABLED", "true")
	t.Setenv("KAFKA_QUOTES_TOPIC", "stock.quotes.realtime")
	t.Setenv("KAFKA_WATCHLIST_TOPIC", "trading.watchlist")

	// Poll quickly so the polling loop actually runs during the test window.
	t.Setenv("POLL_INTERVAL_SECONDS", "1")
	t.Setenv("POLLING_DELAY_MINUTES", "0")

	// Keep backfill tiny: a 60-month backfill paginates into thousands of
	// requests, which is needless work in tests. One month is enough to
	// exercise the backfill wiring against the fake Alpaca server.
	t.Setenv("BACKFILL_MONTHS", "1")
	t.Setenv("BACKFILL_DELAY_DAYS", "0")

	// Unique ports for the health/metrics servers to avoid clashing with
	// other tests in this package.
	t.Setenv("HEALTH_PORT", healthPort)
	t.Setenv("METRICS_PORT", metricsPort)

	// Point migrations at this repo's db/migrations directory (the default
	// is the container path /db/migrations).
	prevMigrations := migrationsSourceURL
	migrationsSourceURL = repoMigrationsURL(t)
	t.Cleanup(func() { migrationsSourceURL = prevMigrations })

	return &serviceEnv{ts: ts, redisC: redisC, rp: rp}
}

// TestRun_Integration_BootAndShutdown stands up real TimescaleDB, Redis and
// Redpanda via testcontainers, points the service config at them, runs run() in
// long-running mode, lets it boot (migrations + producers/consumers + polling),
// then cancels the context and asserts a clean shutdown. This exercises the
// whole bootstrap/wiring path in cmd/ingestion/main.go.
//
// It skips under -short so CI without Docker still passes.
func TestRun_Integration_BootAndShutdown(t *testing.T) {
	env := setupServiceEnv(t, "18181", "18282")
	ts := env.ts

	// --- Run the service in long-running mode ---
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cliOptions{days: 30})
	}()

	// The health endpoint binds first; wait for it as a coarse liveness signal.
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://localhost:18181/health")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 200*time.Millisecond, "service health endpoint never came up")

	// Boot completes asynchronously: wait until migrations have applied and the
	// seeded context symbol exists. This proves the migration wiring ran.
	require.Eventually(t, func() bool {
		var count int
		if err := ts.DB.QueryRow(
			`SELECT COUNT(*) FROM monitored_symbols WHERE symbol = 'SPY'`).Scan(&count); err != nil {
			return false
		}
		return count == 1
	}, 30*time.Second, 250*time.Millisecond, "migrations/seed did not apply")

	// Give the polling loop a moment to spin at least once.
	time.Sleep(1500 * time.Millisecond)

	// --- Trigger graceful shutdown ---
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "run() should return nil on context-cancelled shutdown")
	case <-time.After(30 * time.Second):
		t.Fatal("run() did not shut down within 30s of context cancellation")
	}

	// After shutdown the health endpoint should be closed.
	require.Eventually(t, func() bool {
		_, err := http.Get("http://localhost:18181/health")
		return err != nil
	}, 10*time.Second, 200*time.Millisecond, "health endpoint should be closed after shutdown")
}

// TestRun_Integration_Commands exercises the one-shot command modes of run()
// against real backing containers. Each mode runs the full bootstrap (DB,
// migrations, Alpaca, scheduler) and then the command branch before returning,
// which covers the large command-dispatch section of run() that the
// long-running test skips.
func TestRun_Integration_Commands(t *testing.T) {
	env := setupServiceEnv(t, "18183", "18284")
	ts := env.ts

	ctx := context.Background()

	// list-symbols: bootstrap + migrations (seeds 17 symbols) + list + return.
	require.NoError(t, run(ctx, cliOptions{listSymbols: true, days: 30}))

	// Confirm migrations applied so subsequent commands have a schema.
	var count int
	require.NoError(t, ts.DB.QueryRow(
		`SELECT COUNT(*) FROM monitored_symbols WHERE enabled = true`).Scan(&count))
	require.Equal(t, 17, count)

	// backfill-only for specific symbols: hits the fake Alpaca bars endpoint.
	require.NoError(t, run(ctx, cliOptions{
		backfillOnly:    true,
		backfillSymbols: "SPY,QQQ",
		days:            30,
	}))

	// backfill-only for all monitored symbols.
	require.NoError(t, run(ctx, cliOptions{backfillOnly: true, days: 30}))

	// quality check for a specific symbol.
	require.NoError(t, run(ctx, cliOptions{
		qualityCheck:    true,
		backfillSymbols: "SPY",
		days:            7,
	}))

	// quality check across all monitored symbols.
	require.NoError(t, run(ctx, cliOptions{qualityCheck: true, days: 7}))

	// fill-gaps: detect and fill gaps over the window.
	require.NoError(t, run(ctx, cliOptions{fillGaps: true, days: 7}))

	// refresh continuous aggregates.
	require.NoError(t, run(ctx, cliOptions{refreshAggregates: true, days: 7}))

	// add-symbol then remove-symbol via run().
	require.NoError(t, run(ctx, cliOptions{addSymbol: "TEST:Test Co", days: 30}))
	require.NoError(t, ts.DB.QueryRow(
		`SELECT COUNT(*) FROM monitored_symbols WHERE symbol = 'TEST' AND enabled = true`).Scan(&count))
	require.Equal(t, 1, count)

	require.NoError(t, run(ctx, cliOptions{removeSymbol: "TEST", days: 30}))
	require.NoError(t, ts.DB.QueryRow(
		`SELECT COUNT(*) FROM monitored_symbols WHERE symbol = 'TEST' AND enabled = true`).Scan(&count))
	require.Equal(t, 0, count)
}

// TestRun_ConfigError verifies run() returns (not exits) when configuration is
// invalid — here, the required Alpaca credentials are missing.
func TestRun_ConfigError(t *testing.T) {
	// Ensure required env is absent so config.Load fails fast, before any
	// container or network access.
	t.Setenv("ALPACA_API_KEY_ID", "")
	t.Setenv("ALPACA_API_SECRET_KEY", "")
	// Unique ports so the health/metrics servers do not clash.
	t.Setenv("HEALTH_PORT", "18185")
	t.Setenv("METRICS_PORT", "18286")

	err := run(context.Background(), cliOptions{days: 30})
	require.Error(t, err)
	require.Contains(t, err.Error(), "configuration")
}

// splitHostPort splits a "host:port" address, returning host and port.
func splitHostPort(addr string) (string, string, error) {
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return addr, "", nil
	}
	return addr[:i], addr[i+1:], nil
}
