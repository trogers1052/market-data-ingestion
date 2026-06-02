package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/joho/godotenv"
	"github.com/trogers1052/market-data-ingestion/internal/alpaca"
	"github.com/trogers1052/market-data-ingestion/internal/config"
	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/ingestion"
	"github.com/trogers1052/market-data-ingestion/internal/kafka"
	"github.com/trogers1052/market-data-ingestion/internal/metrics"
	"github.com/trogers1052/market-data-ingestion/internal/quality"
	redisclient "github.com/trogers1052/market-data-ingestion/internal/redis"
	"github.com/trogers1052/market-data-ingestion/internal/status"
	"github.com/trogers1052/market-data-ingestion/internal/symbols"
	"github.com/trogers1052/market-data-ingestion/internal/watchlist"
)

// cliOptions holds the parsed command-line flags that select the service mode.
// They are passed to run() so the bootstrap logic is testable without touching
// global flag state.
type cliOptions struct {
	backfillOnly      bool
	backfillSymbols   string
	addSymbol         string
	removeSymbol      string
	listSymbols       bool
	qualityCheck      bool
	fillGaps          bool
	refreshAggregates bool
	syncPositions     bool
	stockServiceDSN   string
	days              int
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Parse command line flags
	opts := cliOptions{}
	flag.BoolVar(&opts.backfillOnly, "backfill", false, "Run backfill only and exit")
	flag.StringVar(&opts.backfillSymbols, "symbols", "", "Comma-separated list of symbols (for -backfill, -quality, -fill-gaps)")
	flag.StringVar(&opts.addSymbol, "add-symbol", "", "Add a symbol to monitor (format: SYMBOL or SYMBOL:Name)")
	flag.StringVar(&opts.removeSymbol, "remove-symbol", "", "Remove a symbol from monitoring")
	flag.BoolVar(&opts.listSymbols, "list-symbols", false, "List all monitored symbols")
	flag.BoolVar(&opts.qualityCheck, "quality", false, "Run data quality check and exit")
	flag.BoolVar(&opts.fillGaps, "fill-gaps", false, "Detect and fill data gaps")
	flag.BoolVar(&opts.refreshAggregates, "refresh", false, "Refresh continuous aggregates (5min, 1hour, daily)")
	flag.BoolVar(&opts.syncPositions, "sync-positions", false, "Sync symbols from Stock-Service positions")
	flag.StringVar(&opts.stockServiceDSN, "stock-service-dsn", "", "Stock-Service database DSN for position sync")
	flag.IntVar(&opts.days, "days", 30, "Number of days to check for quality/gaps (default: 30)")
	flag.Parse()

	// Build a context that is cancelled on SIGINT/SIGTERM. This is the only
	// place that installs OS signal handling; run() treats context
	// cancellation as the shutdown trigger so it can be driven from tests too.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, opts); err != nil {
		log.Fatalf("%v", err)
	}
}

// run is the testable service bootstrap. It wires up the database, migrations,
// Alpaca client, Redis, Kafka and the polling/backfill goroutines, then blocks
// until either the polling service returns or the provided context is
// cancelled. On context cancellation it performs a graceful shutdown and
// returns nil. It returns a non-nil error only when bootstrap fails (or a
// one-shot command fails); long-running mode never returns an error on a clean
// context-driven shutdown.
//
// main() is a thin wrapper that parses flags, installs signal handling and
// calls run(); all behavior previously inline in main() lives here unchanged.
func run(ctx context.Context, opts cliOptions) error {
	log.Println("========================================")
	log.Println("Market Data Ingestion Service")
	log.Println("========================================")

	// Health endpoint — Docker/systemd HEALTHCHECK target
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8080"
	}
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok")) //nolint:errcheck
	})
	healthServer := &http.Server{
		Addr:         ":" + healthPort,
		Handler:      healthMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Health server error: %v", err)
		}
	}()
	log.Printf("Health endpoint: http://localhost:%s/health", healthPort)

	// Metrics endpoint — Prometheus scrape target
	startMetricsServer()

	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	log.Printf("Database: %s:%d/%s", cfg.DBHost, cfg.DBPort, cfg.DBName)
	log.Printf("Backfill months: %d", cfg.BackfillMonths)
	log.Printf("Backfill delay days: %d", cfg.BackfillDelayDays)
	log.Printf("Market hours: %d:00 - %d:00 ET", cfg.MarketOpenHour, cfg.MarketCloseHour)
	log.Printf("Data mode: REST API polling (every %ds, delay: %d min)",
		cfg.PollIntervalSeconds, cfg.PollingDelayMinutes)

	// Connect to database
	repo, err := database.NewRepository(cfg.DatabaseDSN())
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer repo.Close()
	log.Println("Connected to TimescaleDB")

	// Run migrations (requires URL format with postgres:// scheme)
	if err := runMigrations(cfg.DatabaseURL()); err != nil {
		return fmt.Errorf("failed to run database migrations: %w", err)
	}

	// Create Alpaca client (free IEX feed — real-time, no delay)
	alpacaClient := alpaca.NewClient(cfg.AlpacaKeyID, cfg.AlpacaSecretKey)
	log.Println("Alpaca client initialized (IEX feed)")

	// Create market scheduler
	scheduler := ingestion.NewMarketScheduler(
		cfg.MarketOpenHour,
		cfg.MarketCloseHour,
		cfg.EnablePreMarket,
		cfg.EnableAfterHours,
	)

	// Handle list-symbols command
	if opts.listSymbols {
		return listSymbolsCmd(ctx, repo)
	}

	// Handle add-symbol command
	if opts.addSymbol != "" {
		if _, _, err := addSymbolCmd(ctx, repo, alpacaClient, opts.addSymbol); err != nil {
			return err
		}
		return nil
	}

	// Handle remove-symbol command
	if opts.removeSymbol != "" {
		if _, err := removeSymbolCmd(ctx, repo, opts.removeSymbol); err != nil {
			return err
		}
		return nil
	}

	// Handle sync-positions command
	if opts.syncPositions {
		dsn, err := resolveStockServiceDSN(opts.stockServiceDSN)
		if err != nil {
			return err
		}

		posSource, err := symbols.NewStockServiceDB(dsn)
		if err != nil {
			return fmt.Errorf("failed to connect to Stock-Service: %w", err)
		}
		defer posSource.Close()

		if _, err := syncPositionsCmd(ctx, repo, posSource); err != nil {
			return err
		}
		return nil
	}

	// Handle quality check command
	if opts.qualityCheck {
		log.Printf("Running data quality check (last %d days)", opts.days)
		checker := quality.NewChecker(repo, alpacaClient, scheduler)
		return runQualityCheck(ctx, checker, parseSymbolList(opts.backfillSymbols), opts.days)
	}

	// Handle fill-gaps command
	if opts.fillGaps {
		log.Printf("Detecting and filling gaps (last %d days)", opts.days)
		checker := quality.NewChecker(repo, alpacaClient, scheduler)

		to := time.Now()
		from := to.AddDate(0, 0, -opts.days)

		if err := checker.AutoFill(ctx, from, to); err != nil {
			return fmt.Errorf("gap filling failed: %w", err)
		}
		log.Println("Gap filling complete")
		return nil
	}

	// Handle refresh aggregates command
	if opts.refreshAggregates {
		log.Printf("Refreshing continuous aggregates (last %d days)", opts.days)

		to := time.Now()
		from := to.AddDate(0, 0, -opts.days)

		if err := repo.RefreshContinuousAggregates(ctx, from, to); err != nil {
			return fmt.Errorf("refresh failed: %w", err)
		}
		log.Println("Continuous aggregates refreshed")
		return nil
	}

	scheduler.LogStatus()

	// Connect to Redis (used by watchlist sync and status manager)
	var redisClient *redisclient.Client
	log.Println("Connecting to Redis...")
	redisClient, err = redisclient.NewClient(cfg.RedisAddr(), cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Printf("Warning: Failed to connect to Redis: %v", err)
		log.Println("Continuing without Redis features...")
	}
	if redisClient != nil {
		defer redisClient.Close()
	}

	// Initialize watchlist sync if enabled
	var watchlistSyncService *watchlist.SyncService
	if cfg.WatchlistSyncEnabled && redisClient != nil {
		log.Println("Watchlist sync enabled...")

		// Create watchlist sync service
		watchlistSyncService = watchlist.NewSyncService(repo, redisClient, alpacaClient)

		// Create Kafka consumer
		consumer, err := kafka.NewWatchlistConsumer(
			cfg.KafkaBrokers,
			cfg.KafkaWatchlistTopic,
			cfg.KafkaConsumerGroup,
			watchlistSyncService,
		)
		if err != nil {
			log.Printf("Warning: Failed to create Kafka consumer: %v", err)
		} else {
			watchlistSyncService.SetConsumer(consumer)
			log.Printf("Kafka watchlist consumer ready for topic: %s", cfg.KafkaWatchlistTopic)
		}

		// Sync initial watchlist from Redis
		if _, err := watchlistSyncService.SyncFromRedis(ctx); err != nil {
			log.Printf("Warning: Failed to sync initial watchlist from Redis: %v", err)
		}
	}

	// Create Kafka producer for quote events
	var kafkaProducer *kafka.Producer
	if cfg.KafkaEnabled {
		producer, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaQuotesTopic, true)
		if err != nil {
			log.Printf("Warning: Failed to create Kafka producer: %v", err)
			log.Println("Continuing without Kafka publishing...")
		} else {
			kafkaProducer = producer
			defer kafkaProducer.Close()
			log.Printf("Kafka producer ready for topic: %s", cfg.KafkaQuotesTopic)
		}
	}

	// Create backfill service
	// BackfillDelayDays=1 means exclude today's data from REST API backfill (for delayed Polygon subscriptions)
	// WebSocket handles today's data with 15-minute delay
	backfillService := ingestion.NewBackfillService(alpacaClient, repo, cfg.BackfillMonths, kafkaProducer, cfg.BackfillDelayDays)

	// Handle backfill-only mode
	if opts.backfillOnly {
		log.Println("Running in backfill-only mode")

		syms := parseSymbolList(opts.backfillSymbols)

		if len(syms) > 0 {
			log.Printf("Backfilling specific symbols: %v", syms)
			if err := backfillService.BackfillSymbols(ctx, syms); err != nil {
				return fmt.Errorf("backfill failed: %w", err)
			}
		} else {
			log.Println("Backfilling all monitored symbols")
			if err := backfillService.BackfillAll(ctx); err != nil {
				return fmt.Errorf("backfill failed: %w", err)
			}
		}

		log.Println("Backfill complete")
		return nil
	}

	// Create polling service for REST API data ingestion
	pollingService := ingestion.NewPollingService(
		alpacaClient,
		repo,
		scheduler,
		kafkaProducer,
		cfg.PollIntervalSeconds,
		cfg.PollingDelayMinutes,
	)
	log.Println("Created polling service")

	// Create and start status manager (publishes data freshness to Redis)
	var statusManager *status.Manager
	if redisClient != nil {
		statusManager = status.NewManager(repo, redisClient, scheduler, pollingService)
		statusManager.Start(ctx)
		defer statusManager.Stop()
		log.Println("Status manager started - publishing data freshness to Redis")
	}

	// Derive a cancellable context so we can tear down all background
	// goroutines on shutdown, regardless of whether the trigger was the parent
	// context (signal) or an error from the polling service.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// WaitGroup to track background goroutines
	var wg sync.WaitGroup

	// Run initial backfill for any symbols that need it
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Checking for symbols needing backfill...")
		if err := backfillService.BackfillAll(runCtx); err != nil {
			log.Printf("Initial backfill had errors: %v", err)
		}
	}()

	// Start watchlist consumer if available
	if watchlistSyncService != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := watchlistSyncService.StartConsumer(runCtx); err != nil {
				log.Printf("Watchlist consumer error: %v", err)
			}
		}()

		// Process backfill requests for newly added symbols
		wg.Add(1)
		go func() {
			defer wg.Done()
			for symbol := range watchlistSyncService.BackfillChannel() {
				log.Printf("Processing backfill for new symbol: %s", symbol)
				if err := backfillService.BackfillSymbols(runCtx, []string{symbol}); err != nil {
					log.Printf("Backfill error for %s: %v", symbol, err)
				}
			}
		}()
	}

	// Start polling service in background
	wg.Add(1)
	errCh := make(chan error, 1)
	go func() {
		defer wg.Done()
		errCh <- pollingService.Start(runCtx)
	}()

	// Log monitored symbol count at startup for flow visibility
	startupSymbols, _ := repo.GetMonitoredSymbols(runCtx)
	metrics.SymbolsMonitored.Set(float64(len(startupSymbols)))
	log.Printf("========================================")
	log.Printf("Service started (Alpaca IEX real-time feed)")
	log.Printf("Monitoring %d symbols, polling every %ds", len(startupSymbols), cfg.PollIntervalSeconds)
	log.Printf("Kafka: %v, topic: %s", cfg.KafkaEnabled, cfg.KafkaQuotesTopic)
	log.Printf("========================================")

	// Wait for shutdown signal (context cancellation) or a polling error.
	select {
	case <-ctx.Done():
		log.Println("Shutdown signal received")
	case err := <-errCh:
		if err != nil {
			log.Printf("Service error: %v", err)
		}
	}

	// Graceful shutdown with timeout to prevent hanging forever
	const shutdownTimeout = 15 * time.Second
	log.Printf("Shutting down (timeout: %v)...", shutdownTimeout)

	// Cancel context to signal all goroutines to stop
	cancel()

	// Stop polling service (logs final stats)
	pollingService.Stop()

	// Close watchlist sync (closes backfill channel, which unblocks the
	// backfill-processor goroutine's range loop)
	if watchlistSyncService != nil {
		watchlistSyncService.Close()
	}

	// Shut down health server gracefully
	healthCtx, healthCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer healthCancel()
	if err := healthServer.Shutdown(healthCtx); err != nil {
		log.Printf("Health server shutdown error: %v", err)
	}

	// Wait for all background goroutines to finish, with a hard deadline
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All goroutines stopped cleanly")
	case <-time.After(shutdownTimeout):
		log.Println("WARNING: Shutdown timed out, some goroutines may still be running")
	}

	// Deferred cleanup runs after this: statusManager.Stop(), kafkaProducer.Close(),
	// redisClient.Close(), repo.Close()
	log.Println("Service stopped")
	return nil
}

// migrationsSourceURL is the golang-migrate source URL for the database
// migrations. It defaults to the container path baked into the Docker image and
// is only overridden by tests that need to point at migrations on a different
// filesystem path.
var migrationsSourceURL = "file:///db/migrations"

func runMigrations(databaseUrl string) error {
	return runMigrationsFromSource(migrationsSourceURL, databaseUrl)
}

// runMigrationsFromSource applies all pending migrations from the given source
// URL against the given database URL. It returns an error rather than exiting,
// so callers (and tests) can decide how to handle failures.
func runMigrationsFromSource(sourceURL, databaseUrl string) error {
	m, err := migrate.New(sourceURL, databaseUrl)
	if err != nil {
		return fmt.Errorf("could not create migrate instance: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to apply migrations: %w", err)
	} else if err == migrate.ErrNoChange {
		log.Println("No migrations to apply; database is up to date.")
	}

	return nil
}

// printQualityReport prints a data quality report
func printQualityReport(report *quality.DataQualityReport) {
	log.Println("----------------------------------------")
	log.Printf("Quality Report: %s", report.Symbol)
	log.Printf("  Checked at: %s", report.CheckedAt.Format(time.RFC3339))

	if report.DataRange != nil {
		log.Printf("  Data range: %s to %s",
			report.DataRange.Earliest.Format("2006-01-02"),
			report.DataRange.Latest.Format("2006-01-02"))
	}

	log.Printf("  Total bars: %d", report.TotalBars)
	log.Printf("  Expected bars: %d", report.ExpectedBars)
	log.Printf("  Coverage: %.1f%%", report.CoveragePercent)
	log.Printf("  Gaps found: %d", len(report.Gaps))

	if len(report.Gaps) > 0 && len(report.Gaps) <= 5 {
		for _, gap := range report.Gaps {
			log.Printf("    Gap: %s to %s",
				gap.StartTime.Format("2006-01-02 15:04"),
				gap.EndTime.Format("2006-01-02 15:04"))
		}
	}

	log.Printf("  Anomalies: %d", len(report.Anomalies))
	for _, anomaly := range report.Anomalies {
		log.Printf("    %s: %s", anomaly.Type, anomaly.Description)
	}
}
