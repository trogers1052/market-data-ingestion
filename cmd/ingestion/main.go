package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/joho/godotenv"
	"github.com/trogers1052/market-data-ingestion/internal/config"
	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/ingestion"
	"github.com/trogers1052/market-data-ingestion/internal/kafka"
	"github.com/trogers1052/market-data-ingestion/internal/polygon"
	"github.com/trogers1052/market-data-ingestion/internal/quality"
	redisclient "github.com/trogers1052/market-data-ingestion/internal/redis"
	"github.com/trogers1052/market-data-ingestion/internal/status"
	"github.com/trogers1052/market-data-ingestion/internal/symbols"
	"github.com/trogers1052/market-data-ingestion/internal/watchlist"
)

func main() {
	// Parse command line flags
	backfillOnly := flag.Bool("backfill", false, "Run backfill only and exit")
	backfillSymbols := flag.String("symbols", "", "Comma-separated list of symbols (for -backfill, -quality, -fill-gaps)")
	addSymbol := flag.String("add-symbol", "", "Add a symbol to monitor (format: SYMBOL or SYMBOL:Name)")
	removeSymbol := flag.String("remove-symbol", "", "Remove a symbol from monitoring")
	listSymbols := flag.Bool("list-symbols", false, "List all monitored symbols")
	qualityCheck := flag.Bool("quality", false, "Run data quality check and exit")
	fillGaps := flag.Bool("fill-gaps", false, "Detect and fill data gaps")
	refreshAggregates := flag.Bool("refresh", false, "Refresh continuous aggregates (5min, 1hour, daily)")
	syncPositions := flag.Bool("sync-positions", false, "Sync symbols from Stock-Service positions")
	stockServiceDSN := flag.String("stock-service-dsn", "", "Stock-Service database DSN for position sync")
	days := flag.Int("days", 30, "Number of days to check for quality/gaps (default: 30)")
	flag.Parse()

	log.Println("========================================")
	log.Println("Market Data Ingestion Service")
	log.Println("========================================")

	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Database: %s:%d/%s", cfg.DBHost, cfg.DBPort, cfg.DBName)
	log.Printf("Backfill months: %d", cfg.BackfillMonths)
	log.Printf("Backfill delay days: %d (excludes recent data from REST API for delayed subscriptions)", cfg.BackfillDelayDays)
	log.Printf("Market hours: %d:00 - %d:00 ET", cfg.MarketOpenHour, cfg.MarketCloseHour)
	if cfg.UsePollingMode {
		log.Printf("Data mode: REST API polling (every %ds, %d-min delay)",
			cfg.PollIntervalSeconds, cfg.PollingDelayMinutes)
	} else {
		log.Println("Data mode: WebSocket real-time (requires premium Polygon subscription)")
	}

	// Connect to database
	repo, err := database.NewRepository(cfg.DatabaseDSN())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer repo.Close()
	log.Println("Connected to TimescaleDB")

	// Run migrations (requires URL format with postgres:// scheme)
	if err := runMigrations(cfg.DatabaseURL()); err != nil {
		log.Fatalf("Failed to run database migrations: %v", err)
	}

	// Create Polygon client
	polygonClient := polygon.NewClient(cfg.PolygonAPIKey)
	log.Println("Polygon.io client initialized")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create market scheduler
	scheduler := ingestion.NewMarketScheduler(
		cfg.MarketOpenHour,
		cfg.MarketCloseHour,
		cfg.EnablePreMarket,
		cfg.EnableAfterHours,
	)

	// Handle list-symbols command
	if *listSymbols {
		syms, err := repo.GetMonitoredSymbols(ctx)
		if err != nil {
			log.Fatalf("Failed to get monitored symbols: %v", err)
		}
		log.Println("Monitored symbols:")
		for _, s := range syms {
			status := "enabled"
			if !s.Enabled {
				status = "disabled"
			}
			// Get bar count for this symbol
			count, _ := repo.GetBarCount(ctx, s.Symbol)
			log.Printf("  %s (%s) - %s [%d bars]", s.Symbol, s.Name, status, count)
		}
		return
	}

	// Handle add-symbol command
	if *addSymbol != "" {
		parts := strings.SplitN(*addSymbol, ":", 2)
		symbol := strings.ToUpper(parts[0])
		name := symbol
		if len(parts) > 1 {
			name = parts[1]
		}

		// Fetch ticker details from Polygon
		details, err := polygonClient.GetTickerDetails(ctx, symbol)
		if err != nil {
			log.Printf("Warning: could not fetch ticker details: %v", err)
		} else if details.Name != "" {
			name = details.Name
		}

		if err := repo.UpsertMonitoredSymbol(ctx, symbol, name, true); err != nil {
			log.Fatalf("Failed to add symbol: %v", err)
		}
		log.Printf("Added symbol: %s (%s)", symbol, name)
		return
	}

	// Handle remove-symbol command
	if *removeSymbol != "" {
		symbol := strings.ToUpper(*removeSymbol)
		if err := repo.UpsertMonitoredSymbol(ctx, symbol, "", false); err != nil {
			log.Fatalf("Failed to remove symbol: %v", err)
		}
		log.Printf("Disabled symbol: %s", symbol)
		return
	}

	// Handle sync-positions command
	if *syncPositions {
		dsn := *stockServiceDSN
		if dsn == "" {
			dsn = os.Getenv("STOCK_SERVICE_DSN")
		}
		if dsn == "" {
			log.Fatalf("Stock-Service DSN required. Use -stock-service-dsn or STOCK_SERVICE_DSN env var")
		}

		posSource, err := symbols.NewStockServiceDB(dsn)
		if err != nil {
			log.Fatalf("Failed to connect to Stock-Service: %v", err)
		}
		defer posSource.Close()

		syncService := symbols.NewSymbolSyncService(repo, posSource, 0)
		added, err := syncService.SyncFromPositions(ctx)
		if err != nil {
			log.Fatalf("Sync failed: %v", err)
		}

		if len(added) > 0 {
			log.Printf("Added %d symbols from positions: %v", len(added), added)
		} else {
			log.Println("No new symbols to add from positions")
		}
		return
	}

	// Handle quality check command
	if *qualityCheck {
		log.Printf("Running data quality check (last %d days)", *days)
		checker := quality.NewChecker(repo, polygonClient, scheduler)

		to := time.Now()
		from := to.AddDate(0, 0, -*days)

		var symbolList []string
		if *backfillSymbols != "" {
			symbolList = strings.Split(*backfillSymbols, ",")
			for i := range symbolList {
				symbolList[i] = strings.TrimSpace(strings.ToUpper(symbolList[i]))
			}
		}

		if len(symbolList) > 0 {
			for _, symbol := range symbolList {
				report, err := checker.CheckSymbol(ctx, symbol, from, to)
				if err != nil {
					log.Printf("Quality check failed for %s: %v", symbol, err)
					continue
				}
				printQualityReport(report)
			}
		} else {
			reports, err := checker.CheckAllSymbols(ctx, from, to)
			if err != nil {
				log.Fatalf("Quality check failed: %v", err)
			}
			for _, report := range reports {
				printQualityReport(report)
			}
		}
		return
	}

	// Handle fill-gaps command
	if *fillGaps {
		log.Printf("Detecting and filling gaps (last %d days)", *days)
		checker := quality.NewChecker(repo, polygonClient, scheduler)

		to := time.Now()
		from := to.AddDate(0, 0, -*days)

		if err := checker.AutoFill(ctx, from, to); err != nil {
			log.Fatalf("Gap filling failed: %v", err)
		}
		log.Println("Gap filling complete")
		return
	}

	// Handle refresh aggregates command
	if *refreshAggregates {
		log.Printf("Refreshing continuous aggregates (last %d days)", *days)

		to := time.Now()
		from := to.AddDate(0, 0, -*days)

		if err := repo.RefreshContinuousAggregates(ctx, from, to); err != nil {
			log.Fatalf("Refresh failed: %v", err)
		}
		log.Println("Continuous aggregates refreshed")
		return
	}

	scheduler.LogStatus()

	// Connect to Redis (used by watchlist sync and status manager)
	var redisClient *redisclient.Client
	log.Println("Connecting to Redis...")
	redisClient, err = redisclient.NewClient(cfg.RedisAddr(), cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Printf("Warning: Failed to connect to Redis: %v", err)
		log.Println("Continuing without Redis features...")
	} else {
		defer redisClient.Close()
	}

	// Initialize watchlist sync if enabled
	var watchlistSyncService *watchlist.SyncService
	if cfg.WatchlistSyncEnabled && redisClient != nil {
		log.Println("Watchlist sync enabled...")

		// Create watchlist sync service
		watchlistSyncService = watchlist.NewSyncService(repo, redisClient, polygonClient)

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
	backfillService := ingestion.NewBackfillService(polygonClient, repo, cfg.BackfillMonths, kafkaProducer, cfg.BackfillDelayDays)

	// Handle backfill-only mode
	if *backfillOnly {
		log.Println("Running in backfill-only mode")

		var syms []string
		if *backfillSymbols != "" {
			syms = strings.Split(*backfillSymbols, ",")
			for i := range syms {
				syms[i] = strings.TrimSpace(strings.ToUpper(syms[i]))
			}
		}

		if len(syms) > 0 {
			log.Printf("Backfilling specific symbols: %v", syms)
			if err := backfillService.BackfillSymbols(ctx, syms); err != nil {
				log.Fatalf("Backfill failed: %v", err)
			}
		} else {
			log.Println("Backfilling all monitored symbols")
			if err := backfillService.BackfillAll(ctx); err != nil {
				log.Fatalf("Backfill failed: %v", err)
			}
		}

		log.Println("Backfill complete")
		return
	}

	// Create ingestion service (polling or WebSocket depending on config)
	var realtimeService *ingestion.RealtimeService
	var pollingService *ingestion.PollingService

	if cfg.UsePollingMode {
		// Use REST API polling for delayed Polygon subscriptions
		pollingService = ingestion.NewPollingService(
			polygonClient,
			repo,
			scheduler,
			kafkaProducer,
			cfg.PollIntervalSeconds,
			cfg.PollingDelayMinutes,
		)
		log.Println("Created polling service for delayed data")
	} else {
		// Use WebSocket for real-time data (requires premium subscription)
		realtimeService = ingestion.NewRealtimeService(polygonClient, repo, scheduler, kafkaProducer)
		log.Println("Created WebSocket service for real-time data")
	}

	// Create and start status manager (publishes data freshness to Redis)
	var statusManager *status.Manager
	if redisClient != nil && realtimeService != nil {
		statusManager = status.NewManager(repo, redisClient, scheduler, realtimeService)
		statusManager.Start(ctx)
		defer statusManager.Stop()
		log.Println("Status manager started - publishing data freshness to Redis")
	}

	// Set up signal handling for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Run initial backfill for any symbols that need it
	go func() {
		log.Println("Checking for symbols needing backfill...")
		if err := backfillService.BackfillAll(ctx); err != nil {
			log.Printf("Initial backfill had errors: %v", err)
		}
	}()

	// Start watchlist consumer if available
	if watchlistSyncService != nil {
		go func() {
			if err := watchlistSyncService.StartConsumer(ctx); err != nil {
				log.Printf("Watchlist consumer error: %v", err)
			}
		}()

		// Process backfill requests for newly added symbols
		go func() {
			for symbol := range watchlistSyncService.BackfillChannel() {
				log.Printf("Processing backfill for new symbol: %s", symbol)
				if err := backfillService.BackfillSymbols(ctx, []string{symbol}); err != nil {
					log.Printf("Backfill error for %s: %v", symbol, err)
				}
			}
		}()
	}

	// Start ingestion service in background (polling or WebSocket)
	errCh := make(chan error, 1)
	go func() {
		if cfg.UsePollingMode {
			errCh <- pollingService.Start(ctx)
		} else {
			errCh <- realtimeService.Start(ctx)
		}
	}()

	log.Println("========================================")
	if cfg.UsePollingMode {
		log.Printf("Service started in POLLING mode (%d-min delay). Press Ctrl+C to stop.",
			cfg.PollingDelayMinutes)
	} else {
		log.Println("Service started in WebSocket mode. Press Ctrl+C to stop.")
	}
	log.Println("========================================")

	// Wait for shutdown signal or error
	select {
	case <-quit:
		log.Println("Shutdown signal received")
	case err := <-errCh:
		if err != nil {
			log.Printf("Service error: %v", err)
		}
	}

	// Graceful shutdown
	log.Println("Shutting down...")
	cancel()

	if cfg.UsePollingMode {
		pollingService.Stop()
	} else {
		realtimeService.Stop()
	}

	if watchlistSyncService != nil {
		watchlistSyncService.Close()
	}

	log.Println("Service stopped")
}

func runMigrations(databaseUrl string) error {
	// The "file://" prefix tells the migrate library to use the file driver
	// Use absolute path for Docker container compatibility
	m, err := migrate.New(
		"file:///db/migrations", // Absolute path to migrations directory in container
		databaseUrl)
	if err != nil {
		log.Fatalf("could not create migrate instance: %v", err)
	}

	// Apply all available migrations up to the latest version
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("failed to apply migrations: %v", err)
	}

	// If ErrNoChange is returned, it simply means the database was already current
	if err == migrate.ErrNoChange {
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
