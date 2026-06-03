package ingestion

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/kafka"
	"github.com/trogers1052/market-data-ingestion/internal/marketdata"
	"github.com/trogers1052/market-data-ingestion/internal/metrics"
	"github.com/trogers1052/market-data-ingestion/internal/models"
)

// PollingService handles market data ingestion via REST API polling
type PollingService struct {
	mdClient      marketdata.Client
	repo          *database.Repository
	scheduler     *MarketScheduler
	kafkaProducer *kafka.Producer

	// Configuration
	pollInterval time.Duration
	delayMinutes int // How far behind real-time to fetch (e.g., 15 for delayed subscriptions)

	// Stats
	barsReceived int64
	barsInserted int64
	pollCount    int64
	statsMu      sync.Mutex

	// Tracking last fetched time per symbol to avoid duplicates
	lastFetched   map[string]time.Time
	lastFetchedMu sync.Mutex
}

// NewPollingService creates a new REST API polling service
func NewPollingService(
	mdClient marketdata.Client,
	repo *database.Repository,
	scheduler *MarketScheduler,
	kafkaProducer *kafka.Producer,
	pollIntervalSeconds int,
	delayMinutes int,
) *PollingService {
	return &PollingService{
		mdClient:      mdClient,
		repo:          repo,
		scheduler:     scheduler,
		kafkaProducer: kafkaProducer,
		pollInterval:  time.Duration(pollIntervalSeconds) * time.Second,
		delayMinutes:  delayMinutes,
		lastFetched:   make(map[string]time.Time),
	}
}

// Start begins REST API polling for market data
func (s *PollingService) Start(ctx context.Context) error {
	log.Printf("Starting polling service (interval: %v, delay: %d minutes)",
		s.pollInterval, s.delayMinutes)

	// Get monitored symbols
	symbols, err := s.repo.GetMonitoredSymbols(ctx)
	if err != nil {
		return err
	}

	symbolList := make([]string, len(symbols))
	for i, sym := range symbols {
		symbolList[i] = sym.Symbol
	}

	if len(symbolList) == 0 {
		log.Println("Warning: No monitored symbols found")
		return nil
	}

	log.Printf("Polling %d symbols: %v", len(symbolList), symbolList)
	metrics.SymbolsMonitored.Set(float64(len(symbolList)))

	// Create poll ticker
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	// Do an initial poll immediately
	s.pollAllSymbols(ctx, symbolList)

	// Main polling loop
	for {
		select {
		case <-ctx.Done():
			s.logStats()
			return nil

		case <-ticker.C:
			// Only poll during market hours
			if !s.scheduler.IsMarketHours() {
				sleepDuration := s.scheduler.GetSleepDuration()
				log.Printf("Market closed. Next poll when market opens (%.1f hours)",
					sleepDuration.Hours())

				// Wait for market to open
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(sleepDuration):
					// Refresh symbol list after market reopens
					symbols, err := s.repo.GetMonitoredSymbols(ctx)
					if err == nil {
						symbolList = make([]string, len(symbols))
						for i, sym := range symbols {
							symbolList[i] = sym.Symbol
						}
						metrics.SymbolsMonitored.Set(float64(len(symbolList)))
					}
				}
				continue
			}

			s.pollAllSymbols(ctx, symbolList)
		}
	}
}

// pollAllSymbols fetches the latest delayed data for all symbols
func (s *PollingService) pollAllSymbols(ctx context.Context, symbols []string) {
	s.statsMu.Lock()
	s.pollCount++
	pollNum := s.pollCount
	s.statsMu.Unlock()

	log.Printf("Poll #%d: Fetching data for %d symbols (delay: %d min)",
		pollNum, len(symbols), s.delayMinutes)

	var totalBars int
	var totalInserted int

	for _, symbol := range symbols {
		select {
		case <-ctx.Done():
			return
		default:
		}

		bars, inserted, err := s.pollSymbol(ctx, symbol)
		if err != nil {
			log.Printf("Error polling %s: %v", symbol, err)
			continue
		}

		totalBars += bars
		totalInserted += inserted
	}

	s.statsMu.Lock()
	s.barsReceived += int64(totalBars)
	s.barsInserted += int64(totalInserted)
	s.statsMu.Unlock()

	if totalInserted > 0 {
		log.Printf("Poll #%d complete: fetched %d bars, inserted %d new bars",
			pollNum, totalBars, totalInserted)
	} else {
		log.Printf("Poll #%d complete: no new bars", pollNum)
	}
}

// pollSymbol fetches delayed data for a single symbol
func (s *PollingService) pollSymbol(ctx context.Context, symbol string) (fetched int, inserted int, err error) {
	now := time.Now()

	// Calculate the time range to fetch
	// We want data from (delay + 2 minutes) ago to (delay) ago
	// This gives us a 2-minute window to catch any bars we might have missed
	delayDuration := time.Duration(s.delayMinutes) * time.Minute
	windowEnd := now.Add(-delayDuration)
	windowStart := windowEnd.Add(-2 * time.Minute)

	// Check if we've already fetched this data
	s.lastFetchedMu.Lock()
	lastTime, exists := s.lastFetched[symbol]
	if exists && !lastTime.Before(windowEnd) {
		s.lastFetchedMu.Unlock()
		return 0, 0, nil // Already fetched up to this point
	}

	// If we have a last fetch time, start from there to avoid gaps
	if exists && lastTime.After(windowStart) {
		windowStart = lastTime
	}
	s.lastFetchedMu.Unlock()

	// Fetch bars from market data provider
	bars, err := s.mdClient.GetMinuteBars(ctx, symbol, windowStart, windowEnd)
	if err != nil {
		return 0, 0, err
	}

	if len(bars) == 0 {
		return 0, 0, nil
	}

	// Record quotes ingested from Alpaca
	metrics.QuotesIngested.WithLabelValues(symbol).Add(float64(len(bars)))
	metrics.LastQuoteTimestamp.WithLabelValues(symbol).SetToCurrentTime()

	// Filter out bars we've already processed.
	// Re-read lastFetched under lock — another goroutine (e.g. UpdateSymbols)
	// may have modified the map while we were fetching bars above.
	var newBars []models.OHLCV
	s.lastFetchedMu.Lock()
	lastTime, exists = s.lastFetched[symbol]
	for _, bar := range bars {
		if !exists || bar.Time.After(lastTime) {
			newBars = append(newBars, bar)
		}
	}

	// Update last fetched time — use lastTime (already read above) to avoid
	// redundant map lookup.
	if len(bars) > 0 {
		latestTime := bars[len(bars)-1].Time
		if !exists || latestTime.After(lastTime) {
			s.lastFetched[symbol] = latestTime
		}
	}
	s.lastFetchedMu.Unlock()

	if len(newBars) == 0 {
		return len(bars), 0, nil
	}

	// Validate bars before insert — reject corrupted data at the boundary.
	validBars := make([]models.OHLCV, 0, len(newBars))
	for i := range newBars {
		if err := newBars[i].Validate(); err != nil {
			log.Printf("Rejecting invalid bar for %s: %v", symbol, err)
			continue
		}
		validBars = append(validBars, newBars[i])
	}
	if len(validBars) == 0 {
		return len(bars), 0, nil
	}

	// Insert to database
	if err := s.repo.InsertOHLCVBatch(ctx, validBars); err != nil {
		return len(bars), 0, err
	}

	// Publish to Kafka
	if s.kafkaProducer != nil {
		if err := s.kafkaProducer.PublishQuotesBatch(ctx, validBars, false); err != nil {
			log.Printf("Warning: failed to publish %s bars to Kafka: %v", symbol, err)
		}
	}

	return len(bars), len(validBars), nil
}

// logStats logs polling statistics
func (s *PollingService) logStats() {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()

	log.Printf("Polling stats: polls=%d, bars_received=%d, bars_inserted=%d",
		s.pollCount, s.barsReceived, s.barsInserted)
}

// GetStats returns current polling statistics
func (s *PollingService) GetStats() map[string]int64 {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()

	return map[string]int64{
		"poll_count":    s.pollCount,
		"bars_received": s.barsReceived,
		"bars_inserted": s.barsInserted,
	}
}

// UpdateSymbols updates the list of monitored symbols
// Note: The polling service will pick up new symbols on the next poll
func (s *PollingService) UpdateSymbols(symbols []string) error {
	// Clear last fetched times for removed symbols
	s.lastFetchedMu.Lock()
	defer s.lastFetchedMu.Unlock()

	symbolSet := make(map[string]bool)
	for _, sym := range symbols {
		symbolSet[sym] = true
	}

	for sym := range s.lastFetched {
		if !symbolSet[sym] {
			delete(s.lastFetched, sym)
		}
	}

	return nil
}

// Stop stops the polling service
func (s *PollingService) Stop() {
	log.Println("Stopping polling service")
	s.logStats()
}
