package ingestion

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/kafka"
	"github.com/trogers1052/market-data-ingestion/internal/models"
	"github.com/trogers1052/market-data-ingestion/internal/polygon"
)

// BackfillService handles historical data backfill from Polygon.io
type BackfillService struct {
	polygonClient *polygon.Client
	repo          *database.Repository
	months        int
	kafkaProducer *kafka.Producer
	delayDays     int // Number of days to exclude from REST API backfill (for delayed subscriptions)
}

// NewBackfillService creates a new backfill service
// delayDays should be set to 1 for delayed Polygon.io subscriptions (15-min delayed data)
// This prevents the DELAYED API error by only fetching data up to T-1
func NewBackfillService(polygonClient *polygon.Client, repo *database.Repository, months int, kafkaProducer *kafka.Producer, delayDays int) *BackfillService {
	if delayDays < 0 {
		delayDays = 0
	}
	return &BackfillService{
		polygonClient: polygonClient,
		repo:          repo,
		months:        months,
		kafkaProducer: kafkaProducer,
		delayDays:     delayDays,
	}
}

// BackfillSymbol fetches and stores historical data for a single symbol
func (s *BackfillService) BackfillSymbol(ctx context.Context, symbol string) error {
	log.Printf("Starting backfill for %s (%d months)", symbol, s.months)

	// Check existing data range
	minTime, maxTime, err := s.repo.GetDataRange(ctx, symbol)
	if err != nil {
		log.Printf("Warning: failed to get existing data range: %v", err)
	}

	// Calculate target date range
	// For delayed subscriptions, exclude recent days to avoid DELAYED API errors
	// WebSocket handles today's delayed data in real-time
	endDate := time.Now().AddDate(0, 0, -s.delayDays)
	startDate := time.Now().AddDate(0, -s.months, 0)

	// Determine what data we actually need to fetch
	var fetchStart, fetchEnd time.Time
	needsBackfill := false

	if minTime == nil || maxTime == nil {
		// No existing data, fetch everything
		fetchStart = startDate
		fetchEnd = endDate
		needsBackfill = true
		log.Printf("  No existing data found, will fetch from %s to %s",
			fetchStart.Format("2006-01-02"), fetchEnd.Format("2006-01-02"))
	} else {
		// Check if we need to extend backwards
		if minTime.After(startDate) {
			fetchStart = startDate
			fetchEnd = *minTime
			needsBackfill = true
			log.Printf("  Extending backwards: %s to %s",
				fetchStart.Format("2006-01-02"), fetchEnd.Format("2006-01-02"))
		}

		// Check if we need to extend forwards (catch up to now)
		// Only fetch if we're more than 1 day behind
		oneDayAgo := endDate.AddDate(0, 0, -1)
		if maxTime.Before(oneDayAgo) {
			if needsBackfill {
				// We already have a range, extend it
				fetchEnd = endDate
			} else {
				// Only need to extend forwards
				fetchStart = *maxTime
				fetchEnd = endDate
				needsBackfill = true
			}
			log.Printf("  Extending forwards: %s to %s",
				fetchStart.Format("2006-01-02"), fetchEnd.Format("2006-01-02"))
		}

		if !needsBackfill {
			log.Printf("  Data already up to date (range: %s to %s), skipping backfill",
				minTime.Format("2006-01-02"), maxTime.Format("2006-01-02"))
			// Update status to completed even though we didn't fetch
			if err := s.repo.UpdateBackfillStatus(ctx, symbol, models.BackfillStatusCompleted, minTime, maxTime, ""); err != nil {
				log.Printf("Warning: failed to update backfill status: %v", err)
			}
			return nil
		}
	}

	if !needsBackfill {
		return nil
	}

	// Update status to in_progress
	if err := s.repo.UpdateBackfillStatus(ctx, symbol, models.BackfillStatusInProgress, &fetchStart, &fetchEnd, ""); err != nil {
		log.Printf("Warning: failed to update backfill status: %v", err)
	}

	// Polygon API returns max 50,000 results per request
	// For 1-minute data, that's about 128 trading days (50000 / 390 bars per day)
	// So we need to chunk by month to be safe
	var totalBars int
	currentStart := fetchStart

	for currentStart.Before(fetchEnd) {
		// Chunk by 2 weeks to stay well under limit
		currentEnd := currentStart.AddDate(0, 0, 14)
		if currentEnd.After(fetchEnd) {
			currentEnd = fetchEnd
		}

		log.Printf("  Fetching %s: %s to %s",
			symbol,
			currentStart.Format("2006-01-02"),
			currentEnd.Format("2006-01-02"))

		bars, err := s.polygonClient.GetMinuteBars(ctx, symbol, currentStart, currentEnd)
		if err != nil {
			errMsg := fmt.Sprintf("failed to fetch data: %v", err)
			log.Printf("  Error: %s", errMsg)
			s.repo.UpdateBackfillStatus(ctx, symbol, models.BackfillStatusFailed, nil, nil, errMsg)
			return fmt.Errorf("backfill failed for %s: %w", symbol, err)
		}

		if len(bars) > 0 {
			// Insert bars in batches
			if err := s.repo.InsertOHLCVBatch(ctx, bars); err != nil {
				errMsg := fmt.Sprintf("failed to insert data: %v", err)
				log.Printf("  Error: %s", errMsg)
				s.repo.UpdateBackfillStatus(ctx, symbol, models.BackfillStatusFailed, nil, nil, errMsg)
				return fmt.Errorf("failed to insert bars for %s: %w", symbol, err)
			}

			// Publish to Kafka (non-blocking, log errors but don't fail)
			if s.kafkaProducer != nil {
				if err := s.kafkaProducer.PublishQuotesBatch(ctx, bars); err != nil {
					log.Printf("  Warning: failed to publish bars to Kafka: %v", err)
					// Don't fail the whole operation if Kafka fails
				}
			}

			totalBars += len(bars)
			log.Printf("  Inserted %d bars (total: %d)", len(bars), totalBars)
		}

		// Move to next chunk
		currentStart = currentEnd.AddDate(0, 0, 1)

		// Rate limiting: Polygon free tier is 5 requests/minute
		// Paid tiers are higher, but let's be conservative
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond): // 4 requests per second max
		}
	}

	// Update backfill status to completed
	if err := s.repo.UpdateBackfillStatus(ctx, symbol, models.BackfillStatusCompleted, &fetchStart, &fetchEnd, ""); err != nil {
		log.Printf("Warning: failed to update backfill status: %v", err)
	}

	log.Printf("Backfill complete for %s: %d total bars", symbol, totalBars)

	return nil
}

// BackfillAll fetches historical data for all monitored symbols
func (s *BackfillService) BackfillAll(ctx context.Context) error {
	// Get symbols needing backfill
	symbols, err := s.repo.GetSymbolsNeedingBackfill(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbols needing backfill: %w", err)
	}

	if len(symbols) == 0 {
		log.Println("No symbols need backfill")
		return nil
	}

	log.Printf("Starting backfill for %d symbols", len(symbols))

	var failed []string
	for _, symbol := range symbols {
		if err := s.BackfillSymbol(ctx, symbol); err != nil {
			log.Printf("Backfill failed for %s: %v", symbol, err)
			failed = append(failed, symbol)
			// Continue with other symbols
		}

		// Add delay between symbols to avoid rate limiting
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	if len(failed) > 0 {
		return fmt.Errorf("backfill failed for %d symbols: %v", len(failed), failed)
	}

	log.Printf("Backfill complete for all %d symbols", len(symbols))
	return nil
}

// BackfillSymbols fetches historical data for specific symbols
func (s *BackfillService) BackfillSymbols(ctx context.Context, symbols []string) error {
	log.Printf("Starting backfill for %d symbols", len(symbols))

	var failed []string
	for _, symbol := range symbols {
		if err := s.BackfillSymbol(ctx, symbol); err != nil {
			log.Printf("Backfill failed for %s: %v", symbol, err)
			failed = append(failed, symbol)
			continue
		}

		// Add delay between symbols
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	if len(failed) > 0 {
		return fmt.Errorf("backfill failed for %d symbols: %v", len(failed), failed)
	}

	return nil
}

// FillGaps detects and fills gaps in data for a symbol
func (s *BackfillService) FillGaps(ctx context.Context, symbol string) error {
	log.Printf("Checking for gaps in %s data", symbol)

	// Get current data range
	minTime, maxTime, err := s.repo.GetDataRange(ctx, symbol)
	if err != nil {
		return fmt.Errorf("failed to get data range: %w", err)
	}

	if minTime == nil || maxTime == nil {
		log.Printf("No existing data for %s, running full backfill", symbol)
		return s.BackfillSymbol(ctx, symbol)
	}

	// Check if we need to extend backwards
	targetStart := time.Now().AddDate(0, -s.months, 0)
	if minTime.After(targetStart) {
		log.Printf("Extending data backwards from %s to %s",
			minTime.Format("2006-01-02"),
			targetStart.Format("2006-01-02"))

		bars, err := s.polygonClient.GetMinuteBars(ctx, symbol, targetStart, *minTime)
		if err != nil {
			return fmt.Errorf("failed to fetch gap data: %w", err)
		}

		if len(bars) > 0 {
			if err := s.repo.InsertOHLCVBatch(ctx, bars); err != nil {
				return fmt.Errorf("failed to insert gap data: %w", err)
			}
			log.Printf("Filled %d bars for gap before %s", len(bars), minTime.Format("2006-01-02"))
		}
	}

	// Check if we need to extend forwards (catch up to allowed date)
	// For delayed subscriptions, only fetch up to T-delayDays to avoid DELAYED API errors
	cutoffDate := time.Now().AddDate(0, 0, -s.delayDays)
	checkDate := cutoffDate.AddDate(0, 0, -1)
	if maxTime.Before(checkDate) {
		log.Printf("Extending data forwards from %s to %s",
			maxTime.Format("2006-01-02"),
			cutoffDate.Format("2006-01-02"))

		bars, err := s.polygonClient.GetMinuteBars(ctx, symbol, *maxTime, cutoffDate)
		if err != nil {
			return fmt.Errorf("failed to fetch recent data: %w", err)
		}

		if len(bars) > 0 {
			if err := s.repo.InsertOHLCVBatch(ctx, bars); err != nil {
				return fmt.Errorf("failed to insert recent data: %w", err)
			}
			log.Printf("Filled %d bars for gap after %s", len(bars), maxTime.Format("2006-01-02"))
		}
	}

	return nil
}

// GetBackfillProgress returns progress info for all monitored symbols
func (s *BackfillService) GetBackfillProgress(ctx context.Context) (map[string]*models.BackfillStatus, error) {
	symbols, err := s.repo.GetMonitoredSymbols(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get monitored symbols: %w", err)
	}

	progress := make(map[string]*models.BackfillStatus)
	for _, sym := range symbols {
		status, err := s.repo.GetBackfillStatus(ctx, sym.Symbol)
		if err != nil {
			log.Printf("Warning: failed to get backfill status for %s: %v", sym.Symbol, err)
			continue
		}
		if status == nil {
			status = &models.BackfillStatus{
				Symbol: sym.Symbol,
				Status: models.BackfillStatusPending,
			}
		}
		progress[sym.Symbol] = status
	}

	return progress, nil
}
