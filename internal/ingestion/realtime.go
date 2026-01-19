package ingestion

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/models"
	"github.com/trogers1052/market-data-ingestion/internal/polygon"
)

// RealtimeService handles real-time market data ingestion via WebSocket
type RealtimeService struct {
	polygonClient *polygon.Client
	repo          *database.Repository
	scheduler     *MarketScheduler

	// Buffer for batching inserts
	barBuffer   []models.OHLCV
	bufferMu    sync.Mutex
	flushTicker *time.Ticker

	// Stats
	barsReceived int64
	barsInserted int64
	statsMu      sync.Mutex
}

// NewRealtimeService creates a new real-time ingestion service
func NewRealtimeService(
	polygonClient *polygon.Client,
	repo *database.Repository,
	scheduler *MarketScheduler,
) *RealtimeService {
	return &RealtimeService{
		polygonClient: polygonClient,
		repo:          repo,
		scheduler:     scheduler,
		barBuffer:     make([]models.OHLCV, 0, 100),
	}
}

// Start begins real-time data ingestion
func (s *RealtimeService) Start(ctx context.Context) error {
	log.Println("Starting real-time ingestion service")

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

	log.Printf("Monitoring %d symbols: %v", len(symbolList), symbolList)

	// Start flush ticker (flush buffer every 5 seconds)
	s.flushTicker = time.NewTicker(5 * time.Second)
	go s.flushLoop(ctx)

	// Main loop - only connect during market hours
	for {
		select {
		case <-ctx.Done():
			s.Stop()
			return nil
		default:
		}

		if !s.scheduler.IsMarketHours() {
			sleepDuration := s.scheduler.GetSleepDuration()
			log.Printf("Market closed. Sleeping until market opens (%.1f hours)",
				sleepDuration.Hours())

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(sleepDuration):
				continue
			}
		}

		// Market is open - connect to WebSocket
		log.Println("Market is open, connecting to WebSocket...")

		err := s.polygonClient.StartWebSocket(ctx, symbolList, s.handleBar)
		if err != nil {
			log.Printf("WebSocket connection failed: %v", err)
			// Wait before retrying
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(30 * time.Second):
				continue
			}
		}

		// Wait for market close or context cancellation
		for s.scheduler.IsMarketHours() {
			select {
			case <-ctx.Done():
				s.polygonClient.StopWebSocket()
				return nil
			case <-time.After(1 * time.Minute):
				// Log stats periodically
				s.logStats()
			}
		}

		// Market closed, disconnect
		log.Println("Market closed, disconnecting WebSocket")
		s.polygonClient.StopWebSocket()

		// Flush any remaining bars
		s.flushBuffer(ctx)
	}
}

// handleBar processes a single bar received from WebSocket
func (s *RealtimeService) handleBar(bar models.OHLCV) {
	s.bufferMu.Lock()
	defer s.bufferMu.Unlock()

	s.barBuffer = append(s.barBuffer, bar)

	s.statsMu.Lock()
	s.barsReceived++
	s.statsMu.Unlock()

	// Flush if buffer is large
	if len(s.barBuffer) >= 100 {
		go s.flushBuffer(context.Background())
	}
}

// flushLoop periodically flushes the bar buffer
func (s *RealtimeService) flushLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.flushTicker.C:
			s.flushBuffer(ctx)
		}
	}
}

// flushBuffer writes buffered bars to the database
func (s *RealtimeService) flushBuffer(ctx context.Context) {
	s.bufferMu.Lock()
	if len(s.barBuffer) == 0 {
		s.bufferMu.Unlock()
		return
	}

	// Copy buffer and reset
	bars := make([]models.OHLCV, len(s.barBuffer))
	copy(bars, s.barBuffer)
	s.barBuffer = s.barBuffer[:0]
	s.bufferMu.Unlock()

	// Insert bars
	if err := s.repo.InsertOHLCVBatch(ctx, bars); err != nil {
		log.Printf("Error flushing bars to database: %v", err)
		// Put bars back in buffer for retry
		s.bufferMu.Lock()
		s.barBuffer = append(bars, s.barBuffer...)
		s.bufferMu.Unlock()
		return
	}

	s.statsMu.Lock()
	s.barsInserted += int64(len(bars))
	s.statsMu.Unlock()
}

// logStats logs ingestion statistics
func (s *RealtimeService) logStats() {
	s.statsMu.Lock()
	received := s.barsReceived
	inserted := s.barsInserted
	s.statsMu.Unlock()

	s.bufferMu.Lock()
	buffered := len(s.barBuffer)
	s.bufferMu.Unlock()

	log.Printf("Stats: received=%d, inserted=%d, buffered=%d", received, inserted, buffered)
}

// Stop stops the real-time ingestion service
func (s *RealtimeService) Stop() {
	log.Println("Stopping real-time ingestion service")

	if s.flushTicker != nil {
		s.flushTicker.Stop()
	}

	s.polygonClient.StopWebSocket()

	// Final flush
	s.flushBuffer(context.Background())

	s.logStats()
}

// GetStats returns current ingestion statistics
func (s *RealtimeService) GetStats() map[string]int64 {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()

	return map[string]int64{
		"bars_received": s.barsReceived,
		"bars_inserted": s.barsInserted,
	}
}

// UpdateSymbols updates the list of monitored symbols
func (s *RealtimeService) UpdateSymbols(symbols []string) error {
	return s.polygonClient.UpdateSubscriptions(symbols)
}
