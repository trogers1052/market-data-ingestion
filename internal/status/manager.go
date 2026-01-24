package status

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/ingestion"
	"github.com/trogers1052/market-data-ingestion/internal/models"
	redisclient "github.com/trogers1052/market-data-ingestion/internal/redis"
)

// Manager handles data freshness status updates
type Manager struct {
	repo            *database.Repository
	redisClient     *redisclient.Client
	scheduler       *ingestion.MarketScheduler
	realtimeService *ingestion.RealtimeService

	updateInterval time.Duration
	stopCh         chan struct{}
	wg             sync.WaitGroup
}

// NewManager creates a new status manager
func NewManager(
	repo *database.Repository,
	redisClient *redisclient.Client,
	scheduler *ingestion.MarketScheduler,
	realtimeService *ingestion.RealtimeService,
) *Manager {
	return &Manager{
		repo:            repo,
		redisClient:     redisClient,
		scheduler:       scheduler,
		realtimeService: realtimeService,
		updateInterval:  30 * time.Second, // Update every 30 seconds
		stopCh:          make(chan struct{}),
	}
}

// Start begins the status update loop
func (m *Manager) Start(ctx context.Context) {
	m.wg.Add(1)
	go m.updateLoop(ctx)
	log.Println("Status manager started")
}

// Stop stops the status manager
func (m *Manager) Stop() {
	close(m.stopCh)
	m.wg.Wait()
	log.Println("Status manager stopped")
}

// updateLoop periodically updates status in Redis
func (m *Manager) updateLoop(ctx context.Context) {
	defer m.wg.Done()

	// Initial update
	m.updateAllStatus(ctx)

	ticker := time.NewTicker(m.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.updateAllStatus(ctx)
		}
	}
}

// updateAllStatus updates both service status and per-symbol freshness
func (m *Manager) updateAllStatus(ctx context.Context) {
	m.updateIngestionStatus(ctx)
	m.updateSymbolFreshness(ctx)
}

// updateIngestionStatus updates the overall service status
func (m *Manager) updateIngestionStatus(ctx context.Context) {
	if m.redisClient == nil {
		return
	}

	// Get stats from realtime service
	var barsReceived, barsInserted int64
	if m.realtimeService != nil {
		stats := m.realtimeService.GetStats()
		barsReceived = stats["bars_received"]
		barsInserted = stats["bars_inserted"]
	}

	// Get symbols count
	symbols, err := m.repo.GetMonitoredSymbols(ctx)
	symbolsCount := 0
	if err == nil {
		symbolsCount = len(symbols)
	}

	// Get pending backfills count
	pendingSymbols, err := m.repo.GetSymbolsNeedingBackfill(ctx)
	pendingCount := 0
	if err == nil {
		pendingCount = len(pendingSymbols)
	}

	status := &redisclient.IngestionStatus{
		Status:          "running",
		IsMarketHours:   m.scheduler.IsMarketHours(),
		LastHeartbeat:   time.Now().UTC().Format(time.RFC3339),
		SymbolsTracked:  symbolsCount,
		BarsReceived:    barsReceived,
		BarsInserted:    barsInserted,
		BackfillPending: pendingCount,
	}

	if err := m.redisClient.UpdateIngestionStatus(ctx, status); err != nil {
		log.Printf("Warning: failed to update ingestion status: %v", err)
	}
}

// updateSymbolFreshness updates freshness status for all monitored symbols
func (m *Manager) updateSymbolFreshness(ctx context.Context) {
	if m.redisClient == nil {
		return
	}

	symbols, err := m.repo.GetMonitoredSymbols(ctx)
	if err != nil {
		log.Printf("Warning: failed to get monitored symbols for freshness update: %v", err)
		return
	}

	for _, sym := range symbols {
		m.updateSingleSymbolFreshness(ctx, sym.Symbol)
	}
}

// updateSingleSymbolFreshness updates freshness for a single symbol
func (m *Manager) updateSingleSymbolFreshness(ctx context.Context, symbol string) {
	// Get backfill status
	backfillStatus, err := m.repo.GetBackfillStatus(ctx, symbol)
	if err != nil {
		log.Printf("Warning: failed to get backfill status for %s: %v", symbol, err)
		// Create empty status
		backfillStatus = &models.BackfillStatus{Symbol: symbol, Status: "pending"}
	}
	if backfillStatus == nil {
		backfillStatus = &models.BackfillStatus{Symbol: symbol, Status: "pending"}
	}

	// Get bar count
	barCount, err := m.repo.GetBarCount(ctx, symbol)
	if err != nil {
		barCount = 0
	}

	// Get latest bar time
	var latestBarTime time.Time
	latestBar, err := m.repo.GetLatestBar(ctx, symbol)
	if err == nil && latestBar != nil {
		latestBarTime = latestBar.Time
	}

	// Determine status
	status := m.determineSymbolStatus(backfillStatus, latestBarTime, barCount)

	// Calculate minutes stale
	minutesStale := 0
	if !latestBarTime.IsZero() {
		minutesStale = int(time.Since(latestBarTime).Minutes())
	}

	// Determine if data is ready for analytics
	isReady := m.isDataReady(backfillStatus, barCount, minutesStale)

	freshness := &redisclient.SymbolFreshness{
		Symbol:          symbol,
		Status:          status,
		LastBarTime:     formatTime(latestBarTime),
		BarCount:        barCount,
		BackfillStatus:  getBackfillStatusString(backfillStatus),
		BackfillStart:   formatTimePtr(backfillStatus.BackfillStart),
		BackfillEnd:     formatTimePtr(backfillStatus.BackfillEnd),
		CoveragePercent: 0, // TODO: Calculate from quality check
		GapsDetected:    0, // TODO: Get from quality check
		LastUpdated:     time.Now().UTC().Format(time.RFC3339),
		MinutesStale:    minutesStale,
		IsReady:         isReady,
	}

	if err := m.redisClient.UpdateSymbolFreshness(ctx, freshness); err != nil {
		log.Printf("Warning: failed to update freshness for %s: %v", symbol, err)
	}
}

// determineSymbolStatus determines the overall status for a symbol
func (m *Manager) determineSymbolStatus(
	backfillStatus *models.BackfillStatus,
	latestBarTime time.Time,
	barCount int64,
) string {
	if backfillStatus == nil || backfillStatus.Status == "" {
		return "no_data"
	}

	if backfillStatus.Status == models.BackfillStatusInProgress {
		return "backfilling"
	}

	if backfillStatus.Status == "failed" {
		return "error"
	}

	if barCount == 0 {
		return "no_data"
	}

	// If market is open and latest bar is > 5 minutes old, it's stale
	if m.scheduler.IsMarketHours() {
		if time.Since(latestBarTime) > 5*time.Minute {
			return "stale"
		}
	}

	return "current"
}

// isDataReady determines if data is ready for analytics calculations
func (m *Manager) isDataReady(
	backfillStatus *models.BackfillStatus,
	barCount int64,
	minutesStale int,
) bool {
	// Need backfill to be completed
	if backfillStatus == nil || backfillStatus.Status != models.BackfillStatusCompleted {
		return false
	}

	// Need minimum bars for SMA_200 calculation
	const minBarsRequired = 200
	if barCount < minBarsRequired {
		return false
	}

	// During market hours, data shouldn't be too stale
	if m.scheduler.IsMarketHours() && minutesStale > 10 {
		return false
	}

	return true
}

// Helper functions

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func formatTimePtr(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func getBackfillStatusString(bs *models.BackfillStatus) string {
	if bs == nil || bs.Status == "" {
		return "pending"
	}
	return bs.Status
}
