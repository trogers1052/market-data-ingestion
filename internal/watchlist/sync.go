package watchlist

import (
	"context"
	"log"

	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/kafka"
	"github.com/trogers1052/market-data-ingestion/internal/polygon"
	redisclient "github.com/trogers1052/market-data-ingestion/internal/redis"
)

// SyncService handles watchlist synchronization between Redis, Kafka, and the database
type SyncService struct {
	repo          *database.Repository
	redisClient   *redisclient.Client
	polygonClient *polygon.Client
	consumer      *kafka.WatchlistConsumer
	backfillCh    chan string // Channel to trigger backfill for new symbols
}

// NewSyncService creates a new watchlist sync service
func NewSyncService(
	repo *database.Repository,
	redisClient *redisclient.Client,
	polygonClient *polygon.Client,
) *SyncService {
	return &SyncService{
		repo:          repo,
		redisClient:   redisClient,
		polygonClient: polygonClient,
		backfillCh:    make(chan string, 100),
	}
}

// SetConsumer sets the Kafka consumer (needed for circular dependency resolution)
func (s *SyncService) SetConsumer(consumer *kafka.WatchlistConsumer) {
	s.consumer = consumer
}

// BackfillChannel returns the channel for symbols needing backfill
func (s *SyncService) BackfillChannel() <-chan string {
	return s.backfillCh
}

// SyncFromRedis reads the current watchlist from Redis and syncs to the database
func (s *SyncService) SyncFromRedis(ctx context.Context) ([]string, error) {
	log.Println("Syncing watchlist from Redis...")

	// Get all symbols from Redis
	symbols, err := s.redisClient.GetWatchlistSymbols(ctx)
	if err != nil {
		return nil, err
	}

	if len(symbols) == 0 {
		log.Println("No symbols found in Redis watchlist")
		return nil, nil
	}

	// Get details for all symbols
	details, err := s.redisClient.GetWatchlistDetails(ctx)
	if err != nil {
		log.Printf("Warning: could not get watchlist details: %v", err)
		details = make(map[string]*redisclient.WatchlistStock)
	}

	// Get currently monitored symbols
	currentSymbols, err := s.repo.GetMonitoredSymbols(ctx)
	if err != nil {
		return nil, err
	}

	currentSet := make(map[string]bool)
	for _, sym := range currentSymbols {
		currentSet[sym.Symbol] = true
	}

	// Add new symbols
	var addedSymbols []string
	for _, symbol := range symbols {
		if !currentSet[symbol] {
			name := symbol
			if detail, ok := details[symbol]; ok && detail.Name != "" {
				name = detail.Name
			}

			// Try to get name from Polygon if not in Redis
			if name == symbol {
				if tickerDetails, err := s.polygonClient.GetTickerDetails(ctx, symbol); err == nil && tickerDetails.Name != "" {
					name = tickerDetails.Name
				}
			}

			if err := s.repo.UpsertMonitoredSymbol(ctx, symbol, name, true); err != nil {
				log.Printf("Error adding symbol %s: %v", symbol, err)
				continue
			}

			log.Printf("Added symbol from Redis: %s (%s)", symbol, name)
			addedSymbols = append(addedSymbols, symbol)

			// Signal that this symbol needs backfill
			select {
			case s.backfillCh <- symbol:
			default:
				log.Printf("Backfill channel full, skipping signal for %s", symbol)
			}
		}
	}

	if len(addedSymbols) > 0 {
		log.Printf("Synced %d new symbols from Redis watchlist", len(addedSymbols))
	} else {
		log.Printf("Redis watchlist has %d symbols, all already monitored", len(symbols))
	}

	return addedSymbols, nil
}

// OnSymbolAdded handles a new symbol being added to the watchlist
func (s *SyncService) OnSymbolAdded(ctx context.Context, symbol, name string) error {
	log.Printf("Watchlist: Symbol added - %s (%s)", symbol, name)

	// Try to get better name from Polygon if the name is just the symbol
	if name == symbol || name == "" {
		if details, err := s.polygonClient.GetTickerDetails(ctx, symbol); err == nil && details.Name != "" {
			name = details.Name
		}
	}

	// Add to monitored symbols
	if err := s.repo.UpsertMonitoredSymbol(ctx, symbol, name, true); err != nil {
		return err
	}

	log.Printf("Added to monitored symbols: %s (%s)", symbol, name)

	// Signal that this symbol needs backfill
	select {
	case s.backfillCh <- symbol:
		log.Printf("Triggered backfill for new symbol: %s", symbol)
	default:
		log.Printf("Backfill channel full, skipping signal for %s", symbol)
	}

	return nil
}

// OnSymbolRemoved handles a symbol being removed from the watchlist
func (s *SyncService) OnSymbolRemoved(ctx context.Context, symbol string) error {
	log.Printf("Watchlist: Symbol removed - %s", symbol)

	// Disable the symbol (don't delete, keep the data)
	if err := s.repo.UpsertMonitoredSymbol(ctx, symbol, "", false); err != nil {
		return err
	}

	log.Printf("Disabled monitored symbol: %s", symbol)
	return nil
}

// StartConsumer starts the Kafka consumer for watchlist events
func (s *SyncService) StartConsumer(ctx context.Context) error {
	if s.consumer == nil {
		return nil
	}
	return s.consumer.Start(ctx)
}

// Close cleans up resources
func (s *SyncService) Close() error {
	close(s.backfillCh)
	if s.consumer != nil {
		return s.consumer.Close()
	}
	return nil
}
