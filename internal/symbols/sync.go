package symbols

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/trogers1052/market-data-ingestion/internal/database"
)

// PositionsSource defines the interface for fetching position symbols
type PositionsSource interface {
	GetPositionSymbols(ctx context.Context) ([]string, error)
}

// StockServiceDB fetches position symbols directly from Stock-Service database
type StockServiceDB struct {
	db *sql.DB
}

// NewStockServiceDB creates a new Stock-Service database connection
func NewStockServiceDB(dsn string) (*StockServiceDB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open stock-service database: %w", err)
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping stock-service database: %w", err)
	}

	return &StockServiceDB{db: db}, nil
}

// GetPositionSymbols fetches all symbols from the positions table
func (s *StockServiceDB) GetPositionSymbols(ctx context.Context) ([]string, error) {
	query := `SELECT DISTINCT symbol FROM positions WHERE quantity > 0 ORDER BY symbol`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query positions: %w", err)
	}
	defer rows.Close()

	var symbols []string
	for rows.Next() {
		var symbol string
		if err := rows.Scan(&symbol); err != nil {
			return nil, fmt.Errorf("failed to scan symbol: %w", err)
		}
		symbols = append(symbols, symbol)
	}

	return symbols, rows.Err()
}

// Close closes the database connection
func (s *StockServiceDB) Close() error {
	return s.db.Close()
}

// SymbolSyncService manages symbol synchronization from multiple sources
type SymbolSyncService struct {
	repo            *database.Repository
	positionsSource PositionsSource
	syncInterval    time.Duration
}

// NewSymbolSyncService creates a new symbol sync service
func NewSymbolSyncService(repo *database.Repository, positionsSource PositionsSource, syncInterval time.Duration) *SymbolSyncService {
	return &SymbolSyncService{
		repo:            repo,
		positionsSource: positionsSource,
		syncInterval:    syncInterval,
	}
}

// SyncFromPositions syncs symbols from the positions source
// Adds any new symbols found in positions and optionally enables them
func (s *SymbolSyncService) SyncFromPositions(ctx context.Context) (added []string, err error) {
	if s.positionsSource == nil {
		return nil, nil
	}

	// Get symbols from positions
	positionSymbols, err := s.positionsSource.GetPositionSymbols(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get position symbols: %w", err)
	}

	if len(positionSymbols) == 0 {
		log.Println("No position symbols found")
		return nil, nil
	}

	log.Printf("Found %d symbols in positions: %v", len(positionSymbols), positionSymbols)

	// Get current monitored symbols
	currentSymbols, err := s.repo.GetMonitoredSymbols(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get monitored symbols: %w", err)
	}

	currentMap := make(map[string]bool)
	for _, sym := range currentSymbols {
		currentMap[sym.Symbol] = true
	}

	// Add new symbols from positions
	for _, symbol := range positionSymbols {
		if !currentMap[symbol] {
			if err := s.repo.UpsertMonitoredSymbol(ctx, symbol, "", true); err != nil {
				log.Printf("Warning: failed to add symbol %s: %v", symbol, err)
				continue
			}
			added = append(added, symbol)
			log.Printf("Added symbol from positions: %s", symbol)
		}
	}

	if len(added) > 0 {
		log.Printf("Synced %d new symbols from positions", len(added))
	}

	return added, nil
}

// StartPeriodicSync starts periodic symbol synchronization
func (s *SymbolSyncService) StartPeriodicSync(ctx context.Context, onChange func([]string)) {
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()

	// Do initial sync
	if added, err := s.SyncFromPositions(ctx); err != nil {
		log.Printf("Initial position sync failed: %v", err)
	} else if len(added) > 0 && onChange != nil {
		onChange(added)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if added, err := s.SyncFromPositions(ctx); err != nil {
				log.Printf("Position sync failed: %v", err)
			} else if len(added) > 0 && onChange != nil {
				onChange(added)
			}
		}
	}
}

// GetAllSymbols returns all symbols from both monitored list and positions
func (s *SymbolSyncService) GetAllSymbols(ctx context.Context) ([]string, error) {
	// Get monitored symbols
	monitored, err := s.repo.GetMonitoredSymbols(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get monitored symbols: %w", err)
	}

	symbolSet := make(map[string]bool)
	for _, sym := range monitored {
		if sym.Enabled {
			symbolSet[sym.Symbol] = true
		}
	}

	// Add position symbols if available
	if s.positionsSource != nil {
		posSymbols, err := s.positionsSource.GetPositionSymbols(ctx)
		if err != nil {
			log.Printf("Warning: failed to get position symbols: %v", err)
		} else {
			for _, sym := range posSymbols {
				symbolSet[sym] = true
			}
		}
	}

	// Convert to slice
	symbols := make([]string, 0, len(symbolSet))
	for sym := range symbolSet {
		symbols = append(symbols, sym)
	}

	return symbols, nil
}

// AddWatchlistSymbols adds symbols to the watchlist
func (s *SymbolSyncService) AddWatchlistSymbols(ctx context.Context, symbols []string) error {
	for _, symbol := range symbols {
		if err := s.repo.UpsertMonitoredSymbol(ctx, symbol, "", true); err != nil {
			return fmt.Errorf("failed to add symbol %s: %w", symbol, err)
		}
	}
	return nil
}

// RemoveWatchlistSymbol removes a symbol from the watchlist
func (s *SymbolSyncService) RemoveWatchlistSymbol(ctx context.Context, symbol string) error {
	return s.repo.UpsertMonitoredSymbol(ctx, symbol, "", false)
}
