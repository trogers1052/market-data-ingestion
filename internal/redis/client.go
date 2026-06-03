package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// WatchlistKey is the Redis key for the watchlist symbols set
	WatchlistKey = "trading:watchlist"
	// WatchlistDetailsKey is the Redis key for the watchlist details hash
	WatchlistDetailsKey = "trading:watchlist:details"

	// Data freshness keys
	IngestionStatusKey       = "ingestion:status"  // Overall service status
	SymbolFreshnessKeyPrefix = "ingestion:symbol:" // Per-symbol freshness (suffix: {SYMBOL}:freshness)
	SymbolFreshnessTTL       = 300                 // 5 minutes TTL for freshness data

	// Backfill tracking keys (persistent, no TTL)
	BackfillCompletedSetKey    = "backfill:completed" // Set of symbols with completed backfill
	BackfillCompletedKeyPrefix = "backfill:symbol:"   // Per-symbol completion info (suffix: {SYMBOL})
)

// WatchlistStock represents a stock from the watchlist
type WatchlistStock struct {
	Symbol        string `json:"symbol"`
	Name          string `json:"name"`
	InstrumentURL string `json:"instrument_url"`
	AddedAt       string `json:"added_at"`
}

// IngestionStatus represents the overall service status
type IngestionStatus struct {
	Status          string `json:"status"`           // "running", "stopped", "error"
	IsMarketHours   bool   `json:"is_market_hours"`  // Whether market is currently open
	LastHeartbeat   string `json:"last_heartbeat"`   // ISO timestamp of last heartbeat
	SymbolsTracked  int    `json:"symbols_tracked"`  // Number of symbols being tracked
	BarsReceived    int64  `json:"bars_received"`    // Total bars received in session
	BarsInserted    int64  `json:"bars_inserted"`    // Total bars inserted in session
	BackfillPending int    `json:"backfill_pending"` // Symbols waiting for backfill
	ErrorMessage    string `json:"error_message,omitempty"`
}

// SymbolFreshness represents data freshness for a specific symbol
type SymbolFreshness struct {
	Symbol          string  `json:"symbol"`
	Status          string  `json:"status"`           // "current", "stale", "backfilling", "error", "no_data"
	LastBarTime     string  `json:"last_bar_time"`    // ISO timestamp of most recent bar
	BarCount        int64   `json:"bar_count"`        // Total bars available
	BackfillStatus  string  `json:"backfill_status"`  // "completed", "in_progress", "pending", "failed"
	BackfillStart   string  `json:"backfill_start"`   // Earliest data available
	BackfillEnd     string  `json:"backfill_end"`     // Latest backfill data
	CoveragePercent float64 `json:"coverage_percent"` // Data coverage percentage
	GapsDetected    int     `json:"gaps_detected"`    // Number of gaps in data
	LastUpdated     string  `json:"last_updated"`     // When this status was updated
	MinutesStale    int     `json:"minutes_stale"`    // Minutes since last bar (0 = current)
	IsReady         bool    `json:"is_ready"`         // True if data is ready for analytics
}

// Client wraps the Redis client for watchlist operations
type Client struct {
	client *redis.Client
}

// NewClient creates a new Redis client
func NewClient(addr, password string, db int) (*Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
		MinIdleConns: 2,
	})

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Printf("Connected to Redis at %s", addr)
	return &Client{client: client}, nil
}

// NewClientWithRedis wraps an existing *redis.Client. It performs no ping,
// making it suitable for tests backed by an in-memory Redis (e.g. miniredis).
func NewClientWithRedis(client *redis.Client) *Client {
	return &Client{client: client}
}

// Close closes the Redis connection
func (c *Client) Close() error {
	return c.client.Close()
}

// GetWatchlistSymbols returns all symbols in the watchlist
func (c *Client) GetWatchlistSymbols(ctx context.Context) ([]string, error) {
	symbols, err := c.client.SMembers(ctx, WatchlistKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get watchlist symbols: %w", err)
	}
	return symbols, nil
}

// GetWatchlistDetails returns detailed info for all watchlist symbols
func (c *Client) GetWatchlistDetails(ctx context.Context) (map[string]*WatchlistStock, error) {
	data, err := c.client.HGetAll(ctx, WatchlistDetailsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get watchlist details: %w", err)
	}

	result := make(map[string]*WatchlistStock)
	for symbol, jsonStr := range data {
		var stock WatchlistStock
		if err := json.Unmarshal([]byte(jsonStr), &stock); err != nil {
			log.Printf("Warning: failed to parse watchlist details for %s: %v", symbol, err)
			continue
		}
		result[symbol] = &stock
	}

	return result, nil
}

// GetStockDetails returns details for a specific symbol
func (c *Client) GetStockDetails(ctx context.Context, symbol string) (*WatchlistStock, error) {
	data, err := c.client.HGet(ctx, WatchlistDetailsKey, symbol).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get stock details: %w", err)
	}

	var stock WatchlistStock
	if err := json.Unmarshal([]byte(data), &stock); err != nil {
		return nil, fmt.Errorf("failed to parse stock details: %w", err)
	}

	return &stock, nil
}

// SymbolExists checks if a symbol is in the watchlist
func (c *Client) SymbolExists(ctx context.Context, symbol string) (bool, error) {
	exists, err := c.client.SIsMember(ctx, WatchlistKey, symbol).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check symbol: %w", err)
	}
	return exists, nil
}

// WatchlistCount returns the number of symbols in the watchlist
func (c *Client) WatchlistCount(ctx context.Context) (int64, error) {
	count, err := c.client.SCard(ctx, WatchlistKey).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to count watchlist: %w", err)
	}
	return count, nil
}

// ============================================================================
// Data Freshness Tracking Methods
// ============================================================================

// UpdateIngestionStatus updates the overall service status in Redis
func (c *Client) UpdateIngestionStatus(ctx context.Context, status *IngestionStatus) error {
	data, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("failed to marshal ingestion status: %w", err)
	}

	// Set with 5-minute TTL - if service stops, status expires
	err = c.client.Set(ctx, IngestionStatusKey, data, SymbolFreshnessTTL*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set ingestion status: %w", err)
	}

	return nil
}

// GetIngestionStatus retrieves the overall service status from Redis
func (c *Client) GetIngestionStatus(ctx context.Context) (*IngestionStatus, error) {
	data, err := c.client.Get(ctx, IngestionStatusKey).Result()
	if err == redis.Nil {
		return nil, nil // No status set
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get ingestion status: %w", err)
	}

	var status IngestionStatus
	if err := json.Unmarshal([]byte(data), &status); err != nil {
		return nil, fmt.Errorf("failed to parse ingestion status: %w", err)
	}

	return &status, nil
}

// UpdateSymbolFreshness updates the data freshness status for a specific symbol
func (c *Client) UpdateSymbolFreshness(ctx context.Context, freshness *SymbolFreshness) error {
	data, err := json.Marshal(freshness)
	if err != nil {
		return fmt.Errorf("failed to marshal symbol freshness: %w", err)
	}

	key := SymbolFreshnessKeyPrefix + freshness.Symbol + ":freshness"
	err = c.client.Set(ctx, key, data, SymbolFreshnessTTL*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set symbol freshness for %s: %w", freshness.Symbol, err)
	}

	return nil
}

// GetSymbolFreshness retrieves the data freshness status for a specific symbol
func (c *Client) GetSymbolFreshness(ctx context.Context, symbol string) (*SymbolFreshness, error) {
	key := SymbolFreshnessKeyPrefix + symbol + ":freshness"
	data, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil // No freshness data
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get symbol freshness for %s: %w", symbol, err)
	}

	var freshness SymbolFreshness
	if err := json.Unmarshal([]byte(data), &freshness); err != nil {
		return nil, fmt.Errorf("failed to parse symbol freshness: %w", err)
	}

	return &freshness, nil
}

// GetAllSymbolFreshness retrieves freshness data for all tracked symbols
func (c *Client) GetAllSymbolFreshness(ctx context.Context) (map[string]*SymbolFreshness, error) {
	// Get all keys matching the pattern
	pattern := SymbolFreshnessKeyPrefix + "*:freshness"
	keys, err := c.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get freshness keys: %w", err)
	}

	result := make(map[string]*SymbolFreshness)
	for _, key := range keys {
		data, err := c.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var freshness SymbolFreshness
		if err := json.Unmarshal([]byte(data), &freshness); err != nil {
			continue
		}
		result[freshness.Symbol] = &freshness
	}

	return result, nil
}

// IsSymbolDataReady checks if a symbol's data is ready for analytics
func (c *Client) IsSymbolDataReady(ctx context.Context, symbol string) (bool, string, error) {
	freshness, err := c.GetSymbolFreshness(ctx, symbol)
	if err != nil {
		return false, "", err
	}

	if freshness == nil {
		return false, "no freshness data available", nil
	}

	if !freshness.IsReady {
		return false, fmt.Sprintf("data not ready: status=%s, backfill=%s",
			freshness.Status, freshness.BackfillStatus), nil
	}

	return true, "", nil
}

// ============================================================================
// Backfill Completion Tracking Methods
// ============================================================================

// BackfillCompletion represents backfill completion info for a symbol
type BackfillCompletion struct {
	Symbol        string `json:"symbol"`
	CompletedAt   string `json:"completed_at"`   // ISO timestamp
	BackfillStart string `json:"backfill_start"` // Earliest data date
	BackfillEnd   string `json:"backfill_end"`   // Latest data date
	TotalBars     int64  `json:"total_bars"`     // Number of bars backfilled
	Months        int    `json:"months"`         // Months of data requested
}

// MarkBackfillComplete marks a symbol's backfill as complete in Redis
func (c *Client) MarkBackfillComplete(ctx context.Context, completion *BackfillCompletion) error {
	// Add to completed set
	if err := c.client.SAdd(ctx, BackfillCompletedSetKey, completion.Symbol).Err(); err != nil {
		return fmt.Errorf("failed to add symbol to completed set: %w", err)
	}

	// Store completion details
	data, err := json.Marshal(completion)
	if err != nil {
		return fmt.Errorf("failed to marshal completion data: %w", err)
	}

	key := BackfillCompletedKeyPrefix + completion.Symbol
	if err := c.client.Set(ctx, key, data, 0).Err(); err != nil { // No TTL - persistent
		return fmt.Errorf("failed to set backfill completion for %s: %w", completion.Symbol, err)
	}

	log.Printf("Marked backfill complete in Redis for %s", completion.Symbol)
	return nil
}

// IsBackfillComplete checks if a symbol's backfill is marked complete in Redis
func (c *Client) IsBackfillComplete(ctx context.Context, symbol string) (bool, error) {
	exists, err := c.client.SIsMember(ctx, BackfillCompletedSetKey, symbol).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check backfill completion for %s: %w", symbol, err)
	}
	return exists, nil
}

// GetBackfillCompletion retrieves backfill completion info for a symbol
func (c *Client) GetBackfillCompletion(ctx context.Context, symbol string) (*BackfillCompletion, error) {
	key := BackfillCompletedKeyPrefix + symbol
	data, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil // Not completed
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get backfill completion for %s: %w", symbol, err)
	}

	var completion BackfillCompletion
	if err := json.Unmarshal([]byte(data), &completion); err != nil {
		return nil, fmt.Errorf("failed to parse backfill completion for %s: %w", symbol, err)
	}

	return &completion, nil
}

// GetCompletedBackfills returns all symbols that have completed backfill
func (c *Client) GetCompletedBackfills(ctx context.Context) ([]string, error) {
	symbols, err := c.client.SMembers(ctx, BackfillCompletedSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get completed backfills: %w", err)
	}
	return symbols, nil
}

// GetAllBackfillCompletions returns completion info for all completed symbols
func (c *Client) GetAllBackfillCompletions(ctx context.Context) (map[string]*BackfillCompletion, error) {
	symbols, err := c.GetCompletedBackfills(ctx)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*BackfillCompletion)
	for _, symbol := range symbols {
		completion, err := c.GetBackfillCompletion(ctx, symbol)
		if err != nil {
			log.Printf("Warning: failed to get completion for %s: %v", symbol, err)
			continue
		}
		if completion != nil {
			result[symbol] = completion
		}
	}

	return result, nil
}

// ClearBackfillComplete removes a symbol from the completed set (for re-backfill)
func (c *Client) ClearBackfillComplete(ctx context.Context, symbol string) error {
	// Remove from set
	if err := c.client.SRem(ctx, BackfillCompletedSetKey, symbol).Err(); err != nil {
		return fmt.Errorf("failed to remove symbol from completed set: %w", err)
	}

	// Delete completion details
	key := BackfillCompletedKeyPrefix + symbol
	if err := c.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete backfill completion for %s: %w", symbol, err)
	}

	log.Printf("Cleared backfill completion in Redis for %s", symbol)
	return nil
}

// ClearAllBackfillComplete clears all backfill completion records (for full re-backfill)
func (c *Client) ClearAllBackfillComplete(ctx context.Context) error {
	// Get all completed symbols first
	symbols, err := c.GetCompletedBackfills(ctx)
	if err != nil {
		return err
	}

	// Delete each symbol's completion record
	for _, symbol := range symbols {
		key := BackfillCompletedKeyPrefix + symbol
		c.client.Del(ctx, key)
	}

	// Delete the set
	if err := c.client.Del(ctx, BackfillCompletedSetKey).Err(); err != nil {
		return fmt.Errorf("failed to delete completed set: %w", err)
	}

	log.Printf("Cleared all backfill completion records in Redis (%d symbols)", len(symbols))
	return nil
}
