package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

const (
	// WatchlistKey is the Redis key for the watchlist symbols set
	WatchlistKey = "trading:watchlist"
	// WatchlistDetailsKey is the Redis key for the watchlist details hash
	WatchlistDetailsKey = "trading:watchlist:details"
)

// WatchlistStock represents a stock from the watchlist
type WatchlistStock struct {
	Symbol        string `json:"symbol"`
	Name          string `json:"name"`
	InstrumentURL string `json:"instrument_url"`
	AddedAt       string `json:"added_at"`
}

// Client wraps the Redis client for watchlist operations
type Client struct {
	client *redis.Client
}

// NewClient creates a new Redis client
func NewClient(addr, password string, db int) (*Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Printf("Connected to Redis at %s", addr)
	return &Client{client: client}, nil
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
