package config

import (
	"fmt"
	"strings"

	"github.com/trogers1052/trading-go-commons/env"
)

// Config holds all configuration for the market data ingestion service
type Config struct {
	// Alpaca API (free IEX feed — real-time, no delay)
	AlpacaKeyID     string
	AlpacaSecretKey string

	// Database (TimescaleDB)
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string

	// Kafka (optional - for publishing to other services)
	KafkaBrokers     []string
	KafkaQuotesTopic string // Changed from KafkaOHLCVTopic to match architecture
	KafkaEnabled     bool

	// Kafka watchlist consumer
	KafkaWatchlistTopic  string
	KafkaConsumerGroup   string
	WatchlistSyncEnabled bool

	// Redis (for reading shared watchlist)
	RedisHost     string
	RedisPort     int
	RedisPassword string
	RedisDB       int

	// Ingestion settings
	PollIntervalSeconds int
	BackfillMonths      int
	BackfillDelayDays   int  // Days to exclude from REST API backfill (1 for delayed subscriptions)
	MarketOpenHour      int  // ET timezone (e.g., 4 for pre-market, 9 for regular)
	MarketCloseHour     int  // ET timezone (e.g., 20 for after-hours, 16 for regular)
	EnablePreMarket     bool // Include pre-market hours (4am-9:30am ET)
	EnableAfterHours    bool // Include after-hours (4pm-8pm ET)

	// Polling settings
	PollingDelayMinutes int // How far behind real-time to poll (15 for delayed subscriptions)
}

// Load loads configuration from environment variables
func Load() (*Config, error) {
	cfg := &Config{
		// Alpaca (free IEX feed, real-time)
		AlpacaKeyID:     env.String("ALPACA_API_KEY_ID", ""),
		AlpacaSecretKey: env.String("ALPACA_API_SECRET_KEY", ""),

		// Database
		DBHost:     env.String("DB_HOST", "localhost"),
		DBPort:     env.Int("DB_PORT", 5432),
		DBUser:     env.String("DB_USER", "trader"),
		DBPassword: env.String("DB_PASSWORD", ""),
		DBName:     env.String("DB_NAME", "market_data"),
		DBSSLMode:  env.String("DB_SSL_MODE", "disable"),

		// Kafka
		// Use strings.Split (not env.StringSlice) to preserve exact split
		// semantics: empty entries are retained rather than dropped.
		KafkaBrokers:     strings.Split(env.String("KAFKA_BROKERS", "localhost:19092"), ","),
		KafkaQuotesTopic: env.String("KAFKA_QUOTES_TOPIC", "stock.quotes.realtime"),
		KafkaEnabled:     env.Bool("KAFKA_ENABLED", true), // Default to enabled

		// Kafka watchlist consumer
		KafkaWatchlistTopic:  env.String("KAFKA_WATCHLIST_TOPIC", "trading.watchlist"),
		KafkaConsumerGroup:   env.String("KAFKA_CONSUMER_GROUP", "market-data-ingestion"),
		WatchlistSyncEnabled: env.Bool("WATCHLIST_SYNC_ENABLED", true),

		// Redis
		RedisHost:     env.String("REDIS_HOST", "localhost"),
		RedisPort:     env.Int("REDIS_PORT", 6379),
		RedisPassword: env.String("REDIS_PASSWORD", ""),
		RedisDB:       env.Int("REDIS_DB", 0),

		// Ingestion
		PollIntervalSeconds: env.Int("POLL_INTERVAL_SECONDS", 60),
		BackfillMonths:      env.Int("BACKFILL_MONTHS", 60),
		BackfillDelayDays:   env.Int("BACKFILL_DELAY_DAYS", 0), // 0 = include today (Alpaca IEX is real-time)
		MarketOpenHour:      env.Int("MARKET_OPEN_HOUR", 4),    // 4am ET (pre-market)
		MarketCloseHour:     env.Int("MARKET_CLOSE_HOUR", 20),  // 8pm ET (after-hours)
		EnablePreMarket:     env.Bool("ENABLE_PRE_MARKET", true),
		EnableAfterHours:    env.Bool("ENABLE_AFTER_HOURS", true),

		// Polling settings
		PollingDelayMinutes: env.Int("POLLING_DELAY_MINUTES", 0), // 0 = real-time (Alpaca IEX feed)
	}

	// Validate required fields
	if cfg.AlpacaKeyID == "" {
		return nil, fmt.Errorf("ALPACA_API_KEY_ID is required")
	}
	if cfg.AlpacaSecretKey == "" {
		return nil, fmt.Errorf("ALPACA_API_SECRET_KEY is required")
	}

	return cfg, nil
}

// DatabaseDSN returns the PostgreSQL connection string
func (c *Config) DatabaseDSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.DBHost, c.DBPort, c.DBUser, c.DBPassword, c.DBName, c.DBSSLMode,
	)
}

// DatabaseURL returns the PostgreSQL connection URL
func (c *Config) DatabaseURL() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.DBUser, c.DBPassword, c.DBHost, c.DBPort, c.DBName, c.DBSSLMode,
	)
}

// RedisAddr returns the Redis address in host:port format
func (c *Config) RedisAddr() string {
	return fmt.Sprintf("%s:%d", c.RedisHost, c.RedisPort)
}
