package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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
		AlpacaKeyID:     getEnv("ALPACA_API_KEY_ID", ""),
		AlpacaSecretKey: getEnv("ALPACA_API_SECRET_KEY", ""),

		// Database
		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnvInt("DB_PORT", 5432),
		DBUser:     getEnv("DB_USER", "trader"),
		DBPassword: getEnv("DB_PASSWORD", "REDACTED_PASSWORD"),
		DBName:     getEnv("DB_NAME", "market_data"),
		DBSSLMode:  getEnv("DB_SSL_MODE", "disable"),

		// Kafka
		KafkaBrokers:     strings.Split(getEnv("KAFKA_BROKERS", "localhost:19092"), ","),
		KafkaQuotesTopic: getEnv("KAFKA_QUOTES_TOPIC", "stock.quotes.realtime"),
		KafkaEnabled:     getEnvBool("KAFKA_ENABLED", true), // Default to enabled

		// Kafka watchlist consumer
		KafkaWatchlistTopic:  getEnv("KAFKA_WATCHLIST_TOPIC", "trading.watchlist"),
		KafkaConsumerGroup:   getEnv("KAFKA_CONSUMER_GROUP", "market-data-ingestion"),
		WatchlistSyncEnabled: getEnvBool("WATCHLIST_SYNC_ENABLED", true),

		// Redis
		RedisHost:     getEnv("REDIS_HOST", "localhost"),
		RedisPort:     getEnvInt("REDIS_PORT", 6379),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvInt("REDIS_DB", 0),

		// Ingestion
		PollIntervalSeconds: getEnvInt("POLL_INTERVAL_SECONDS", 60),
		BackfillMonths:      getEnvInt("BACKFILL_MONTHS", 60),
		BackfillDelayDays:   getEnvInt("BACKFILL_DELAY_DAYS", 0), // 0 = include today (Alpaca IEX is real-time)
		MarketOpenHour:      getEnvInt("MARKET_OPEN_HOUR", 4),    // 4am ET (pre-market)
		MarketCloseHour:     getEnvInt("MARKET_CLOSE_HOUR", 20),  // 8pm ET (after-hours)
		EnablePreMarket:     getEnvBool("ENABLE_PRE_MARKET", true),
		EnableAfterHours:    getEnvBool("ENABLE_AFTER_HOURS", true),

		// Polling settings
		PollingDelayMinutes: getEnvInt("POLLING_DELAY_MINUTES", 0), // 0 = real-time (Alpaca IEX feed)
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

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}
