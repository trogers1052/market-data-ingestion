package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds all configuration for the market data ingestion service
type Config struct {
	// Polygon.io API
	PolygonAPIKey string

	// Database (TimescaleDB)
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string

	// Kafka (optional - for publishing to other services)
	KafkaBrokers    []string
	KafkaOHLCVTopic string
	KafkaEnabled    bool

	// Ingestion settings
	PollIntervalSeconds int
	BackfillMonths      int
	MarketOpenHour      int  // ET timezone (e.g., 4 for pre-market, 9 for regular)
	MarketCloseHour     int  // ET timezone (e.g., 20 for after-hours, 16 for regular)
	EnablePreMarket     bool // Include pre-market hours (4am-9:30am ET)
	EnableAfterHours    bool // Include after-hours (4pm-8pm ET)
}

// Load loads configuration from environment variables
func Load() (*Config, error) {
	cfg := &Config{
		// Polygon.io
		PolygonAPIKey: getEnv("POLYGON_API_KEY", ""),

		// Database
		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnvInt("DB_PORT", 5432),
		DBUser:     getEnv("DB_USER", "trader"),
		DBPassword: getEnv("DB_PASSWORD", "REDACTED_PASSWORD"),
		DBName:     getEnv("DB_NAME", "market_data"),
		DBSSLMode:  getEnv("DB_SSL_MODE", "disable"),

		// Kafka
		KafkaBrokers:    strings.Split(getEnv("KAFKA_BROKERS", "localhost:19092"), ","),
		KafkaOHLCVTopic: getEnv("KAFKA_OHLCV_TOPIC", "market.ohlcv.1min"),
		KafkaEnabled:    getEnvBool("KAFKA_ENABLED", false),

		// Ingestion
		PollIntervalSeconds: getEnvInt("POLL_INTERVAL_SECONDS", 60),
		BackfillMonths:      getEnvInt("BACKFILL_MONTHS", 6),
		MarketOpenHour:      getEnvInt("MARKET_OPEN_HOUR", 4),  // 4am ET (pre-market)
		MarketCloseHour:     getEnvInt("MARKET_CLOSE_HOUR", 20), // 8pm ET (after-hours)
		EnablePreMarket:     getEnvBool("ENABLE_PRE_MARKET", true),
		EnableAfterHours:    getEnvBool("ENABLE_AFTER_HOURS", true),
	}

	// Validate required fields
	if cfg.PolygonAPIKey == "" {
		return nil, fmt.Errorf("POLYGON_API_KEY is required")
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
