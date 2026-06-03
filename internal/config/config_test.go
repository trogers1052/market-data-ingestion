package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allEnvKeys lists every environment variable Load reads, so tests can
// guarantee a clean slate regardless of the host environment.
var allEnvKeys = []string{
	"ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY",
	"DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_SSL_MODE",
	"KAFKA_BROKERS", "KAFKA_QUOTES_TOPIC", "KAFKA_ENABLED",
	"KAFKA_WATCHLIST_TOPIC", "KAFKA_CONSUMER_GROUP", "WATCHLIST_SYNC_ENABLED",
	"REDIS_HOST", "REDIS_PORT", "REDIS_PASSWORD", "REDIS_DB",
	"POLL_INTERVAL_SECONDS", "BACKFILL_MONTHS", "BACKFILL_DELAY_DAYS",
	"MARKET_OPEN_HOUR", "MARKET_CLOSE_HOUR", "ENABLE_PRE_MARKET", "ENABLE_AFTER_HOURS",
	"POLLING_DELAY_MINUTES",
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range allEnvKeys {
		t.Setenv(k, "") // ensures original is restored after the test
		os.Unsetenv(k)  // getEnv treats "" as unset, but be explicit
	}
}

func TestLoad_RequiredFields(t *testing.T) {
	clearEnv(t)

	// Missing both keys
	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ALPACA_API_KEY_ID is required")

	// Only key id present -> secret still required
	os.Setenv("ALPACA_API_KEY_ID", "key")
	_, err = Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ALPACA_API_SECRET_KEY is required")
}

func TestLoad_Defaults(t *testing.T) {
	clearEnv(t)
	os.Setenv("ALPACA_API_KEY_ID", "key")
	os.Setenv("ALPACA_API_SECRET_KEY", "secret")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "key", cfg.AlpacaKeyID)
	assert.Equal(t, "secret", cfg.AlpacaSecretKey)
	assert.Equal(t, "localhost", cfg.DBHost)
	assert.Equal(t, 5432, cfg.DBPort)
	assert.Equal(t, "trader", cfg.DBUser)
	assert.Equal(t, "market_data", cfg.DBName)
	assert.Equal(t, "disable", cfg.DBSSLMode)
	assert.Equal(t, []string{"localhost:19092"}, cfg.KafkaBrokers)
	assert.Equal(t, "stock.quotes.realtime", cfg.KafkaQuotesTopic)
	assert.True(t, cfg.KafkaEnabled)
	assert.Equal(t, "trading.watchlist", cfg.KafkaWatchlistTopic)
	assert.Equal(t, "market-data-ingestion", cfg.KafkaConsumerGroup)
	assert.True(t, cfg.WatchlistSyncEnabled)
	assert.Equal(t, "localhost", cfg.RedisHost)
	assert.Equal(t, 6379, cfg.RedisPort)
	assert.Equal(t, 0, cfg.RedisDB)
	assert.Equal(t, 60, cfg.PollIntervalSeconds)
	assert.Equal(t, 60, cfg.BackfillMonths)
	assert.Equal(t, 0, cfg.BackfillDelayDays)
	assert.Equal(t, 4, cfg.MarketOpenHour)
	assert.Equal(t, 20, cfg.MarketCloseHour)
	assert.True(t, cfg.EnablePreMarket)
	assert.True(t, cfg.EnableAfterHours)
	assert.Equal(t, 0, cfg.PollingDelayMinutes)
}

func TestLoad_Overrides(t *testing.T) {
	clearEnv(t)
	env := map[string]string{
		"ALPACA_API_KEY_ID":      "k",
		"ALPACA_API_SECRET_KEY":  "s",
		"DB_HOST":                "db.example.com",
		"DB_PORT":                "6543",
		"DB_USER":                "alice",
		"DB_PASSWORD":            "pw",
		"DB_NAME":                "mydb",
		"DB_SSL_MODE":            "require",
		"KAFKA_BROKERS":          "a:9092,b:9092",
		"KAFKA_QUOTES_TOPIC":     "quotes",
		"KAFKA_ENABLED":          "false",
		"KAFKA_WATCHLIST_TOPIC":  "wl",
		"KAFKA_CONSUMER_GROUP":   "grp",
		"WATCHLIST_SYNC_ENABLED": "false",
		"REDIS_HOST":             "redis.example.com",
		"REDIS_PORT":             "6380",
		"REDIS_PASSWORD":         "rpw",
		"REDIS_DB":               "3",
		"POLL_INTERVAL_SECONDS":  "30",
		"BACKFILL_MONTHS":        "12",
		"BACKFILL_DELAY_DAYS":    "1",
		"MARKET_OPEN_HOUR":       "9",
		"MARKET_CLOSE_HOUR":      "16",
		"ENABLE_PRE_MARKET":      "false",
		"ENABLE_AFTER_HOURS":     "false",
		"POLLING_DELAY_MINUTES":  "15",
	}
	for k, v := range env {
		os.Setenv(k, v)
	}

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "db.example.com", cfg.DBHost)
	assert.Equal(t, 6543, cfg.DBPort)
	assert.Equal(t, "alice", cfg.DBUser)
	assert.Equal(t, "pw", cfg.DBPassword)
	assert.Equal(t, "mydb", cfg.DBName)
	assert.Equal(t, "require", cfg.DBSSLMode)
	assert.Equal(t, []string{"a:9092", "b:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, "quotes", cfg.KafkaQuotesTopic)
	assert.False(t, cfg.KafkaEnabled)
	assert.Equal(t, "wl", cfg.KafkaWatchlistTopic)
	assert.Equal(t, "grp", cfg.KafkaConsumerGroup)
	assert.False(t, cfg.WatchlistSyncEnabled)
	assert.Equal(t, "redis.example.com", cfg.RedisHost)
	assert.Equal(t, 6380, cfg.RedisPort)
	assert.Equal(t, "rpw", cfg.RedisPassword)
	assert.Equal(t, 3, cfg.RedisDB)
	assert.Equal(t, 30, cfg.PollIntervalSeconds)
	assert.Equal(t, 12, cfg.BackfillMonths)
	assert.Equal(t, 1, cfg.BackfillDelayDays)
	assert.Equal(t, 9, cfg.MarketOpenHour)
	assert.Equal(t, 16, cfg.MarketCloseHour)
	assert.False(t, cfg.EnablePreMarket)
	assert.False(t, cfg.EnableAfterHours)
	assert.Equal(t, 15, cfg.PollingDelayMinutes)
}

func TestLoad_InvalidIntAndBoolFallBackToDefaults(t *testing.T) {
	clearEnv(t)
	os.Setenv("ALPACA_API_KEY_ID", "k")
	os.Setenv("ALPACA_API_SECRET_KEY", "s")
	os.Setenv("DB_PORT", "not-a-number")
	os.Setenv("KAFKA_ENABLED", "not-a-bool")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 5432, cfg.DBPort) // fell back to default
	assert.True(t, cfg.KafkaEnabled)  // fell back to default true
}

func TestConnectionStringHelpers(t *testing.T) {
	cfg := &Config{
		DBHost:     "h",
		DBPort:     5555,
		DBUser:     "u",
		DBPassword: "p",
		DBName:     "n",
		DBSSLMode:  "disable",
		RedisHost:  "rh",
		RedisPort:  6666,
	}

	assert.Equal(t,
		"host=h port=5555 user=u password=p dbname=n sslmode=disable",
		cfg.DatabaseDSN())
	assert.Equal(t,
		"postgres://u:p@h:5555/n?sslmode=disable",
		cfg.DatabaseURL())
	assert.Equal(t, "rh:6666", cfg.RedisAddr())
}

func TestGetEnvHelpers(t *testing.T) {
	os.Unsetenv("SOME_TEST_KEY")
	assert.Equal(t, "fallback", getEnv("SOME_TEST_KEY", "fallback"))
	os.Setenv("SOME_TEST_KEY", "value")
	defer os.Unsetenv("SOME_TEST_KEY")
	assert.Equal(t, "value", getEnv("SOME_TEST_KEY", "fallback"))

	os.Setenv("SOME_INT_KEY", "42")
	defer os.Unsetenv("SOME_INT_KEY")
	assert.Equal(t, 42, getEnvInt("SOME_INT_KEY", 7))
	assert.Equal(t, 7, getEnvInt("MISSING_INT_KEY", 7))

	os.Setenv("SOME_BOOL_KEY", "true")
	defer os.Unsetenv("SOME_BOOL_KEY")
	assert.True(t, getEnvBool("SOME_BOOL_KEY", false))
	assert.False(t, getEnvBool("MISSING_BOOL_KEY", false))
}
