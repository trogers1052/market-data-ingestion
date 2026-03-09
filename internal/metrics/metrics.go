package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// QuotesIngested counts quotes received from Alpaca, labeled by symbol.
	QuotesIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mdi_quotes_ingested_total",
		Help: "Total number of quotes received from Alpaca.",
	}, []string{"symbol"})

	// QuotesPublished counts quotes successfully published to Kafka.
	QuotesPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mdi_quotes_published_total",
		Help: "Total number of quotes published to Kafka.",
	})

	// KafkaPublishErrors counts Kafka publish failures.
	KafkaPublishErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mdi_kafka_publish_errors_total",
		Help: "Total number of Kafka publish failures.",
	})

	// DBWrites counts TimescaleDB write operations, labeled by status (success|error).
	DBWrites = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mdi_db_writes_total",
		Help: "Total number of TimescaleDB write operations.",
	}, []string{"status"})

	// DBWriteDuration observes TimescaleDB write latency in seconds.
	DBWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "mdi_db_write_duration_seconds",
		Help:    "Histogram of TimescaleDB write latency in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// APICallDuration observes Alpaca API call latency in seconds.
	APICallDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "mdi_api_call_duration_seconds",
		Help:    "Histogram of Alpaca API call latency in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// SymbolsMonitored tracks the current number of symbols being monitored.
	SymbolsMonitored = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mdi_symbols_monitored",
		Help: "Number of symbols currently being tracked.",
	})

	// BackfillBars counts bars backfilled from historical data.
	BackfillBars = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mdi_backfill_bars_total",
		Help: "Total number of bars backfilled.",
	})

	// WebSocketReconnects counts WebSocket reconnection events.
	WebSocketReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mdi_websocket_reconnects_total",
		Help: "Total number of WebSocket reconnections.",
	})

	// LastQuoteTimestamp records the Unix timestamp of the last quote per symbol.
	LastQuoteTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mdi_last_quote_timestamp_seconds",
		Help: "Unix timestamp of the last quote received for each symbol.",
	}, []string{"symbol"})
)
