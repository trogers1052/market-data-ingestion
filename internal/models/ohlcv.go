package models

import (
	"time"

	"github.com/shopspring/decimal"
)

// OHLCV represents a single candlestick bar
type OHLCV struct {
	Time       time.Time       `json:"time" db:"time"`
	Symbol     string          `json:"symbol" db:"symbol"`
	Open       decimal.Decimal `json:"open" db:"open"`
	High       decimal.Decimal `json:"high" db:"high"`
	Low        decimal.Decimal `json:"low" db:"low"`
	Close      decimal.Decimal `json:"close" db:"close"`
	Volume     int64           `json:"volume" db:"volume"`
	VWAP       decimal.Decimal `json:"vwap,omitempty" db:"vwap"`
	TradeCount int             `json:"trade_count,omitempty" db:"trade_count"`
}

// MonitoredSymbol represents a symbol being tracked for market data
type MonitoredSymbol struct {
	Symbol    string    `json:"symbol" db:"symbol"`
	Name      string    `json:"name" db:"name"`
	Enabled   bool      `json:"enabled" db:"enabled"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// BackfillStatus tracks the progress of historical data backfill for a symbol
type BackfillStatus struct {
	Symbol        string     `json:"symbol" db:"symbol"`
	LastBackfill  *time.Time `json:"last_backfill" db:"last_backfill"`
	BackfillStart *time.Time `json:"backfill_start" db:"backfill_start"`
	BackfillEnd   *time.Time `json:"backfill_end" db:"backfill_end"`
	Status        string     `json:"status" db:"status"` // pending, in_progress, completed, failed
	ErrorMessage  string     `json:"error_message" db:"error_message"`
	CreatedAt     time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at" db:"updated_at"`
}

// Backfill status constants
const (
	BackfillStatusPending    = "pending"
	BackfillStatusInProgress = "in_progress"
	BackfillStatusCompleted  = "completed"
	BackfillStatusFailed     = "failed"
)

// Timeframe represents different bar intervals
type Timeframe string

const (
	Timeframe1Min  Timeframe = "1min"
	Timeframe5Min  Timeframe = "5min"
	Timeframe1Hour Timeframe = "1hour"
	Timeframe1Day  Timeframe = "1day"
)

// OHLCVBatch represents a batch of OHLCV bars for bulk insert
type OHLCVBatch struct {
	Symbol string
	Bars   []OHLCV
}
