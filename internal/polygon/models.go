package polygon

import (
	"time"

	"github.com/shopspring/decimal"
)

// AggregateBar represents a single bar from Polygon's aggregates endpoint
type AggregateBar struct {
	Ticker     string  `json:"T,omitempty"`  // Ticker symbol (only in grouped endpoint)
	Volume     float64 `json:"v"`            // Volume
	VWAP       float64 `json:"vw,omitempty"` // Volume weighted average price
	Open       float64 `json:"o"`            // Open price
	Close      float64 `json:"c"`            // Close price
	High       float64 `json:"h"`            // High price
	Low        float64 `json:"l"`            // Low price
	Timestamp  int64   `json:"t"`            // Unix timestamp in milliseconds
	TradeCount int     `json:"n,omitempty"`  // Number of transactions
}

// Time returns the bar timestamp as time.Time
func (b *AggregateBar) Time() time.Time {
	return time.UnixMilli(b.Timestamp)
}

// OpenDecimal returns open price as decimal
func (b *AggregateBar) OpenDecimal() decimal.Decimal {
	return decimal.NewFromFloat(b.Open)
}

// HighDecimal returns high price as decimal
func (b *AggregateBar) HighDecimal() decimal.Decimal {
	return decimal.NewFromFloat(b.High)
}

// LowDecimal returns low price as decimal
func (b *AggregateBar) LowDecimal() decimal.Decimal {
	return decimal.NewFromFloat(b.Low)
}

// CloseDecimal returns close price as decimal
func (b *AggregateBar) CloseDecimal() decimal.Decimal {
	return decimal.NewFromFloat(b.Close)
}

// VWAPDecimal returns VWAP as decimal
func (b *AggregateBar) VWAPDecimal() decimal.Decimal {
	return decimal.NewFromFloat(b.VWAP)
}

// AggregatesResponse is the response from Polygon's aggregates endpoint
type AggregatesResponse struct {
	Ticker       string         `json:"ticker"`
	QueryCount   int            `json:"queryCount"`
	ResultsCount int            `json:"resultsCount"`
	Adjusted     bool           `json:"adjusted"`
	Results      []AggregateBar `json:"results"`
	Status       string         `json:"status"`
	RequestID    string         `json:"request_id"`
	NextURL      string         `json:"next_url,omitempty"` // For pagination
}

// GroupedDailyResponse is the response from Polygon's grouped daily endpoint
type GroupedDailyResponse struct {
	QueryCount   int            `json:"queryCount"`
	ResultsCount int            `json:"resultsCount"`
	Adjusted     bool           `json:"adjusted"`
	Results      []AggregateBar `json:"results"`
	Status       string         `json:"status"`
	RequestID    string         `json:"request_id"`
}

// TickerDetails contains detailed information about a ticker
type TickerDetails struct {
	Ticker      string `json:"ticker"`
	Name        string `json:"name"`
	Market      string `json:"market"`
	Locale      string `json:"locale"`
	Type        string `json:"type"`
	Active      bool   `json:"active"`
	CurrencyName string `json:"currency_name"`
}

// TickerDetailsResponse is the response from the ticker details endpoint
type TickerDetailsResponse struct {
	Results   TickerDetails `json:"results"`
	Status    string        `json:"status"`
	RequestID string        `json:"request_id"`
}

// MarketStatus represents the current market status
type MarketStatus struct {
	Market     string `json:"market"`
	ServerTime string `json:"serverTime"`
	Exchanges  struct {
		NYSE   string `json:"nyse"`
		NASDAQ string `json:"nasdaq"`
	} `json:"exchanges"`
	Currencies struct {
		FX     string `json:"fx"`
		Crypto string `json:"crypto"`
	} `json:"currencies"`
}
