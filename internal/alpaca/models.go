package alpaca

import "encoding/json"

// Bar represents a single OHLCV bar from Alpaca's bars API.
// Prices use json.Number to preserve the exact decimal representation
// from the API response, avoiding float64 binary rounding artifacts.
type Bar struct {
	Timestamp  string      `json:"t"` // RFC3339 timestamp
	Open       json.Number `json:"o"`
	High       json.Number `json:"h"`
	Low        json.Number `json:"l"`
	Close      json.Number `json:"c"`
	Volume     json.Number `json:"v"`
	VWAP       json.Number `json:"vw"`
	TradeCount int         `json:"n"`
}

// BarsResponse is the response from Alpaca's stock bars endpoint
type BarsResponse struct {
	Bars          []Bar  `json:"bars"`
	Symbol        string `json:"symbol"`
	NextPageToken string `json:"next_page_token"` // empty or null when no more pages
}

// Asset represents a stock/ETF asset from Alpaca's assets endpoint
type Asset struct {
	Symbol   string `json:"symbol"`
	Name     string `json:"name"`
	Exchange string `json:"exchange"`
	Status   string `json:"status"`
}
