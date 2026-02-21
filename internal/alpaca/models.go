package alpaca

// Bar represents a single OHLCV bar from Alpaca's bars API
type Bar struct {
	Timestamp  string  `json:"t"`  // RFC3339 timestamp
	Open       float64 `json:"o"`
	High       float64 `json:"h"`
	Low        float64 `json:"l"`
	Close      float64 `json:"c"`
	Volume     float64 `json:"v"`
	VWAP       float64 `json:"vw"`
	TradeCount int     `json:"n"`
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
