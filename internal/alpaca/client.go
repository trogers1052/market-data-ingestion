package alpaca

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/shopspring/decimal"
	"github.com/trogers1052/market-data-ingestion/internal/marketdata"
	"github.com/trogers1052/market-data-ingestion/internal/metrics"
	"github.com/trogers1052/market-data-ingestion/internal/models"
)

const (
	dataBaseURL   = "https://data.alpaca.markets"
	brokerBaseURL = "https://api.alpaca.markets"
	httpTimeout   = 30 * time.Second
)

// Client is the Alpaca market data client.
// It implements marketdata.Client using Alpaca's free IEX feed (real-time, no delay).
type Client struct {
	keyID      string
	secretKey  string
	httpClient *http.Client
}

// NewClient creates a new Alpaca API client
func NewClient(keyID, secretKey string) *Client {
	return &Client{
		keyID:     keyID,
		secretKey: secretKey,
		httpClient: &http.Client{
			Timeout: httpTimeout,
		},
	}
}

// GetMinuteBars fetches 1-minute bars for a symbol within the given time range.
// Uses Alpaca's IEX feed (real-time, no delay on free tier).
func (c *Client) GetMinuteBars(ctx context.Context, symbol string, from, to time.Time) ([]models.OHLCV, error) {
	return c.getBars(ctx, symbol, "1Min", from, to)
}

// GetDailyBars fetches daily bars for a symbol within the given time range
func (c *Client) GetDailyBars(ctx context.Context, symbol string, from, to time.Time) ([]models.OHLCV, error) {
	return c.getBars(ctx, symbol, "1Day", from, to)
}

// GetTickerDetails fetches the name and metadata for a symbol via Alpaca's assets API
func (c *Client) GetTickerDetails(ctx context.Context, symbol string) (*marketdata.TickerDetails, error) {
	endpoint := fmt.Sprintf("%s/v2/assets/%s", brokerBaseURL, symbol)

	body, err := c.doRequest(ctx, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get ticker details for %s: %w", symbol, err)
	}

	var asset Asset
	if err := json.Unmarshal(body, &asset); err != nil {
		return nil, fmt.Errorf("failed to parse asset response for %s: %w", symbol, err)
	}

	return &marketdata.TickerDetails{
		Symbol: asset.Symbol,
		Name:   asset.Name,
	}, nil
}

// getBars is the shared implementation for GetMinuteBars and GetDailyBars.
// It handles pagination via next_page_token.
func (c *Client) getBars(ctx context.Context, symbol string, timeframe string, from, to time.Time) ([]models.OHLCV, error) {
	endpoint := fmt.Sprintf("%s/v2/stocks/%s/bars", dataBaseURL, symbol)

	var allBars []models.OHLCV
	pageToken := ""

	for {
		params := map[string]string{
			"timeframe":  timeframe,
			"start":      from.UTC().Format(time.RFC3339),
			"end":        to.UTC().Format(time.RFC3339),
			"feed":       "iex",
			"adjustment": "all",
			"sort":       "asc",
			"limit":      "10000",
		}
		if pageToken != "" {
			params["page_token"] = pageToken
		}

		body, err := c.doRequest(ctx, endpoint, params)
		if err != nil {
			return nil, fmt.Errorf("failed to get %s bars for %s: %w", timeframe, symbol, err)
		}

		var resp BarsResponse
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.UseNumber()
		if err := dec.Decode(&resp); err != nil {
			return nil, fmt.Errorf("failed to parse bars response for %s: %w", symbol, err)
		}

		for _, bar := range resp.Bars {
			ts, err := time.Parse(time.RFC3339, bar.Timestamp)
			if err != nil {
				continue
			}
			open, _ := decimal.NewFromString(bar.Open.String())
			high, _ := decimal.NewFromString(bar.High.String())
			low, _ := decimal.NewFromString(bar.Low.String())
			cls, _ := decimal.NewFromString(bar.Close.String())
			vwap, _ := decimal.NewFromString(bar.VWAP.String())
			vol, _ := bar.Volume.Int64()
			allBars = append(allBars, models.OHLCV{
				Time:       ts,
				Symbol:     symbol,
				Open:       open,
				High:       high,
				Low:        low,
				Close:      cls,
				Volume:     vol,
				VWAP:       vwap,
				TradeCount: bar.TradeCount,
			})
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	return allBars, nil
}

// doRequest makes an authenticated GET request to the Alpaca API.
// It retries up to 3 times on HTTP 429 rate-limit responses.
func (c *Client) doRequest(ctx context.Context, endpoint string, params map[string]string) ([]byte, error) {
	// Build URL with query params
	reqURL := endpoint
	if len(params) > 0 {
		u, err := url.Parse(endpoint)
		if err != nil {
			return nil, fmt.Errorf("invalid endpoint URL: %w", err)
		}
		q := u.Query()
		for k, v := range params {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
		reqURL = u.String()
	}

	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Back off before retrying a rate-limited request
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(30 * time.Second):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("APCA-API-KEY-ID", c.keyID)
		req.Header.Set("APCA-API-SECRET-KEY", c.secretKey)

		apiStart := time.Now()
		resp, err := c.httpClient.Do(req)
		metrics.APICallDuration.Observe(time.Since(apiStart).Seconds())
		if err != nil {
			return nil, err
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 429 {
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
		}

		if readErr != nil {
			return nil, fmt.Errorf("failed to read response body: %w", readErr)
		}

		return body, nil
	}

	return nil, fmt.Errorf("exceeded max retries (%d) on rate limit", maxRetries)
}
