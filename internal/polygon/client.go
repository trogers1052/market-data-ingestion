package polygon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/trogers1052/market-data-ingestion/internal/models"
	"github.com/trogers1052/market-data-ingestion/internal/ratelimit"
)

const (
	baseURL        = "https://api.polygon.io"
	defaultTimeout = 30 * time.Second
)

// Client is the Polygon.io API client
type Client struct {
	apiKey     string
	httpClient *http.Client
	limiter    *ratelimit.PolygonLimiter
}

// NewClient creates a new Polygon API client
func NewClient(apiKey string) *Client {
	return &Client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		limiter: ratelimit.NewPolygonLimiter("starter"), // Default to starter tier
	}
}

// NewClientWithTier creates a new Polygon API client with a specific rate limit tier
func NewClientWithTier(apiKey string, tier string) *Client {
	return &Client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		limiter: ratelimit.NewPolygonLimiter(tier),
	}
}

// GetAggregates fetches aggregate bars for a symbol
// timespan: minute, hour, day, week, month, quarter, year
// from/to: Unix milliseconds or YYYY-MM-DD format
func (c *Client) GetAggregates(ctx context.Context, symbol string, multiplier int, timespan string, from, to time.Time) ([]models.OHLCV, error) {
	fromStr := from.Format("2006-01-02")
	toStr := to.Format("2006-01-02")

	endpoint := fmt.Sprintf("/v2/aggs/ticker/%s/range/%d/%s/%s/%s",
		symbol, multiplier, timespan, fromStr, toStr)

	params := url.Values{}
	params.Set("adjusted", "true")
	params.Set("sort", "asc")
	params.Set("limit", "50000") // Max limit

	var allBars []models.OHLCV

	for {
		resp, err := c.doRequest(ctx, endpoint, params)
		if err != nil {
			return nil, fmt.Errorf("failed to get aggregates: %w", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}

		var aggResp AggregatesResponse
		if err := json.Unmarshal(body, &aggResp); err != nil {
			return nil, fmt.Errorf("failed to parse response: %w", err)
		}

		// Accept both "OK" and "DELAYED" as valid statuses
		// "DELAYED" is returned by free tier for recent data
		if aggResp.Status != "OK" && aggResp.Status != "DELAYED" {
			return nil, fmt.Errorf("API error: %s", aggResp.Status)
		}

		// Convert to our OHLCV model
		for _, bar := range aggResp.Results {
			allBars = append(allBars, models.OHLCV{
				Time:       bar.Time(),
				Symbol:     symbol,
				Open:       bar.OpenDecimal(),
				High:       bar.HighDecimal(),
				Low:        bar.LowDecimal(),
				Close:      bar.CloseDecimal(),
				Volume:     int64(bar.Volume),
				VWAP:       bar.VWAPDecimal(),
				TradeCount: bar.TradeCount,
			})
		}

		// Check for pagination
		if aggResp.NextURL == "" {
			break
		}

		// Parse next URL for pagination
		nextURL, err := url.Parse(aggResp.NextURL)
		if err != nil {
			break
		}
		endpoint = nextURL.Path
		params = nextURL.Query()
	}

	return allBars, nil
}

// GetMinuteBars fetches 1-minute bars for a symbol for a date range
func (c *Client) GetMinuteBars(ctx context.Context, symbol string, from, to time.Time) ([]models.OHLCV, error) {
	return c.GetAggregates(ctx, symbol, 1, "minute", from, to)
}

// GetDailyBars fetches daily bars for a symbol
func (c *Client) GetDailyBars(ctx context.Context, symbol string, from, to time.Time) ([]models.OHLCV, error) {
	return c.GetAggregates(ctx, symbol, 1, "day", from, to)
}

// GetTickerDetails fetches details about a ticker
func (c *Client) GetTickerDetails(ctx context.Context, symbol string) (*TickerDetails, error) {
	endpoint := fmt.Sprintf("/v3/reference/tickers/%s", symbol)

	resp, err := c.doRequest(ctx, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get ticker details: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var detailsResp TickerDetailsResponse
	if err := json.Unmarshal(body, &detailsResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &detailsResp.Results, nil
}

// GetMarketStatus fetches the current market status
func (c *Client) GetMarketStatus(ctx context.Context) (*MarketStatus, error) {
	endpoint := "/v1/marketstatus/now"

	resp, err := c.doRequest(ctx, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get market status: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var status MarketStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &status, nil
}

// doRequest makes an authenticated HTTP request to the Polygon API
// It automatically handles rate limiting
func (c *Client) doRequest(ctx context.Context, endpoint string, params url.Values) (*http.Response, error) {
	if params == nil {
		params = url.Values{}
	}
	params.Set("apiKey", c.apiKey)

	reqURL := fmt.Sprintf("%s%s?%s", baseURL, endpoint, params.Encode())

	const maxRetries = 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := c.limiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limiter: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == 429 {
			resp.Body.Close()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(60 * time.Second):
			}
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("exceeded max retries (%d) on rate limit", maxRetries)
}
