package polygon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	"github.com/trogers1052/market-data-ingestion/internal/models"
	"github.com/trogers1052/market-data-ingestion/internal/ratelimit"
)

const (
	baseURL        = "https://api.polygon.io"
	wsURL          = "wss://socket.polygon.io/stocks"
	defaultTimeout = 30 * time.Second
)

// Client is the Polygon.io API client
type Client struct {
	apiKey     string
	httpClient *http.Client
	limiter    *ratelimit.PolygonLimiter

	// WebSocket
	wsConn     *websocket.Conn
	wsMu       sync.Mutex
	wsHandler  func(bar models.OHLCV)
	wsStop     chan struct{}
	wsSymbols  []string
}

// NewClient creates a new Polygon API client
func NewClient(apiKey string) *Client {
	return &Client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		limiter: ratelimit.NewPolygonLimiter("starter"), // Default to starter tier
		wsStop:  make(chan struct{}),
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
		wsStop:  make(chan struct{}),
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

		if aggResp.Status != "OK" {
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
	// Wait for rate limiter
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter: %w", err)
	}

	if params == nil {
		params = url.Values{}
	}
	params.Set("apiKey", c.apiKey)

	reqURL := fmt.Sprintf("%s%s?%s", baseURL, endpoint, params.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	// Handle rate limit responses
	if resp.StatusCode == 429 {
		resp.Body.Close()
		// Wait longer and retry once
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(60 * time.Second):
		}
		return c.doRequest(ctx, endpoint, params)
	}

	return resp, nil
}

// StartWebSocket connects to Polygon WebSocket and subscribes to minute aggregates
func (c *Client) StartWebSocket(ctx context.Context, symbols []string, handler func(bar models.OHLCV)) error {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()

	c.wsHandler = handler
	c.wsSymbols = symbols

	// Connect to WebSocket
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}
	c.wsConn = conn

	// Authenticate
	authMsg := WebSocketAuth{
		Action: "auth",
		Params: c.apiKey,
	}
	if err := conn.WriteJSON(authMsg); err != nil {
		conn.Close()
		return fmt.Errorf("failed to authenticate: %w", err)
	}

	// Wait for auth response
	_, msg, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to read auth response: %w", err)
	}
	log.Printf("WebSocket auth response: %s", string(msg))

	// Subscribe to minute aggregates for all symbols
	// AM.* for all symbols, or AM.AAPL,AM.MSFT for specific ones
	var subscribeParams string
	if len(symbols) == 0 {
		subscribeParams = "AM.*" // All symbols
	} else {
		for i, s := range symbols {
			if i > 0 {
				subscribeParams += ","
			}
			subscribeParams += "AM." + s
		}
	}

	subMsg := WebSocketSubscribe{
		Action: "subscribe",
		Params: subscribeParams,
	}
	if err := conn.WriteJSON(subMsg); err != nil {
		conn.Close()
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	log.Printf("Subscribed to: %s", subscribeParams)

	// Start reading messages in background
	go c.readWebSocketMessages(ctx)

	return nil
}

// readWebSocketMessages reads messages from the WebSocket connection
func (c *Client) readWebSocketMessages(ctx context.Context) {
	defer func() {
		c.wsMu.Lock()
		if c.wsConn != nil {
			c.wsConn.Close()
			c.wsConn = nil
		}
		c.wsMu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.wsStop:
			return
		default:
			c.wsMu.Lock()
			conn := c.wsConn
			c.wsMu.Unlock()

			if conn == nil {
				return
			}

			_, msg, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					log.Println("WebSocket closed normally")
					return
				}
				log.Printf("WebSocket read error: %v", err)
				// Attempt reconnect
				go c.reconnectWebSocket(ctx)
				return
			}

			// Parse message - can be array of events
			var events []json.RawMessage
			if err := json.Unmarshal(msg, &events); err != nil {
				// Might be a single object (status message)
				var status WebSocketStatus
				if err := json.Unmarshal(msg, &status); err == nil {
					log.Printf("WebSocket status: %s - %s", status.Status, status.Message)
				}
				continue
			}

			for _, eventData := range events {
				var wsMsg WebSocketMessage
				if err := json.Unmarshal(eventData, &wsMsg); err != nil {
					continue
				}

				// Only process minute aggregate events
				if wsMsg.EventType != "AM" {
					continue
				}

				if c.wsHandler != nil {
					bar := models.OHLCV{
						Time:   time.UnixMilli(wsMsg.Timestamp),
						Symbol: wsMsg.Symbol,
						Open:   decimal.NewFromFloat(wsMsg.Open),
						High:   decimal.NewFromFloat(wsMsg.High),
						Low:    decimal.NewFromFloat(wsMsg.Low),
						Close:  decimal.NewFromFloat(wsMsg.Close),
						Volume: wsMsg.Volume,
						VWAP:   decimal.NewFromFloat(wsMsg.VWAP),
					}
					c.wsHandler(bar)
				}
			}
		}
	}
}

// reconnectWebSocket attempts to reconnect to the WebSocket
func (c *Client) reconnectWebSocket(ctx context.Context) {
	log.Println("Attempting WebSocket reconnect...")

	// Wait before reconnecting
	select {
	case <-ctx.Done():
		return
	case <-time.After(5 * time.Second):
	}

	c.wsMu.Lock()
	symbols := c.wsSymbols
	handler := c.wsHandler
	c.wsMu.Unlock()

	if err := c.StartWebSocket(ctx, symbols, handler); err != nil {
		log.Printf("WebSocket reconnect failed: %v", err)
		// Try again after longer delay
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
			go c.reconnectWebSocket(ctx)
		}
	}
}

// StopWebSocket closes the WebSocket connection
func (c *Client) StopWebSocket() {
	close(c.wsStop)

	c.wsMu.Lock()
	defer c.wsMu.Unlock()

	if c.wsConn != nil {
		c.wsConn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		c.wsConn.Close()
		c.wsConn = nil
	}
}

// UpdateSubscriptions updates the WebSocket subscriptions
func (c *Client) UpdateSubscriptions(symbols []string) error {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()

	if c.wsConn == nil {
		return fmt.Errorf("WebSocket not connected")
	}

	// Unsubscribe from old symbols
	if len(c.wsSymbols) > 0 {
		var unsubParams string
		for i, s := range c.wsSymbols {
			if i > 0 {
				unsubParams += ","
			}
			unsubParams += "AM." + s
		}
		unsubMsg := WebSocketSubscribe{
			Action: "unsubscribe",
			Params: unsubParams,
		}
		if err := c.wsConn.WriteJSON(unsubMsg); err != nil {
			return fmt.Errorf("failed to unsubscribe: %w", err)
		}
	}

	// Subscribe to new symbols
	var subscribeParams string
	for i, s := range symbols {
		if i > 0 {
			subscribeParams += ","
		}
		subscribeParams += "AM." + s
	}
	subMsg := WebSocketSubscribe{
		Action: "subscribe",
		Params: subscribeParams,
	}
	if err := c.wsConn.WriteJSON(subMsg); err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	c.wsSymbols = symbols
	log.Printf("Updated subscriptions: %v", symbols)

	return nil
}
