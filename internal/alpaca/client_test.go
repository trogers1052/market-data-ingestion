package alpaca

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustDec(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

// rewriteTransport redirects all outbound requests to the test server,
// preserving the original path and query so handlers can route on them.
type rewriteTransport struct {
	base *url.URL
}

func (rt *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = rt.base.Scheme
	req.URL.Host = rt.base.Host
	return http.DefaultTransport.RoundTrip(req)
}

func newTestClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	c := NewClient("test-key", "test-secret")
	c.httpClient = &http.Client{Transport: &rewriteTransport{base: u}}
	return c
}

func TestNewClient(t *testing.T) {
	c := NewClient("k", "s")
	assert.Equal(t, "k", c.keyID)
	assert.Equal(t, "s", c.secretKey)
	assert.NotNil(t, c.httpClient)
}

func TestGetMinuteBars(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "test-key", r.Header.Get("APCA-API-KEY-ID"))
		assert.Equal(t, "test-secret", r.Header.Get("APCA-API-SECRET-KEY"))
		assert.Contains(t, r.URL.Path, "/v2/stocks/AAPL/bars")
		assert.Equal(t, "1Min", r.URL.Query().Get("timeframe"))
		assert.Equal(t, "iex", r.URL.Query().Get("feed"))

		resp := BarsResponse{
			Symbol: "AAPL",
			Bars: []Bar{
				{Timestamp: "2026-01-02T15:00:00Z", Open: "100.5", High: "101", Low: "100", Close: "100.75", Volume: "1000", VWAP: "100.6", TradeCount: 10},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	bars, err := c.GetMinuteBars(context.Background(), "AAPL", time.Now().Add(-time.Hour), time.Now())
	require.NoError(t, err)
	require.Len(t, bars, 1)
	assert.Equal(t, "AAPL", bars[0].Symbol)
	assert.True(t, bars[0].Open.Equal(mustDec("100.5")))
	assert.Equal(t, int64(1000), bars[0].Volume)
	assert.Equal(t, 10, bars[0].TradeCount)
}

func TestGetDailyBars(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "1Day", r.URL.Query().Get("timeframe"))
		json.NewEncoder(w).Encode(BarsResponse{Symbol: "AAPL", Bars: []Bar{
			{Timestamp: "2026-01-02T00:00:00Z", Open: "100", High: "110", Low: "95", Close: "105", Volume: "5000"},
		}})
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	bars, err := c.GetDailyBars(context.Background(), "AAPL", time.Now().Add(-24*time.Hour), time.Now())
	require.NoError(t, err)
	require.Len(t, bars, 1)
}

func TestGetBars_Pagination(t *testing.T) {
	call := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call++
		if r.URL.Query().Get("page_token") == "" {
			json.NewEncoder(w).Encode(BarsResponse{
				Symbol:        "AAPL",
				Bars:          []Bar{{Timestamp: "2026-01-02T15:00:00Z", Open: "1", High: "2", Low: "1", Close: "2", Volume: "10"}},
				NextPageToken: "page2",
			})
			return
		}
		json.NewEncoder(w).Encode(BarsResponse{
			Symbol: "AAPL",
			Bars:   []Bar{{Timestamp: "2026-01-02T15:01:00Z", Open: "2", High: "3", Low: "2", Close: "3", Volume: "20"}},
		})
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	bars, err := c.GetMinuteBars(context.Background(), "AAPL", time.Now().Add(-time.Hour), time.Now())
	require.NoError(t, err)
	assert.Len(t, bars, 2)
	assert.Equal(t, 2, call)
}

func TestGetBars_SkipsBadTimestamp(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(BarsResponse{Symbol: "AAPL", Bars: []Bar{
			{Timestamp: "not-a-time", Open: "1", High: "2", Low: "1", Close: "2", Volume: "10"},
			{Timestamp: "2026-01-02T15:00:00Z", Open: "1", High: "2", Low: "1", Close: "2", Volume: "10"},
		}})
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	bars, err := c.GetMinuteBars(context.Background(), "AAPL", time.Now().Add(-time.Hour), time.Now())
	require.NoError(t, err)
	assert.Len(t, bars, 1) // bad timestamp skipped
}

func TestGetBars_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server error"))
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	_, err := c.GetMinuteBars(context.Background(), "AAPL", time.Now().Add(-time.Hour), time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 500")
}

func TestGetBars_BadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{not json"))
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	_, err := c.GetMinuteBars(context.Background(), "AAPL", time.Now().Add(-time.Hour), time.Now())
	require.Error(t, err)
}

func TestGetTickerDetails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/v2/assets/AAPL")
		json.NewEncoder(w).Encode(Asset{Symbol: "AAPL", Name: "Apple Inc", Exchange: "NASDAQ", Status: "active"})
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	d, err := c.GetTickerDetails(context.Background(), "AAPL")
	require.NoError(t, err)
	assert.Equal(t, "AAPL", d.Symbol)
	assert.Equal(t, "Apple Inc", d.Name)
}

func TestGetTickerDetails_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	_, err := c.GetTickerDetails(context.Background(), "ZZZZ")
	require.Error(t, err)
}

func TestGetTickerDetails_BadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{bad"))
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	_, err := c.GetTickerDetails(context.Background(), "AAPL")
	require.Error(t, err)
}

func TestDoRequest_ContextCancelledDuringRetry(t *testing.T) {
	// Always return 429 so it would retry; cancel context so the backoff
	// select returns ctx.Err() instead of sleeping 30s.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.GetTickerDetails(ctx, "AAPL")
	require.Error(t, err)
}

// errTransport always fails, simulating a network/transport error without
// making any real outbound connection.
type errTransport struct{}

func (errTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errAlwaysFail
}

var errAlwaysFail = &url.Error{Op: "Get", URL: "x", Err: errString("dial refused")}

type errString string

func (e errString) Error() string { return string(e) }

func TestDoRequest_RequestError(t *testing.T) {
	c := NewClient("k", "s")
	c.httpClient = &http.Client{Transport: errTransport{}}
	_, err := c.GetTickerDetails(context.Background(), "AAPL")
	require.Error(t, err)
	_ = strings.TrimSpace(err.Error())
}
