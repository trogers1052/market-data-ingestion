package marketdata

import (
	"context"
	"time"

	"github.com/trogers1052/market-data-ingestion/internal/models"
)

// TickerDetails contains basic information about a ticker symbol
type TickerDetails struct {
	Symbol string
	Name   string
}

// Client defines the interface for fetching market data from an external provider
type Client interface {
	GetMinuteBars(ctx context.Context, symbol string, from, to time.Time) ([]models.OHLCV, error)
	GetTickerDetails(ctx context.Context, symbol string) (*TickerDetails, error)
}
