package models

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dec(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

func validBar() OHLCV {
	return OHLCV{
		Time:   time.Date(2026, 1, 2, 15, 0, 0, 0, time.UTC),
		Symbol: "AAPL",
		Open:   dec("100"),
		High:   dec("110"),
		Low:    dec("95"),
		Close:  dec("105"),
		Volume: 1000,
	}
}

func TestOHLCV_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(b *OHLCV)
		wantErr string
	}{
		{"valid", func(b *OHLCV) {}, ""},
		{"empty symbol", func(b *OHLCV) { b.Symbol = "" }, "empty symbol"},
		{"zero time", func(b *OHLCV) { b.Time = time.Time{} }, "zero timestamp"},
		{"zero open", func(b *OHLCV) { b.Open = dec("0") }, "non-positive price"},
		{"negative high", func(b *OHLCV) { b.High = dec("-1") }, "non-positive price"},
		{"zero low", func(b *OHLCV) { b.Low = dec("0") }, "non-positive price"},
		{"zero close", func(b *OHLCV) { b.Close = dec("0") }, "non-positive price"},
		{"high below low", func(b *OHLCV) {
			b.High = dec("90")
			b.Low = dec("95")
			b.Open = dec("90")
			b.Close = dec("90")
		}, "high (90) < low (95)"},
		{"open above high", func(b *OHLCV) { b.Open = dec("200") }, "open (200) outside"},
		{"open below low", func(b *OHLCV) { b.Open = dec("1") }, "open (1) outside"},
		{"close above high", func(b *OHLCV) { b.Close = dec("200") }, "close (200) outside"},
		{"close below low", func(b *OHLCV) { b.Close = dec("1") }, "close (1) outside"},
		{"negative volume", func(b *OHLCV) { b.Volume = -5 }, "negative volume"},
		{"zero volume allowed", func(b *OHLCV) { b.Volume = 0 }, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := validBar()
			tt.mutate(&b)
			err := b.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestBackfillStatusConstants(t *testing.T) {
	assert.Equal(t, "pending", BackfillStatusPending)
	assert.Equal(t, "in_progress", BackfillStatusInProgress)
	assert.Equal(t, "completed", BackfillStatusCompleted)
	assert.Equal(t, "failed", BackfillStatusFailed)
}

func TestTimeframeConstants(t *testing.T) {
	assert.Equal(t, Timeframe("1min"), Timeframe1Min)
	assert.Equal(t, Timeframe("5min"), Timeframe5Min)
	assert.Equal(t, Timeframe("1hour"), Timeframe1Hour)
	assert.Equal(t, Timeframe("1day"), Timeframe1Day)
}
