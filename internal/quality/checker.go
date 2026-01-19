package quality

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/ingestion"
	"github.com/trogers1052/market-data-ingestion/internal/polygon"
)

// Gap is an alias for database.Gap
type Gap = database.Gap

// DataQualityReport contains the results of a data quality check
type DataQualityReport struct {
	Symbol         string
	CheckedAt      time.Time
	TotalBars      int64
	ExpectedBars   int64
	CoveragePercent float64
	Gaps           []Gap
	Anomalies      []Anomaly
	DataRange      *DataRange
}

// DataRange represents the time range of available data
type DataRange struct {
	Earliest time.Time
	Latest   time.Time
}

// Anomaly represents a data anomaly
type Anomaly struct {
	Time        time.Time
	Type        string // "zero_volume", "price_spike", "stale_data"
	Description string
}

// Checker performs data quality checks
type Checker struct {
	repo          *database.Repository
	polygonClient *polygon.Client
	scheduler     *ingestion.MarketScheduler
}

// NewChecker creates a new data quality checker
func NewChecker(repo *database.Repository, polygonClient *polygon.Client, scheduler *ingestion.MarketScheduler) *Checker {
	return &Checker{
		repo:          repo,
		polygonClient: polygonClient,
		scheduler:     scheduler,
	}
}

// CheckSymbol performs a data quality check for a single symbol
func (c *Checker) CheckSymbol(ctx context.Context, symbol string, from, to time.Time) (*DataQualityReport, error) {
	report := &DataQualityReport{
		Symbol:    symbol,
		CheckedAt: time.Now(),
	}

	// Get data range
	earliest, latest, err := c.repo.GetDataRange(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get data range: %w", err)
	}

	if earliest != nil && latest != nil {
		report.DataRange = &DataRange{
			Earliest: *earliest,
			Latest:   *latest,
		}
	}

	// Get bar count
	count, err := c.repo.GetBarCount(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get bar count: %w", err)
	}
	report.TotalBars = count

	// Calculate expected bars
	tradingDays := c.scheduler.GetTradingDaysInRange(from, to)
	barsPerDay := 390 // Regular market hours (9:30am - 4pm = 390 minutes)
	report.ExpectedBars = int64(tradingDays * barsPerDay)

	if report.ExpectedBars > 0 {
		report.CoveragePercent = float64(report.TotalBars) / float64(report.ExpectedBars) * 100
	}

	// Detect gaps
	gaps, err := c.detectGaps(ctx, symbol, from, to)
	if err != nil {
		log.Printf("Warning: gap detection failed for %s: %v", symbol, err)
	} else {
		report.Gaps = gaps
	}

	// Check for anomalies
	anomalies, err := c.detectAnomalies(ctx, symbol, from, to)
	if err != nil {
		log.Printf("Warning: anomaly detection failed for %s: %v", symbol, err)
	} else {
		report.Anomalies = anomalies
	}

	return report, nil
}

// detectGaps finds gaps in the data for a symbol
func (c *Checker) detectGaps(ctx context.Context, symbol string, from, to time.Time) ([]Gap, error) {
	// Query to find gaps larger than 5 minutes during market hours
	query := `
		WITH bars AS (
			SELECT time,
				   LEAD(time) OVER (ORDER BY time) as next_time
			FROM ohlcv_1min
			WHERE symbol = $1
			  AND time >= $2
			  AND time <= $3
		)
		SELECT time, next_time
		FROM bars
		WHERE next_time - time > INTERVAL '5 minutes'
		  AND EXTRACT(DOW FROM time) BETWEEN 1 AND 5
		  AND EXTRACT(HOUR FROM time AT TIME ZONE 'America/New_York') BETWEEN 9 AND 15
		ORDER BY time
		LIMIT 100
	`

	rows, err := c.repo.QueryGaps(ctx, query, symbol, from, to)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// detectAnomalies finds data anomalies
func (c *Checker) detectAnomalies(ctx context.Context, symbol string, from, to time.Time) ([]Anomaly, error) {
	var anomalies []Anomaly

	// Check for zero volume bars
	zeroVolumeBars, err := c.repo.CountZeroVolumeBars(ctx, symbol, from, to)
	if err != nil {
		return nil, err
	}
	if zeroVolumeBars > 0 {
		anomalies = append(anomalies, Anomaly{
			Time:        time.Now(),
			Type:        "zero_volume",
			Description: fmt.Sprintf("Found %d bars with zero volume", zeroVolumeBars),
		})
	}

	// Check for price spikes (>10% change in 1 minute)
	priceSpikes, err := c.repo.CountPriceSpikes(ctx, symbol, from, to, 0.10)
	if err != nil {
		return nil, err
	}
	if priceSpikes > 0 {
		anomalies = append(anomalies, Anomaly{
			Time:        time.Now(),
			Type:        "price_spike",
			Description: fmt.Sprintf("Found %d potential price spikes (>10%% change)", priceSpikes),
		})
	}

	return anomalies, nil
}

// FillGaps attempts to fill detected gaps by fetching missing data
func (c *Checker) FillGaps(ctx context.Context, symbol string, gaps []Gap) (int, error) {
	if len(gaps) == 0 {
		return 0, nil
	}

	totalFilled := 0

	for _, gap := range gaps {
		log.Printf("Filling gap for %s: %s to %s",
			symbol, gap.StartTime.Format(time.RFC3339), gap.EndTime.Format(time.RFC3339))

		bars, err := c.polygonClient.GetMinuteBars(ctx, symbol, gap.StartTime, gap.EndTime)
		if err != nil {
			log.Printf("Warning: failed to fetch data for gap: %v", err)
			continue
		}

		if len(bars) > 0 {
			if err := c.repo.InsertOHLCVBatch(ctx, bars); err != nil {
				log.Printf("Warning: failed to insert gap data: %v", err)
				continue
			}
			totalFilled += len(bars)
			log.Printf("Filled %d bars for gap", len(bars))
		}
	}

	return totalFilled, nil
}

// CheckAllSymbols runs quality checks on all monitored symbols
func (c *Checker) CheckAllSymbols(ctx context.Context, from, to time.Time) (map[string]*DataQualityReport, error) {
	symbols, err := c.repo.GetMonitoredSymbols(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get monitored symbols: %w", err)
	}

	reports := make(map[string]*DataQualityReport)

	for _, sym := range symbols {
		if !sym.Enabled {
			continue
		}

		report, err := c.CheckSymbol(ctx, sym.Symbol, from, to)
		if err != nil {
			log.Printf("Quality check failed for %s: %v", sym.Symbol, err)
			continue
		}

		reports[sym.Symbol] = report

		log.Printf("Quality report for %s: %.1f%% coverage, %d gaps, %d anomalies",
			sym.Symbol, report.CoveragePercent, len(report.Gaps), len(report.Anomalies))
	}

	return reports, nil
}

// AutoFill automatically detects and fills gaps for all symbols
func (c *Checker) AutoFill(ctx context.Context, from, to time.Time) error {
	reports, err := c.CheckAllSymbols(ctx, from, to)
	if err != nil {
		return err
	}

	for symbol, report := range reports {
		if len(report.Gaps) > 0 {
			filled, err := c.FillGaps(ctx, symbol, report.Gaps)
			if err != nil {
				log.Printf("Auto-fill failed for %s: %v", symbol, err)
			} else {
				log.Printf("Auto-filled %d bars for %s", filled, symbol)
			}
		}
	}

	return nil
}
