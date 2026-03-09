package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/shopspring/decimal"
	"github.com/trogers1052/market-data-ingestion/internal/metrics"
	"github.com/trogers1052/market-data-ingestion/internal/models"
)

// Repository handles all database operations for market data
type Repository struct {
	db *sql.DB
}

// NewRepository creates a new database repository
func NewRepository(dsn string) (*Repository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Repository{db: db}, nil
}

// Close closes the database connection
func (r *Repository) Close() error {
	return r.db.Close()
}

// InsertOHLCV inserts a single OHLCV bar
func (r *Repository) InsertOHLCV(ctx context.Context, bar *models.OHLCV) error {
	query := `
		INSERT INTO ohlcv_1min (time, symbol, open, high, low, close, volume, vwap, trade_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (symbol, time) DO UPDATE SET
			open = EXCLUDED.open,
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			volume = EXCLUDED.volume,
			vwap = EXCLUDED.vwap,
			trade_count = EXCLUDED.trade_count
	`

	start := time.Now()
	_, err := r.db.ExecContext(ctx, query,
		bar.Time, bar.Symbol, bar.Open, bar.High, bar.Low, bar.Close,
		bar.Volume, bar.VWAP, bar.TradeCount)
	metrics.DBWriteDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		metrics.DBWrites.WithLabelValues("error").Inc()
		return err
	}
	metrics.DBWrites.WithLabelValues("success").Inc()
	return nil
}

// InsertOHLCVBatch inserts multiple OHLCV bars efficiently using COPY
func (r *Repository) InsertOHLCVBatch(ctx context.Context, bars []models.OHLCV) error {
	if len(bars) == 0 {
		return nil
	}

	start := time.Now()

	// Use a transaction for batch insert
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		metrics.DBWrites.WithLabelValues("error").Inc()
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Build batch insert query
	// Using VALUES with multiple rows is faster than individual inserts
	const batchSize = 1000
	for i := 0; i < len(bars); i += batchSize {
		end := i + batchSize
		if end > len(bars) {
			end = len(bars)
		}
		batch := bars[i:end]

		// Build VALUES clause
		valueStrings := make([]string, 0, len(batch))
		valueArgs := make([]interface{}, 0, len(batch)*9)

		for j, bar := range batch {
			offset := j * 9
			valueStrings = append(valueStrings, fmt.Sprintf(
				"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				offset+1, offset+2, offset+3, offset+4, offset+5,
				offset+6, offset+7, offset+8, offset+9))
			valueArgs = append(valueArgs,
				bar.Time, bar.Symbol, bar.Open, bar.High, bar.Low,
				bar.Close, bar.Volume, bar.VWAP, bar.TradeCount)
		}

		query := fmt.Sprintf(`
			INSERT INTO ohlcv_1min (time, symbol, open, high, low, close, volume, vwap, trade_count)
			VALUES %s
			ON CONFLICT (symbol, time) DO UPDATE SET
				open = EXCLUDED.open,
				high = EXCLUDED.high,
				low = EXCLUDED.low,
				close = EXCLUDED.close,
				volume = EXCLUDED.volume,
				vwap = EXCLUDED.vwap,
				trade_count = EXCLUDED.trade_count
		`, strings.Join(valueStrings, ","))

		_, err := tx.ExecContext(ctx, query, valueArgs...)
		if err != nil {
			metrics.DBWriteDuration.Observe(time.Since(start).Seconds())
			metrics.DBWrites.WithLabelValues("error").Inc()
			return fmt.Errorf("failed to insert batch: %w", err)
		}
	}

	err = tx.Commit()
	metrics.DBWriteDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.DBWrites.WithLabelValues("error").Inc()
		return err
	}
	metrics.DBWrites.WithLabelValues("success").Inc()
	return nil
}

// GetOHLCV retrieves OHLCV bars for a symbol within a time range
func (r *Repository) GetOHLCV(ctx context.Context, symbol string, from, to time.Time, timeframe models.Timeframe) ([]models.OHLCV, error) {
	// Select table based on timeframe
	table := "ohlcv_1min"
	switch timeframe {
	case models.Timeframe5Min:
		table = "ohlcv_5min"
	case models.Timeframe1Hour:
		table = "ohlcv_1hour"
	case models.Timeframe1Day:
		table = "ohlcv_1day"
	}

	query := fmt.Sprintf(`
		SELECT time, symbol, open, high, low, close, volume, vwap, trade_count
		FROM %s
		WHERE symbol = $1 AND time >= $2 AND time <= $3
		ORDER BY time ASC
	`, table)

	rows, err := r.db.QueryContext(ctx, query, symbol, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query OHLCV: %w", err)
	}
	defer rows.Close()

	var bars []models.OHLCV
	for rows.Next() {
		var bar models.OHLCV
		var vwap sql.NullString
		var tradeCount sql.NullInt32
		err := rows.Scan(&bar.Time, &bar.Symbol, &bar.Open, &bar.High, &bar.Low,
			&bar.Close, &bar.Volume, &vwap, &tradeCount)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		if vwap.Valid {
			bar.VWAP, _ = decimal.NewFromString(vwap.String)
		}
		if tradeCount.Valid {
			bar.TradeCount = int(tradeCount.Int32)
		}
		bars = append(bars, bar)
	}

	return bars, rows.Err()
}

// GetLatestBar returns the most recent bar for a symbol
func (r *Repository) GetLatestBar(ctx context.Context, symbol string) (*models.OHLCV, error) {
	query := `
		SELECT time, symbol, open, high, low, close, volume, vwap, trade_count
		FROM ohlcv_1min
		WHERE symbol = $1
		ORDER BY time DESC
		LIMIT 1
	`

	var bar models.OHLCV
	var vwap sql.NullString
	var tradeCount sql.NullInt32

	err := r.db.QueryRowContext(ctx, query, symbol).Scan(
		&bar.Time, &bar.Symbol, &bar.Open, &bar.High, &bar.Low,
		&bar.Close, &bar.Volume, &vwap, &tradeCount)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest bar: %w", err)
	}

	if vwap.Valid {
		bar.VWAP, _ = decimal.NewFromString(vwap.String)
	}
	if tradeCount.Valid {
		bar.TradeCount = int(tradeCount.Int32)
	}

	return &bar, nil
}

// GetDataRange returns the earliest and latest timestamps for a symbol
func (r *Repository) GetDataRange(ctx context.Context, symbol string) (*time.Time, *time.Time, error) {
	query := `
		SELECT MIN(time), MAX(time)
		FROM ohlcv_1min
		WHERE symbol = $1
	`

	var minTime, maxTime sql.NullTime
	err := r.db.QueryRowContext(ctx, query, symbol).Scan(&minTime, &maxTime)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get data range: %w", err)
	}

	if !minTime.Valid {
		return nil, nil, nil
	}

	return &minTime.Time, &maxTime.Time, nil
}

// GetMonitoredSymbols returns all enabled monitored symbols
func (r *Repository) GetMonitoredSymbols(ctx context.Context) ([]models.MonitoredSymbol, error) {
	query := `
		SELECT symbol, name, enabled, created_at, updated_at
		FROM monitored_symbols
		WHERE enabled = true
		ORDER BY symbol
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query monitored symbols: %w", err)
	}
	defer rows.Close()

	var symbols []models.MonitoredSymbol
	for rows.Next() {
		var s models.MonitoredSymbol
		var name sql.NullString
		err := rows.Scan(&s.Symbol, &name, &s.Enabled, &s.CreatedAt, &s.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		if name.Valid {
			s.Name = name.String
		}
		symbols = append(symbols, s)
	}

	return symbols, rows.Err()
}

// UpsertMonitoredSymbol adds or updates a monitored symbol
func (r *Repository) UpsertMonitoredSymbol(ctx context.Context, symbol, name string, enabled bool) error {
	query := `
		INSERT INTO monitored_symbols (symbol, name, enabled, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (symbol) DO UPDATE SET
			name = EXCLUDED.name,
			enabled = EXCLUDED.enabled,
			updated_at = NOW()
	`

	_, err := r.db.ExecContext(ctx, query, symbol, name, enabled)
	return err
}

// GetBackfillStatus returns the backfill status for a symbol
func (r *Repository) GetBackfillStatus(ctx context.Context, symbol string) (*models.BackfillStatus, error) {
	query := `
		SELECT symbol, last_backfill, backfill_start, backfill_end, status, error_message, created_at, updated_at
		FROM backfill_status
		WHERE symbol = $1
	`

	var bs models.BackfillStatus
	var lastBackfill, backfillStart, backfillEnd sql.NullTime
	var errorMessage sql.NullString

	err := r.db.QueryRowContext(ctx, query, symbol).Scan(
		&bs.Symbol, &lastBackfill, &backfillStart, &backfillEnd,
		&bs.Status, &errorMessage, &bs.CreatedAt, &bs.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get backfill status: %w", err)
	}

	if lastBackfill.Valid {
		bs.LastBackfill = &lastBackfill.Time
	}
	if backfillStart.Valid {
		bs.BackfillStart = &backfillStart.Time
	}
	if backfillEnd.Valid {
		bs.BackfillEnd = &backfillEnd.Time
	}
	if errorMessage.Valid {
		bs.ErrorMessage = errorMessage.String
	}

	return &bs, nil
}

// UpdateBackfillStatus updates the backfill status for a symbol
func (r *Repository) UpdateBackfillStatus(ctx context.Context, symbol, status string, backfillStart, backfillEnd *time.Time, errorMsg string) error {
	query := `
		INSERT INTO backfill_status (symbol, status, backfill_start, backfill_end, last_backfill, error_message, updated_at)
		VALUES ($1, $2, $3, $4, NOW(), $5, NOW())
		ON CONFLICT (symbol) DO UPDATE SET
			status = EXCLUDED.status,
			backfill_start = COALESCE(EXCLUDED.backfill_start, backfill_status.backfill_start),
			backfill_end = COALESCE(EXCLUDED.backfill_end, backfill_status.backfill_end),
			last_backfill = NOW(),
			error_message = EXCLUDED.error_message,
			updated_at = NOW()
	`

	_, err := r.db.ExecContext(ctx, query, symbol, status, backfillStart, backfillEnd, errorMsg)
	return err
}

// GetSymbolsNeedingBackfill returns symbols that need backfill
func (r *Repository) GetSymbolsNeedingBackfill(ctx context.Context) ([]string, error) {
	query := `
		SELECT ms.symbol
		FROM monitored_symbols ms
		LEFT JOIN backfill_status bs ON ms.symbol = bs.symbol
		WHERE ms.enabled = true
		AND (bs.status IS NULL OR bs.status IN ('pending', 'failed'))
		ORDER BY ms.symbol
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query symbols needing backfill: %w", err)
	}
	defer rows.Close()

	var symbols []string
	for rows.Next() {
		var symbol string
		if err := rows.Scan(&symbol); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		symbols = append(symbols, symbol)
	}

	return symbols, rows.Err()
}

// GetBarCount returns the number of bars for a symbol
func (r *Repository) GetBarCount(ctx context.Context, symbol string) (int64, error) {
	query := `SELECT COUNT(*) FROM ohlcv_1min WHERE symbol = $1`

	var count int64
	err := r.db.QueryRowContext(ctx, query, symbol).Scan(&count)
	return count, err
}

// RefreshContinuousAggregates manually refreshes continuous aggregates
func (r *Repository) RefreshContinuousAggregates(ctx context.Context, from, to time.Time) error {
	aggregates := []string{"ohlcv_5min", "ohlcv_1hour", "ohlcv_1day"}

	for _, agg := range aggregates {
		query := fmt.Sprintf(`CALL refresh_continuous_aggregate('%s', $1, $2)`, agg)
		_, err := r.db.ExecContext(ctx, query, from, to)
		if err != nil {
			log.Printf("Warning: failed to refresh %s: %v", agg, err)
			// Continue with other aggregates
		}
	}

	return nil
}

// Gap represents a gap in the data
type Gap struct {
	Symbol    string
	StartTime time.Time
	EndTime   time.Time
}

// QueryGaps finds gaps in the data for a symbol
func (r *Repository) QueryGaps(ctx context.Context, query string, symbol string, from, to time.Time) ([]Gap, error) {
	rows, err := r.db.QueryContext(ctx, query, symbol, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query gaps: %w", err)
	}
	defer rows.Close()

	var gaps []Gap
	for rows.Next() {
		var g Gap
		g.Symbol = symbol
		if err := rows.Scan(&g.StartTime, &g.EndTime); err != nil {
			return nil, fmt.Errorf("failed to scan gap: %w", err)
		}
		gaps = append(gaps, g)
	}

	return gaps, rows.Err()
}

// CountZeroVolumeBars counts bars with zero volume
func (r *Repository) CountZeroVolumeBars(ctx context.Context, symbol string, from, to time.Time) (int, error) {
	query := `
		SELECT COUNT(*)
		FROM ohlcv_1min
		WHERE symbol = $1
		  AND time >= $2
		  AND time <= $3
		  AND volume = 0
	`

	var count int
	err := r.db.QueryRowContext(ctx, query, symbol, from, to).Scan(&count)
	return count, err
}

// CountPriceSpikes counts potential price spikes (large % changes)
func (r *Repository) CountPriceSpikes(ctx context.Context, symbol string, from, to time.Time, threshold float64) (int, error) {
	query := `
		WITH price_changes AS (
			SELECT time,
				   close,
				   LAG(close) OVER (ORDER BY time) as prev_close,
				   ABS(close - LAG(close) OVER (ORDER BY time)) / NULLIF(LAG(close) OVER (ORDER BY time), 0) as pct_change
			FROM ohlcv_1min
			WHERE symbol = $1
			  AND time >= $2
			  AND time <= $3
		)
		SELECT COUNT(*)
		FROM price_changes
		WHERE pct_change > $4
	`

	var count int
	err := r.db.QueryRowContext(ctx, query, symbol, from, to, threshold).Scan(&count)
	return count, err
}

// GetDailyStats returns aggregated daily stats for a symbol
func (r *Repository) GetDailyStats(ctx context.Context, symbol string, date time.Time) (*models.OHLCV, error) {
	query := `
		SELECT
			time_bucket('1 day', time) as day,
			symbol,
			first(open, time) as open,
			max(high) as high,
			min(low) as low,
			last(close, time) as close,
			sum(volume) as volume,
			sum(volume * vwap) / NULLIF(sum(volume), 0) as vwap,
			sum(trade_count) as trade_count
		FROM ohlcv_1min
		WHERE symbol = $1
		  AND time >= $2
		  AND time < $2 + INTERVAL '1 day'
		GROUP BY time_bucket('1 day', time), symbol
	`

	var bar models.OHLCV
	var vwap sql.NullFloat64
	var tradeCount sql.NullInt32

	err := r.db.QueryRowContext(ctx, query, symbol, date).Scan(
		&bar.Time, &bar.Symbol, &bar.Open, &bar.High, &bar.Low,
		&bar.Close, &bar.Volume, &vwap, &tradeCount)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get daily stats: %w", err)
	}

	if tradeCount.Valid {
		bar.TradeCount = int(tradeCount.Int32)
	}

	return &bar, nil
}
