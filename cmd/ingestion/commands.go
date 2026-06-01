package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/trogers1052/market-data-ingestion/internal/database"
	"github.com/trogers1052/market-data-ingestion/internal/marketdata"
	"github.com/trogers1052/market-data-ingestion/internal/quality"
	"github.com/trogers1052/market-data-ingestion/internal/symbols"
)

// parseSymbolList splits a comma-separated symbol string, trims whitespace and
// upper-cases each entry. Empty input yields a nil slice.
func parseSymbolList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(strings.ToUpper(p))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseAddSymbolArg parses an "-add-symbol" argument of the form "SYMBOL" or
// "SYMBOL:Name" and returns the upper-cased symbol and resolved name.
func parseAddSymbolArg(arg string) (symbol, name string) {
	parts := strings.SplitN(arg, ":", 2)
	symbol = strings.ToUpper(parts[0])
	name = symbol
	if len(parts) > 1 && parts[1] != "" {
		name = parts[1]
	}
	return symbol, name
}

// listSymbolsCmd prints all monitored symbols with their bar counts.
func listSymbolsCmd(ctx context.Context, repo *database.Repository) error {
	syms, err := repo.GetMonitoredSymbols(ctx)
	if err != nil {
		return fmt.Errorf("failed to get monitored symbols: %w", err)
	}
	log.Println("Monitored symbols:")
	for _, s := range syms {
		st := "enabled"
		if !s.Enabled {
			st = "disabled"
		}
		count, _ := repo.GetBarCountExact(ctx, s.Symbol)
		log.Printf("  %s (%s) - %s [%d bars]", s.Symbol, s.Name, st, count)
	}
	return nil
}

// addSymbolCmd resolves a symbol's name (via the market data provider when
// available) and upserts it as an enabled monitored symbol.
func addSymbolCmd(ctx context.Context, repo *database.Repository, md marketdata.Client, arg string) (string, string, error) {
	symbol, name := parseAddSymbolArg(arg)

	if md != nil {
		if details, err := md.GetTickerDetails(ctx, symbol); err != nil {
			log.Printf("Warning: could not fetch ticker details: %v", err)
		} else if details.Name != "" {
			name = details.Name
		}
	}

	if err := repo.UpsertMonitoredSymbol(ctx, symbol, name, true); err != nil {
		return "", "", fmt.Errorf("failed to add symbol: %w", err)
	}
	log.Printf("Added symbol: %s (%s)", symbol, name)
	return symbol, name, nil
}

// removeSymbolCmd disables a monitored symbol (data is retained).
func removeSymbolCmd(ctx context.Context, repo *database.Repository, arg string) (string, error) {
	symbol := strings.ToUpper(arg)
	if err := repo.UpsertMonitoredSymbol(ctx, symbol, "", false); err != nil {
		return "", fmt.Errorf("failed to remove symbol: %w", err)
	}
	log.Printf("Disabled symbol: %s", symbol)
	return symbol, nil
}

// resolveStockServiceDSN returns the configured Stock-Service DSN, preferring
// the explicit flag value and falling back to the STOCK_SERVICE_DSN env var.
func resolveStockServiceDSN(flagVal string) (string, error) {
	dsn := flagVal
	if dsn == "" {
		dsn = os.Getenv("STOCK_SERVICE_DSN")
	}
	if dsn == "" {
		return "", fmt.Errorf("Stock-Service DSN required. Use -stock-service-dsn or STOCK_SERVICE_DSN env var")
	}
	return dsn, nil
}

// syncPositionsCmd syncs monitored symbols from a positions source and logs the
// result. The positions source is injected so the logic is testable without a
// live Stock-Service database.
func syncPositionsCmd(ctx context.Context, repo *database.Repository, posSource symbols.PositionsSource) ([]string, error) {
	syncService := symbols.NewSymbolSyncService(repo, posSource, 0)
	added, err := syncService.SyncFromPositions(ctx)
	if err != nil {
		return nil, fmt.Errorf("sync failed: %w", err)
	}
	if len(added) > 0 {
		log.Printf("Added %d symbols from positions: %v", len(added), added)
	} else {
		log.Println("No new symbols to add from positions")
	}
	return added, nil
}

// runQualityCheck runs quality checks for the given symbols (or all monitored
// symbols when symbolList is empty) and prints a report for each.
func runQualityCheck(ctx context.Context, checker *quality.Checker, symbolList []string, days int) error {
	to := time.Now()
	from := to.AddDate(0, 0, -days)

	if len(symbolList) > 0 {
		for _, symbol := range symbolList {
			report, err := checker.CheckSymbol(ctx, symbol, from, to)
			if err != nil {
				log.Printf("Quality check failed for %s: %v", symbol, err)
				continue
			}
			printQualityReport(report)
		}
		return nil
	}

	reports, err := checker.CheckAllSymbols(ctx, from, to)
	if err != nil {
		return fmt.Errorf("quality check failed: %w", err)
	}
	for _, report := range reports {
		printQualityReport(report)
	}
	return nil
}
