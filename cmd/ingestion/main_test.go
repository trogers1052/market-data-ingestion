package main

import (
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/market-data-ingestion/internal/quality"
)

func TestPrintQualityReport(t *testing.T) {
	now := time.Now()
	report := &quality.DataQualityReport{
		Symbol:          "AAPL",
		CheckedAt:       now,
		TotalBars:       1000,
		ExpectedBars:    1200,
		CoveragePercent: 83.3,
		DataRange:       &quality.DataRange{Earliest: now.Add(-24 * time.Hour), Latest: now},
		Gaps: []quality.Gap{
			{Symbol: "AAPL", StartTime: now.Add(-time.Hour), EndTime: now},
		},
		Anomalies: []quality.Anomaly{
			{Time: now, Type: "zero_volume", Description: "found some"},
		},
	}
	// Should not panic and exercises all branches.
	printQualityReport(report)
}

func TestPrintQualityReport_NoDataRangeNoGaps(t *testing.T) {
	report := &quality.DataQualityReport{
		Symbol:    "MSFT",
		CheckedAt: time.Now(),
	}
	printQualityReport(report)
}

func TestPrintQualityReport_ManyGapsNotListed(t *testing.T) {
	now := time.Now()
	var gaps []quality.Gap
	for i := 0; i < 10; i++ {
		gaps = append(gaps, quality.Gap{Symbol: "AAPL", StartTime: now, EndTime: now})
	}
	report := &quality.DataQualityReport{Symbol: "AAPL", CheckedAt: now, Gaps: gaps}
	printQualityReport(report) // >5 gaps -> not individually listed
}

func TestStartMetricsServer(t *testing.T) {
	t.Setenv("METRICS_PORT", "19191")
	startMetricsServer()
	// Give the goroutine a moment to bind.
	var resp *http.Response
	var err error
	for i := 0; i < 20; i++ {
		resp, err = http.Get("http://localhost:19191/metrics")
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "mdi_")
}

func TestStartMetricsServer_DefaultPort(t *testing.T) {
	os.Unsetenv("METRICS_PORT")
	// Default port 9090 — just ensure it does not panic when invoked.
	// (Another server may already own 9090 in this process; the goroutine
	// logs and exits, which is acceptable.)
	startMetricsServer()
	time.Sleep(20 * time.Millisecond)
}
