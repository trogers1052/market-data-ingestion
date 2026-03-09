package main

import (
	"log"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	// Import the metrics package so that promauto registers all custom
	// metrics with the default Prometheus registry before we serve /metrics.
	_ "github.com/trogers1052/market-data-ingestion/internal/metrics"
)

func startMetricsServer() {
	port := os.Getenv("METRICS_PORT")
	if port == "" {
		port = "9090"
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":"+port, metricsMux); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()
	log.Printf("Metrics server listening on :%s/metrics", port)
}
