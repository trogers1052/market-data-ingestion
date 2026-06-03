package main

import (
	"log"
	"os"

	"github.com/trogers1052/trading-go-commons/httpserver"

	// Import the metrics package so that promauto registers all custom
	// metrics with the default Prometheus registry before we serve /metrics.
	_ "github.com/trogers1052/market-data-ingestion/internal/metrics"
)

func startMetricsServer() {
	port := os.Getenv("METRICS_PORT")
	if port == "" {
		port = "9090"
	}
	server := httpserver.NewMetricsServer(":" + port)
	errCh := server.Start()
	go func() {
		if err := <-errCh; err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()
	log.Printf("Metrics server listening on :%s/metrics", port)
}
