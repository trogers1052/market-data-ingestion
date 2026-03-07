// Healthcheck binary for Docker HEALTHCHECK in distroless images.
package main

import (
	"net/http"
	"os"
)

func main() {
	port := os.Getenv("HEALTH_PORT")
	if port == "" {
		port = "8080"
	}
	resp, err := http.Get("http://localhost:" + port + "/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
