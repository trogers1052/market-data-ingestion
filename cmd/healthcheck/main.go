// Healthcheck binary for Docker HEALTHCHECK in distroless images.
package main

import (
	"net/http"
	"os"
)

// run performs the health probe and returns the process exit code:
// 0 if the /health endpoint returns 200, 1 otherwise.
func run() int {
	port := os.Getenv("HEALTH_PORT")
	if port == "" {
		port = "8080"
	}
	resp, err := http.Get("http://localhost:" + port + "/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		return 1
	}
	resp.Body.Close()
	return 0
}

func main() {
	os.Exit(run())
}
