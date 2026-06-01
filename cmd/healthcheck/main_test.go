package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func portOf(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	_, p, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	require.NoError(t, err)
	return p
}

func TestRun_Healthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/health", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	t.Setenv("HEALTH_PORT", portOf(t, srv))
	assert.Equal(t, 0, run())
}

func TestRun_Non200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	t.Setenv("HEALTH_PORT", portOf(t, srv))
	assert.Equal(t, 1, run())
}

func TestRun_ConnectionRefused(t *testing.T) {
	// Nothing listening on port 1 -> http.Get errors -> exit code 1.
	t.Setenv("HEALTH_PORT", "1")
	assert.Equal(t, 1, run())
}
