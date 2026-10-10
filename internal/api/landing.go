package api

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"strings"
)

//go:embed dashboard.html
var landingHTML string

// HandleRoot serves an interactive status dashboard for browsers or JSON summary for API clients.
func HandleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	accept := r.Header.Get("Accept")
	if strings.Contains(accept, "application/json") {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"system":    "TaskFlow",
			"status":    "online",
			"version":   "1.0.0",
			"endpoints": []string{"/health/live", "/health/ready", "/metrics", "/api/v1/ping", "/api/v1/jobs"},
		})
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(landingHTML))
}
