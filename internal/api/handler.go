package api

import (
	"context"
	"encoding/json"
	"net/http"
)

// DatabasePinger defines the interface required to verify database health.
type DatabasePinger interface {
	Ping(ctx context.Context) error
}

// HealthHandler encapsulates health check endpoints.
type HealthHandler struct {
	db DatabasePinger
}

// NewHealthHandler creates a new HealthHandler instance.
func NewHealthHandler(db DatabasePinger) *HealthHandler {
	return &HealthHandler{db: db}
}

// Live handles GET /health/live.
// Indicates whether the application process is running.
func (h *HealthHandler) Live(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{
		"status": "live",
	})
}

// Ready handles GET /health/ready.
// Indicates whether the application can successfully reach its critical dependencies (PostgreSQL).
func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		respondJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status":   "not ready",
			"database": "uninitialized",
		})
		return
	}

	if err := h.db.Ping(r.Context()); err != nil {
		respondJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status":   "not ready",
			"database": "disconnected",
			"error":    err.Error(),
		})
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"status":   "ready",
		"database": "connected",
	})
}

func respondJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}
