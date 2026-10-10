package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Aurialedge/TaskFlow/internal/observability"
)

func TestAuthMiddleware(t *testing.T) {
	expectedKey := "secret-token-123"

	handler := NewRouter(RouterConfig{
		Database:   &mockDatabasePinger{},
		JobService: nil,
		APIKey:     expectedKey,
	})

	t.Run("whitelisted endpoint /health/live succeeds without auth", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK on /health/live, got %d", rec.Code)
		}
	})

	t.Run("whitelisted endpoint /metrics succeeds without auth", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK on /metrics, got %d", rec.Code)
		}
	})

	t.Run("protected endpoint without key returns 401 Unauthorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 Unauthorized, got %d", rec.Code)
		}
	})

	t.Run("protected endpoint with wrong key returns 401 Unauthorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
		req.Header.Set("X-API-Key", "wrong-key")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 Unauthorized, got %d", rec.Code)
		}
	})

	t.Run("protected endpoint with valid X-API-Key succeeds", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
		req.Header.Set("X-API-Key", expectedKey)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}
	})

	t.Run("protected endpoint with valid Bearer token succeeds", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
		req.Header.Set("Authorization", "Bearer "+expectedKey)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}
	})
}

func TestMetricsEndpoint(t *testing.T) {
	observability.GetMetrics().IncJobsSubmitted("test", "echo")

	handler := NewRouter(RouterConfig{Database: &mockDatabasePinger{}})
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "taskflow_jobs_submitted_total") {
		t.Errorf("expected metrics body to contain taskflow metrics, got:\n%s", body[:bytes.IndexByte(rec.Body.Bytes(), '\n')])
	}
}
