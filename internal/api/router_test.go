package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockDatabasePinger struct {
	pingErr error
}

func (m *mockDatabasePinger) Ping(ctx context.Context) error {
	return m.pingErr
}

func TestHealthRoutes(t *testing.T) {
	t.Run("Live always returns 200", func(t *testing.T) {
		router := NewRouter(RouterConfig{Database: &mockDatabasePinger{pingErr: errors.New("db down")}})

		req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}
	})

	t.Run("Ready returns 200 when DB ping succeeds", func(t *testing.T) {
		router := NewRouter(RouterConfig{Database: &mockDatabasePinger{pingErr: nil}})

		req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}
	})

	t.Run("Ready returns 503 when DB ping fails", func(t *testing.T) {
		router := NewRouter(RouterConfig{Database: &mockDatabasePinger{pingErr: errors.New("db disconnected")}})

		req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 Service Unavailable, got %d", rec.Code)
		}
	})

	t.Run("Ready returns 503 when DB is nil", func(t *testing.T) {
		router := NewRouter(RouterConfig{Database: nil})

		req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 Service Unavailable, got %d", rec.Code)
		}
	})

	t.Run("API ping endpoint responds 200", func(t *testing.T) {
		router := NewRouter(RouterConfig{Database: &mockDatabasePinger{}})

		req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}
	})
}
