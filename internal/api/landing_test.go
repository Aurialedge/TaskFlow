package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleRoot(t *testing.T) {
	router := NewRouter(RouterConfig{Database: &mockDatabasePinger{}})

	t.Run("browser request returns HTML status page", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Accept", "text/html,application/xhtml+xml")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}

		if !strings.Contains(rec.Body.String(), "TaskFlow") {
			t.Errorf("expected HTML body to contain TaskFlow")
		}
	})

	t.Run("API client requesting JSON returns JSON summary", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Accept", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}

		if !strings.Contains(rec.Body.String(), `"system":"TaskFlow"`) {
			t.Errorf("expected JSON to contain system TaskFlow, got %s", rec.Body.String())
		}
	})
}
