package api

import (
	"net/http"
	"strings"
)

// APIKeyAuthMiddleware verifies API key authentication for protected endpoints.
func APIKeyAuthMiddleware(expectedKey string) func(http.Handler) http.Handler {
	trimmedExpected := strings.TrimSpace(expectedKey)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// If authentication is not configured, allow requests through
			if trimmedExpected == "" {
				next.ServeHTTP(w, r)
				return
			}

			// Whitelist system endpoints (root dashboard, health probes, metrics)
			path := r.URL.Path
			if path == "/" || path == "/health/live" || path == "/health/ready" || path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}

			// Extract key from X-API-Key header or Authorization: Bearer <token>
			providedKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
			if providedKey == "" {
				authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
				if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
					providedKey = strings.TrimSpace(authHeader[7:])
				}
			}

			if providedKey == "" || providedKey != trimmedExpected {
				respondError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid or missing API key")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
