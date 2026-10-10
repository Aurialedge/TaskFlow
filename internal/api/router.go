package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/Aurialedge/TaskFlow/internal/service"
)

// RouterConfig holds dependencies required to build the HTTP router.
type RouterConfig struct {
	Database   DatabasePinger
	JobService *service.JobService
}

// NewRouter constructs a chi.Mux router with standard middleware and routes.
func NewRouter(cfg RouterConfig) *chi.Mux {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(slogMiddleware)
	r.Use(middleware.Recoverer)

	healthHandler := NewHealthHandler(cfg.Database)

	r.Get("/health/live", healthHandler.Live)
	r.Get("/health/ready", healthHandler.Ready)

	// API v1 group
	r.Route("/api/v1", func(v1 chi.Router) {
		v1.Get("/ping", func(w http.ResponseWriter, r *http.Request) {
			respondJSON(w, http.StatusOK, map[string]string{
				"message": "TaskFlow API v1 is active",
			})
		})

		if cfg.JobService != nil {
			jobHandler := NewJobHandler(cfg.JobService)
			v1.Post("/jobs", jobHandler.SubmitJob)
			v1.Get("/jobs/{id}", jobHandler.GetJob)
			v1.Get("/jobs", jobHandler.ListJobs)
		}
	})

	return r
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

func slogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(rec, r)

		duration := time.Since(start)
		reqID := middleware.GetReqID(r.Context())

		slog.InfoContext(r.Context(), "http request completed",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.statusCode,
			"duration_ms", duration.Milliseconds(),
			"request_id", reqID,
			"remote_ip", r.RemoteAddr,
		)
	})
}
