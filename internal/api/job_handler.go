package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Aurialedge/TaskFlow/internal/domain"
	"github.com/Aurialedge/TaskFlow/internal/service"
)

// JobHandler handles HTTP requests for job ingestion and querying.
type JobHandler struct {
	jobService *service.JobService
}

// NewJobHandler constructs a new JobHandler.
func NewJobHandler(jobService *service.JobService) *JobHandler {
	return &JobHandler{jobService: jobService}
}

// SubmitJobRequest defines the JSON payload for submitting a job.
type SubmitJobRequest struct {
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          *time.Time      `json:"run_at"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	IdempotencyKey *string         `json:"idempotency_key"`
}

// JobResponse wraps a job and its attempt history for API responses.
type JobResponse struct {
	domain.Job
	Attempts []*domain.JobAttempt `json:"attempts,omitempty"`
}

// ListJobsResponse represents the paginated response for jobs listing.
type ListJobsResponse struct {
	Data       []*domain.Job      `json:"data"`
	Pagination PaginationMetadata `json:"pagination"`
}

// PaginationMetadata provides pagination details.
type PaginationMetadata struct {
	Limit  int   `json:"limit"`
	Offset int   `json:"offset"`
	Total  int64 `json:"total"`
}

// APIError represents the standardized JSON error structure.
type APIError struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail contains error code and message.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// SubmitJob handles POST /api/v1/jobs.
// Responds with 202 Accepted after durable persistence.
func (h *JobHandler) SubmitJob(w http.ResponseWriter, r *http.Request) {
	var req SubmitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_JSON", "failed to parse request body: "+err.Error())
		return
	}

	// Idempotency key precedence: header over body
	idempotencyKey := req.IdempotencyKey
	if headerKey := strings.TrimSpace(r.Header.Get("Idempotency-Key")); headerKey != "" {
		idempotencyKey = &headerKey
	}

	// Scope precedence: X-Tenant-ID header over default
	scope := strings.TrimSpace(r.Header.Get("X-Tenant-ID"))
	if scope == "" {
		scope = "default"
	}

	input := domain.CreateJobInput{
		Queue:            req.Queue,
		Type:             req.Type,
		Payload:          req.Payload,
		Priority:         req.Priority,
		MaxAttempts:      req.MaxAttempts,
		RunAt:            req.RunAt,
		TimeoutSeconds:   req.TimeoutSeconds,
		IdempotencyKey:   idempotencyKey,
		IdempotencyScope: scope,
	}

	job, _, err := h.jobService.SubmitJob(r.Context(), input)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidJobInput) {
			respondError(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}
		if errors.Is(err, domain.ErrPayloadTooLarge) {
			respondError(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", err.Error())
			return
		}
		respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to persist job: "+err.Error())
		return
	}

	// 202 Accepted only after durable job creation
	respondJSON(w, http.StatusAccepted, job)
}

// GetJob handles GET /api/v1/jobs/{id}.
func (h *JobHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	jobID, err := uuid.Parse(idStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_UUID", "invalid job UUID format")
		return
	}

	job, attempts, err := h.jobService.GetJob(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			respondError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to retrieve job: "+err.Error())
		return
	}

	res := JobResponse{
		Job:      *job,
		Attempts: attempts,
	}
	respondJSON(w, http.StatusOK, res)
}

// ListJobs handles GET /api/v1/jobs.
func (h *JobHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	queue := strings.TrimSpace(query.Get("queue"))
	state := domain.State(strings.TrimSpace(query.Get("state")))

	limit := 20
	if l := query.Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	offset := 0
	if o := query.Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	filter := domain.ListJobsFilter{
		Queue:  queue,
		State:  state,
		Limit:  limit,
		Offset: offset,
	}

	jobs, total, err := h.jobService.ListJobs(r.Context(), filter)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidState) {
			respondError(w, http.StatusBadRequest, "INVALID_STATE", err.Error())
			return
		}
		respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list jobs: "+err.Error())
		return
	}

	if jobs == nil {
		jobs = []*domain.Job{}
	}

	respondJSON(w, http.StatusOK, ListJobsResponse{
		Data: jobs,
		Pagination: PaginationMetadata{
			Limit:  limit,
			Offset: offset,
			Total:  total,
		},
	})
}

// CancelJob handles POST /api/v1/jobs/{id}/cancel.
func (h *JobHandler) CancelJob(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	jobID, err := uuid.Parse(idStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_UUID", "invalid job UUID format")
		return
	}

	job, err := h.jobService.CancelJob(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			respondError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		if errors.Is(err, domain.ErrInvalidStateTransition) {
			respondError(w, http.StatusConflict, "CONFLICT", err.Error())
			return
		}
		respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to cancel job: "+err.Error())
		return
	}

	respondJSON(w, http.StatusOK, job)
}

// RetryJob handles POST /api/v1/jobs/{id}/retry.
func (h *JobHandler) RetryJob(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	jobID, err := uuid.Parse(idStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_UUID", "invalid job UUID format")
		return
	}

	job, err := h.jobService.ManualRetryJob(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			respondError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		if errors.Is(err, domain.ErrInvalidStateTransition) {
			respondError(w, http.StatusConflict, "CONFLICT", err.Error())
			return
		}
		respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to retry job: "+err.Error())
		return
	}

	respondJSON(w, http.StatusOK, job)
}

// GetQueueStats handles GET /api/v1/queues/{name}/stats.
func (h *JobHandler) GetQueueStats(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")
	if strings.TrimSpace(queueName) == "" {
		respondError(w, http.StatusBadRequest, "INVALID_QUEUE", "queue name cannot be empty")
		return
	}

	stats, err := h.jobService.GetQueueStats(r.Context(), queueName)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to query queue stats: "+err.Error())
		return
	}

	respondJSON(w, http.StatusOK, stats)
}

// DeleteJob handles DELETE /api/v1/jobs/{id}.
func (h *JobHandler) DeleteJob(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	jobID, err := uuid.Parse(idStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_UUID", "invalid job UUID format")
		return
	}

	if err := h.jobService.DeleteJob(r.Context(), jobID); err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			respondError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to delete job: "+err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func respondError(w http.ResponseWriter, statusCode int, code, message string) {
	respondJSON(w, statusCode, APIError{
		Error: ErrorDetail{
			Code:    code,
			Message: message,
		},
	})
}
