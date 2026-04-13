package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// BackendClient interface for dependency injection and testing
type BackendClient interface {
	CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult
}

// HealthChecker is an optional interface backends can implement for readiness checks.
type HealthChecker interface {
	HealthCheck(ctx context.Context) error
}

type Handler struct {
	backend        BackendClient
	healthChecker  HealthChecker
	logger         *slog.Logger
	requestsTotal  atomic.Int64
	requestsErrors atomic.Int64
}

func New(backend BackendClient, logger *slog.Logger) *Handler {
	h := &Handler{
		backend: backend,
		logger:  logger,
	}
	// If the backend implements HealthChecker, use it for readiness
	if hc, ok := backend.(HealthChecker); ok {
		h.healthChecker = hc
	}
	return h
}

// NewWithHealthChecker creates a handler with an explicit health checker
// (useful when the backend is wrapped in a cache layer).
func NewWithHealthChecker(backend BackendClient, healthChecker HealthChecker, logger *slog.Logger) *Handler {
	return &Handler{
		backend:       backend,
		healthChecker: healthChecker,
		logger:        logger,
	}
}

func (h *Handler) HandleExists(w http.ResponseWriter, r *http.Request) {
	h.requestsTotal.Add(1)

	// Extract namespace and pvc from path
	// Expected path: /exists/{namespace}/{pvc}
	path := r.URL.Path

	var namespace, pvc string
	// Simple path parsing
	if len(path) > 8 { // "/exists/"
		parts := path[8:] // Remove "/exists/"
		var foundSlash bool
		var i int
		for i = 0; i < len(parts); i++ {
			if parts[i] == '/' {
				namespace = parts[:i]
				foundSlash = true
				break
			}
		}
		if foundSlash && i+1 < len(parts) {
			pvc = parts[i+1:]
		}
	}

	if namespace == "" || pvc == "" {
		h.requestsErrors.Add(1)
		h.logger.Warn("invalid request path", "path", path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"exists": false,
			"error":  "invalid path format, expected /exists/{namespace}/{pvc}",
		})
		return
	}

	h.logger.Info("checking backup", "namespace", namespace, "pvc", pvc)

	result := h.backend.CheckBackupExists(r.Context(), namespace, pvc)

	if result.Error != "" {
		h.requestsErrors.Add(1)
	}

	h.logger.Info("backup check complete",
		"namespace", namespace,
		"pvc", pvc,
		"exists", result.Exists,
		"backend", result.Backend)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (h *Handler) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *Handler) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	if h.healthChecker != nil {
		if err := h.healthChecker.HealthCheck(r.Context()); err != nil {
			h.logger.Warn("readiness check failed", "error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "not ready",
				"error":  err.Error(),
			})
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *Handler) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintf(w, "# HELP pvc_plumber_requests_total Total number of backup check requests\n")
	_, _ = fmt.Fprintf(w, "# TYPE pvc_plumber_requests_total counter\n")
	_, _ = fmt.Fprintf(w, "pvc_plumber_requests_total %d\n", h.requestsTotal.Load())
	_, _ = fmt.Fprintf(w, "# HELP pvc_plumber_requests_errors_total Total number of failed backup check requests\n")
	_, _ = fmt.Fprintf(w, "# TYPE pvc_plumber_requests_errors_total counter\n")
	_, _ = fmt.Fprintf(w, "pvc_plumber_requests_errors_total %d\n", h.requestsErrors.Load())
}
