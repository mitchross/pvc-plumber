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

type Handler struct {
	backend        BackendClient
	logger         *slog.Logger
	requestsTotal  atomic.Int64
	requestsErrors atomic.Int64
}

func New(backend BackendClient, logger *slog.Logger) *Handler {
	return &Handler{
		backend: backend,
		logger:  logger,
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
	// Same as healthz for now
	h.HandleHealthz(w, r)
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
