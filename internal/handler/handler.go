package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// JSON keys used in /healthz and /readyz responses.
const (
	healthStatusKey = "status"
	healthStatusOK  = "ok"
)

// BackendClient interface for dependency injection and testing
type BackendClient interface {
	CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult
}

// HealthChecker is an optional interface backends can implement for readiness checks.
type HealthChecker interface {
	HealthCheck(ctx context.Context) error
}

// DedupCounter is an optional interface a wrapped backend can implement to
// report how many /exists lookups were collapsed by singleflight (i.e.
// served from a concurrent leader's result rather than a fresh upstream
// call). Exposed via the pvcplumber_exists_singleflight_dedup_total metric.
type DedupCounter interface {
	DedupedCalls() int64
}

type Handler struct {
	backend        BackendClient
	healthChecker  HealthChecker
	logger         *slog.Logger
	requestTimeout time.Duration
	requestsTotal  atomic.Int64
	requestsErrors atomic.Int64
	metricsMu      sync.Mutex
	backupChecks   map[metricKey]int64
}

type metricKey struct {
	backend  string
	decision string
}

func New(backend BackendClient, logger *slog.Logger) *Handler {
	h := &Handler{
		backend:      backend,
		logger:       logger,
		backupChecks: make(map[metricKey]int64),
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
		backupChecks:  make(map[metricKey]int64),
	}
}

func (h *Handler) SetRequestTimeout(timeout time.Duration) {
	h.requestTimeout = timeout
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
		_ = json.NewEncoder(w).Encode(backend.CheckResult{
			Exists:        false,
			Decision:      backend.DecisionUnknown,
			Authoritative: false,
			Error:         "invalid path format, expected /exists/{namespace}/{pvc}",
		})
		return
	}

	h.logger.Info("checking backup", "namespace", namespace, "pvc", pvc)

	ctx := r.Context()
	if h.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.requestTimeout)
		defer cancel()
	}

	result := h.backend.CheckBackupExists(ctx, namespace, pvc)

	if result.Error != "" {
		h.requestsErrors.Add(1)
	}
	h.recordBackupCheck(result)

	h.logger.Info("backup check complete",
		"namespace", namespace,
		"pvc", pvc,
		"exists", result.Exists,
		"decision", result.Decision,
		"authoritative", result.Authoritative,
		"backend", result.Backend)

	w.Header().Set("Content-Type", "application/json")
	if result.Error != "" || !result.Authoritative || result.Decision == backend.DecisionUnknown {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(result)
}

func (h *Handler) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{healthStatusKey: healthStatusOK})
}

func (h *Handler) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	if h.healthChecker != nil {
		if err := h.healthChecker.HealthCheck(r.Context()); err != nil {
			h.logger.Warn("readiness check failed", "error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{
				healthStatusKey: "not ready",
				"error":         err.Error(),
			})
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{healthStatusKey: healthStatusOK})
}

func (h *Handler) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintf(w, "# HELP pvc_plumber_requests_total Total number of backup check requests\n")
	_, _ = fmt.Fprintf(w, "# TYPE pvc_plumber_requests_total counter\n")
	_, _ = fmt.Fprintf(w, "pvc_plumber_requests_total %d\n", h.requestsTotal.Load())
	_, _ = fmt.Fprintf(w, "# HELP pvc_plumber_requests_errors_total Total number of failed backup check requests\n")
	_, _ = fmt.Fprintf(w, "# TYPE pvc_plumber_requests_errors_total counter\n")
	_, _ = fmt.Fprintf(w, "pvc_plumber_requests_errors_total %d\n", h.requestsErrors.Load())
	_, _ = fmt.Fprintf(w, "# HELP pvc_plumber_backup_check_total Total number of backup check results by backend and decision\n")
	_, _ = fmt.Fprintf(w, "# TYPE pvc_plumber_backup_check_total counter\n")
	for _, sample := range h.backupCheckSamples() {
		_, _ = fmt.Fprintf(w, "pvc_plumber_backup_check_total{backend=%q,decision=%q} %d\n", sample.backend, sample.decision, sample.value)
	}
	if dc, ok := h.backend.(DedupCounter); ok {
		_, _ = fmt.Fprintf(w, "# HELP pvcplumber_exists_singleflight_dedup_total Total number of /exists lookups collapsed by singleflight (served from a concurrent leader's result)\n")
		_, _ = fmt.Fprintf(w, "# TYPE pvcplumber_exists_singleflight_dedup_total counter\n")
		_, _ = fmt.Fprintf(w, "pvcplumber_exists_singleflight_dedup_total %d\n", dc.DedupedCalls())
	}
}

func (h *Handler) recordBackupCheck(result backend.CheckResult) {
	backendName := result.Backend
	if backendName == "" {
		backendName = "unknown"
	}
	decision := result.Decision
	if decision == "" {
		decision = backend.DecisionUnknown
	}

	h.metricsMu.Lock()
	h.backupChecks[metricKey{backend: backendName, decision: decision}]++
	h.metricsMu.Unlock()
}

type metricSample struct {
	backend  string
	decision string
	value    int64
}

func (h *Handler) backupCheckSamples() []metricSample {
	h.metricsMu.Lock()
	defer h.metricsMu.Unlock()

	samples := make([]metricSample, 0, len(h.backupChecks))
	for key, value := range h.backupChecks {
		samples = append(samples, metricSample{
			backend:  key.backend,
			decision: key.decision,
			value:    value,
		})
	}
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].backend == samples[j].backend {
			return samples[i].decision < samples[j].decision
		}
		return samples[i].backend < samples[j].backend
	})
	return samples
}
