package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/mitchross/pvc-plumber/internal/controller"
)

// ParitySnapshotter is the read-only surface AuditHandler needs from
// the controller's parity Store. Defined as a narrow interface so the
// /audit handler is testable with a tiny in-package fake — no
// reconciler, no kube client, no backend dependencies — and so the
// production Store (which satisfies this implicitly via its Snapshot
// method) can be swapped for an alternative report source in the
// future without touching this file.
type ParitySnapshotter interface {
	Snapshot() controller.ParityReport
}

// AuditHandler serves the GET /audit endpoint: a moment-in-time
// parity report produced by the V4AuditReconciler and held in the
// in-memory controller Store.
//
// Backend-independent by construction. The handler performs no reads
// or writes against the cluster, the configured storage backend
// (kopia/S3/RustFS), or persistent configuration — its only data
// source is Store.Snapshot(), which acquires an internal RLock and
// returns a deep copy. That means concurrent GETs are inherently
// safe and the JSON encoding pass cannot mutate the live Store.
//
// No authentication. This endpoint is currently exposed only on the
// operator's internal cluster Service. If it ever fronts a publicly
// reachable surface, wrap the registered http.Handler in an auth
// middleware at registration time rather than baking auth checks
// into this handler.
type AuditHandler struct {
	snapshotter ParitySnapshotter
	logger      *slog.Logger
}

// NewAuditHandler constructs an AuditHandler. The snapshotter must be
// non-nil; logger may be nil (log lines simply become no-ops).
func NewAuditHandler(snapshotter ParitySnapshotter, logger *slog.Logger) *AuditHandler {
	return &AuditHandler{snapshotter: snapshotter, logger: logger}
}

// ServeHTTP implements http.Handler.
//
// Method routing:
//   - GET  → 200, application/json, ParityReport body.
//   - HEAD → 200, application/json, no body (snapshot is still taken
//     so reachability probes exercise the full code path).
//   - anything else → 405 with `Allow: GET, HEAD`.
func (h *AuditHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		// fall through
	default:
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Snapshot() is a pure read of in-memory state and returns a
	// freshly-allocated ParityReport with deep-copied slices/maps.
	// Nothing the encoder does below can affect the underlying Store.
	report := h.snapshotter.Snapshot()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")

	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	if err := json.NewEncoder(w).Encode(report); err != nil {
		// Status was implicitly 200 once the encoder wrote the first
		// byte. Best-effort log; no recovery path is meaningful here.
		if h.logger != nil {
			h.logger.Warn("audit endpoint encode failed", "error", err)
		}
	}
}
