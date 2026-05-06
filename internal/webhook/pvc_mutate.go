// Package webhook contains admission webhook handlers for pvc-plumber.
//
// Phase 1 scaffold: handlers are stubs returning admission.Allowed("") so the
// webhook server can register routes and serve healthchecks. Real logic
// (Kopia check, dataSourceRef injection, validation gates, NFS mount
// injection) lands in Phase 2.
package webhook

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// PVCMutator is the mutating admission handler for PersistentVolumeClaim
// CREATE requests. In Phase 2 it will call the Kopia client and inject
// `spec.dataSourceRef` when an authoritative restore is available.
type PVCMutator struct{}

// Handle is the Phase 1 stub — admit everything unchanged.
func (h *PVCMutator) Handle(_ context.Context, _ admission.Request) admission.Response {
	return admission.Allowed("")
}
