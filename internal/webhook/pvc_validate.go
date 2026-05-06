package webhook

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// PVCValidator is the validating admission handler for PersistentVolumeClaim
// CREATE requests. In Phase 2 it will fail-closed when pvc-plumber cannot
// produce an authoritative restore decision, and verify the
// dataSourceRef shape when one is required.
type PVCValidator struct{}

// Handle is the Phase 1 stub — admit everything unchanged.
func (h *PVCValidator) Handle(_ context.Context, _ admission.Request) admission.Response {
	return admission.Allowed("")
}
