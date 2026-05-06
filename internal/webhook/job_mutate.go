package webhook

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// JobMutator is the mutating admission handler for batch/v1 Job CREATE
// requests. In Phase 2 it will match VolSync mover jobs
// (`app.kubernetes.io/created-by=volsync`) and inject the NFS volume +
// volumeMount, replacing the current Kyverno volsync-nfs-inject policy.
type JobMutator struct{}

// Handle is the Phase 1 stub — admit everything unchanged.
func (h *JobMutator) Handle(_ context.Context, _ admission.Request) admission.Response {
	return admission.Allowed("")
}
