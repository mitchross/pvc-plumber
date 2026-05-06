// Package webhook contains admission webhook handlers for pvc-plumber.
//
// These handlers replace the runtime decision logic that previously lived in
// the Kyverno ClusterPolicy `volsync-pvc-backup-restore` (rules 1, 2, 3, 4)
// and `volsync-nfs-inject`. The PVC handlers split into a fail-OPEN mutator
// (PVCMutator) and a fail-CLOSED validator (PVCValidator). The Job handler
// (JobMutator) injects the shared NFS Kopia repository volume into VolSync
// mover pods.
package webhook

import (
	"context"
	"encoding/json"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// kopiaClient is the narrow surface PVC handlers need from the Kopia client.
// Defining it here lets tests inject a fake without spinning up a real Kopia
// repository or wrestling with the concrete `*kopia.Client` constructor.
type kopiaClient interface {
	CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult
}

// Backup label values that opt a PVC into VolSync backup. Anything outside
// this set is invisible to the webhook and admitted unchanged.
const (
	backupLabelKey   = "backup"
	backupLabelHour  = "hourly"
	backupLabelDay   = "daily"
	skipRestoreAnnot = "volsync.backup/skip-restore"

	dataSourceAPIGroup = "volsync.backube"
	dataSourceKind     = "ReplicationDestination"
	dataSourceSuffix   = "-backup"
)

// hasBackupLabel returns true if the PVC carries `backup=hourly|daily`. The
// admission controllers only act on these PVCs; everything else is admitted
// untouched (the webhook configurations also filter by label, so this is
// defense-in-depth against misconfigured webhook objectSelectors).
func hasBackupLabel(pvc *corev1.PersistentVolumeClaim) bool {
	v, ok := pvc.Labels[backupLabelKey]
	if !ok {
		return false
	}
	return v == backupLabelHour || v == backupLabelDay
}

// PVCMutator is the mutating admission handler for PersistentVolumeClaim
// CREATE requests. It mirrors Kyverno rule 2 (`add-datasource-if-backup-exists`):
// when pvc-plumber reports an authoritative restore decision, it injects
// `spec.dataSourceRef` so VolSync's CSI populator can hydrate the new PV from
// the matching ReplicationDestination.
//
// Failure mode: fail-OPEN. If the Kopia check errors out or returns a
// non-authoritative result, this mutator admits the PVC unchanged and lets
// the validator (PVCValidator) make the final fail-CLOSED decision. Doing it
// this way matches the Kyverno behaviour and keeps the mutate webhook from
// blocking creates over transient plumber flakiness — the validator is the
// safety gate.
type PVCMutator struct {
	Decoder          admission.Decoder
	Kopia            kopiaClient
	SystemNamespaces map[string]struct{}
}

// Handle implements admission.Handler. See the type comment for semantics.
func (h *PVCMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	if req.Operation != admissionv1.Create {
		return admission.Allowed("")
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := h.Decoder.Decode(req, pvc); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Out-of-scope PVCs: no backup label, system namespace, explicit opt-out,
	// or operator-supplied dataSourceRef. The Kyverno rule 2 preconditions
	// drop on the same conditions.
	if !hasBackupLabel(pvc) {
		return admission.Allowed("")
	}
	if pvc.Annotations[skipRestoreAnnot] == "true" {
		return admission.Allowed("")
	}
	if _, ok := h.SystemNamespaces[pvc.Namespace]; ok {
		return admission.Allowed("")
	}
	if pvc.Spec.DataSourceRef != nil {
		// Caller already chose a data source — never overwrite. Matches the
		// Kyverno `dataSourceRef || ''` precondition.
		return admission.Allowed("")
	}

	result := h.Kopia.CheckBackupExists(ctx, pvc.Namespace, pvc.Name)

	// Fail-OPEN: anything short of an authoritative restore decision admits
	// the PVC unchanged. The validator runs a second independent Kopia check
	// and is fail-CLOSED, so a transient error here just defers the gate.
	if result.Error != "" || !result.Authoritative || result.Decision != backend.DecisionRestore {
		return admission.Allowed("")
	}

	pvc.Spec.DataSourceRef = &corev1.TypedObjectReference{
		APIGroup: ptr.To(dataSourceAPIGroup),
		Kind:     dataSourceKind,
		Name:     pvc.Name + dataSourceSuffix,
	}

	marshaled, err := json.Marshal(pvc)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}
