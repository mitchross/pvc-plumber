package webhook

import (
	"context"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// Annotation that explains why an operator opted out of restore-on-create.
// Required (non-empty) whenever skipRestoreAnnot is "true". See
// Kyverno rule 4 (`require-skip-restore-reason`) — a stale skip-restore
// annotation in Git silently disables the fail-closed gate forever, so the
// operator must justify it.
const skipRestoreReasonAnnot = "volsync.backup/skip-restore-reason"

// Deny messages — kept as constants so they're easy to keep in sync with the
// Kyverno YAML these handlers replace. Operators searching for a deny
// message in alerts/PR review will land on these strings.
const (
	denyMsgUnknown = "PVC Plumber could not make an authoritative restore/fresh decision " +
		"for this backup-labeled PVC. PVC creation is denied to prevent silent " +
		"empty-volume initialization when backup state is unknown. ArgoCD will retry."

	denyMsgRestoreNoRef = "PVC Plumber reports a Kopia backup exists for this PVC " +
		"(decision=restore, authoritative), but the admitted object is missing " +
		"the expected dataSourceRef pointing at ReplicationDestination <pvc>-backup. " +
		"This typically means the mutate webhook's pvc-plumber call failed or " +
		"returned non-authoritative while the validate-side call succeeded — " +
		"admitting the PVC would provision an empty volume over restorable backup data. " +
		"ArgoCD will retry; if pvc-plumber stays flaky, check its pods/logs. " +
		"To intentionally start fresh, set annotation volsync.backup/skip-restore=true on the PVC."

	denyMsgSkipRestoreNoReason = "volsync.backup/skip-restore=true requires a non-empty " +
		"volsync.backup/skip-restore-reason annotation. Skipping restore disables the " +
		"fail-closed admission gate and could initialize an empty volume over a real " +
		"Kopia backup. Add an annotation explaining why, e.g. " +
		"volsync.backup/skip-restore-reason=\"<reason>\"."
)

// PVCValidator is the validating admission handler for PersistentVolumeClaim
// CREATE requests. It implements the fail-CLOSED safety gate that pairs with
// the fail-OPEN PVCMutator. Three independent denials live here, mirroring
// Kyverno rules 1, 3, and 4 from `volsync-pvc-backup-restore`:
//
//  1. Unknown / non-authoritative / errored Kopia check → DENY (rule 1).
//     False-negative here = empty volume admitted over restorable data.
//  2. Authoritative restore decision but missing/wrong dataSourceRef → DENY
//     (rule 3, belt-and-suspenders for rule 2 in the mutator).
//  3. skip-restore=true without a non-empty reason → DENY (rule 4).
//
// Like the mutator, this handler runs its own Kopia check rather than
// trusting state set by the mutator. Two independent checks close the race
// where mutate's call returns `unknown` (transient) but validate's succeeds
// with `restore` — without the rule 3 cross-check, the PVC would be admitted
// without dataSourceRef and Longhorn would provision empty over a real backup.
type PVCValidator struct {
	Decoder          admission.Decoder
	Kopia            kopiaClient
	SystemNamespaces map[string]struct{}
}

// Handle implements admission.Handler.
func (h *PVCValidator) Handle(ctx context.Context, req admission.Request) admission.Response {
	if req.Operation != admissionv1.Create {
		return admission.Allowed("")
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := h.Decoder.Decode(req, pvc); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	if !hasBackupLabel(pvc) {
		return admission.Allowed("")
	}
	if _, ok := h.SystemNamespaces[pvc.Namespace]; ok {
		return admission.Allowed("")
	}

	// skip-restore is the explicit "nuke and start fresh" escape hatch. It
	// disables the rest of the gate, so we require an audit trail (rule 4).
	if pvc.Annotations[skipRestoreAnnot] == "true" {
		if pvc.Annotations[skipRestoreReasonAnnot] == "" {
			return admission.Denied(denyMsgSkipRestoreNoReason)
		}
		return admission.Allowed("")
	}

	result := h.Kopia.CheckBackupExists(ctx, pvc.Namespace, pvc.Name)

	// Rule 1 fail-closed: any flavour of "we don't know" denies. Note
	// `Decision == DecisionUnknown` is a deliberate triple-check alongside
	// `!Authoritative` and a non-empty Error — backends can populate any
	// subset of those fields, and we want all three paths to converge on
	// the same denial.
	if result.Error != "" || !result.Authoritative || result.Decision == backend.DecisionUnknown {
		return admission.Denied(denyMsgUnknown)
	}

	// Rule 3 fail-closed: if the authoritative check says restore, the
	// admitted object must already carry the matching dataSourceRef. The
	// mutator runs first and would have injected it on a successful path;
	// missing here means the mutator's call returned non-authoritative
	// (rare but possible if the Kopia connection just recovered between
	// the two webhook calls).
	if result.Decision == backend.DecisionRestore {
		if !hasExpectedDataSourceRef(pvc) {
			return admission.Denied(denyMsgRestoreNoRef)
		}
	}

	return admission.Allowed("")
}

// hasExpectedDataSourceRef returns true iff `pvc.Spec.DataSourceRef` exactly
// matches the shape the mutator injects: APIGroup=volsync.backube,
// Kind=ReplicationDestination, Name=<pvc>-backup. Any deviation (operator
// supplied a different data source, mutator never ran, name mismatch) fails
// closed — the corresponding Kyverno rule 3 deny block uses NotEquals on each
// of the three fields, so any single mismatch is a denial.
func hasExpectedDataSourceRef(pvc *corev1.PersistentVolumeClaim) bool {
	ref := pvc.Spec.DataSourceRef
	if ref == nil {
		return false
	}
	if ref.APIGroup == nil || *ref.APIGroup != dataSourceAPIGroup {
		return false
	}
	if ref.Kind != dataSourceKind {
		return false
	}
	if ref.Name != pvc.Name+dataSourceSuffix {
		return false
	}
	return true
}
