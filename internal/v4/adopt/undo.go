package adopt

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// UndoOptions controls one Undo invocation.
type UndoOptions struct {
	FieldManager string
	DryRun       bool

	// Confirm: like ApplyOptions.Confirm, permits adopt to force
	// ownership when SSA's relinquish would conflict. Rare for Undo
	// since adopt only relinquishes keys it already owns, but the
	// flag is wired through for symmetry with Apply.
	Confirm bool

	// RemoveOverrideAnnotations: when true, undo also relinquishes
	// ownership of the six override annotation keys. Default false —
	// undo strips only the three gate labels and leaves override
	// annotations alone so operators may inspect them after rollback.
	RemoveOverrideAnnotations bool
}

// Undo relinquishes adopt's field-manager ownership of the v4 gate
// labels (and optionally the override annotations) by re-applying an
// SSA payload with those keys absent. SSA semantics: keys previously
// owned by opts.FieldManager that are not in the new apply are
// released; if no other field manager owns them, they are deleted.
//
// Undo never touches:
//   - labels owned by any other field manager (Argo, Helm, kubectl,
//     operators) — the FM identity is the protection;
//   - annotations outside the override-key allow-list;
//   - PVC spec, finalizers, ownerReferences;
//   - ReplicationSource / ReplicationDestination — rollback of RS/RD
//     remains a manual Git/Argo operation per the cutover runbook.
//
// If the PVC has none of the v4 gate labels live AND (override
// annotation cleanup is requested AND no override annotations exist),
// Undo returns Patched=false without contacting the apiserver.
func Undo(ctx context.Context, c client.Client, namespace, pvcName string, opts UndoOptions) (ApplyResult, error) {
	fm := opts.FieldManager
	if fm == "" {
		fm = DefaultFieldManager
	}

	live := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: namespace, Name: pvcName}
	if err := c.Get(ctx, key, live); err != nil {
		if apierrors.IsNotFound(err) {
			return ApplyResult{}, &RefusedError{
				Reason: fmt.Sprintf("PVC %s/%s not found", namespace, pvcName),
			}
		}
		return ApplyResult{}, fmt.Errorf("read PVC %s/%s: %w", namespace, pvcName, err)
	}

	result := ApplyResult{
		BeforeLabels:      copyMap(live.Labels),
		BeforeAnnotations: copyMap(live.Annotations),
	}

	// Nothing-to-undo check. The conservative interpretation: if
	// none of the keys adopt would release are live, skip the round
	// trip. Match annotation removal scope to opts.RemoveOverrideAnnotations.
	if !hasAnyKey(live.Labels, ownedLabelKeys) {
		if !opts.RemoveOverrideAnnotations || !hasAnyKey(live.Annotations, ownedAnnotationKeys) {
			result.AfterLabels = result.BeforeLabels
			result.AfterAnnotations = result.BeforeAnnotations
			return result, nil
		}
	}

	// SSA payload: empty labels (always — undo always relinquishes
	// gate labels). Annotations only when override-annotation removal
	// is requested. Sending an empty annotations map relinquishes
	// adopt's ownership of every annotation it had previously claimed
	// (the six override keys, never more — buildApplyPayload was the
	// only producer).
	var emptyAnnotations map[string]string
	if opts.RemoveOverrideAnnotations {
		emptyAnnotations = map[string]string{}
	}
	patch := buildApplyPayload(namespace, pvcName, map[string]string{}, emptyAnnotations)

	patchOpts := []client.PatchOption{client.FieldOwner(fm)}
	if opts.Confirm {
		patchOpts = append(patchOpts, client.ForceOwnership)
	}
	if opts.DryRun {
		patchOpts = append(patchOpts, client.DryRunAll)
	}

	applyPatch, err := ssaPatch(patch)
	if err != nil {
		return result, err
	}
	if err := c.Patch(ctx, patch, applyPatch, patchOpts...); err != nil {
		return result, fmt.Errorf("undo PVC %s/%s: %w", namespace, pvcName, err)
	}

	if opts.DryRun {
		result.AfterLabels = copyMap(patch.GetLabels())
		result.AfterAnnotations = copyMap(patch.GetAnnotations())
	} else {
		after := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, key, after); err != nil {
			return result, fmt.Errorf("re-read PVC after undo: %w", err)
		}
		result.AfterLabels = copyMap(after.Labels)
		result.AfterAnnotations = copyMap(after.Annotations)
	}
	result.Patched = true
	return result, nil
}

// hasAnyKey returns true if any of keys appears in m.
func hasAnyKey(m map[string]string, keys []string) bool {
	for _, k := range keys {
		if _, ok := m[k]; ok {
			return true
		}
	}
	return false
}
