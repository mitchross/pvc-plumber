package adopt

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
)

// ssaPatch builds the apply patch used by both Apply and Undo from
// the marshaled unstructured payload. Using client.RawPatch with
// types.ApplyPatchType avoids the deprecated client.Apply sentinel
// while preserving server-side apply semantics for unstructured
// payloads.
func ssaPatch(obj *unstructured.Unstructured) (client.Patch, error) {
	data, err := json.Marshal(obj.Object)
	if err != nil {
		return nil, fmt.Errorf("marshal apply payload: %w", err)
	}
	return client.RawPatch(types.ApplyPatchType, data), nil
}

// DefaultFieldManager is the field-manager string adopt declares for
// every Apply / Undo write. Stable so production code, tests, and
// /audit (when it surfaces managedFields) all reference the same name.
const DefaultFieldManager = "pvc-plumber-adopt"

// ApplyOptions controls one Apply invocation.
type ApplyOptions struct {
	// FieldManager identifies adopt's ownership in the apiserver's
	// managedFields list. Default "pvc-plumber-adopt". Override only
	// for tests.
	FieldManager string

	// DryRun runs the patch through server-side dry-run; no mutation
	// is persisted. ApplyResult.AfterLabels / AfterAnnotations reflect
	// what the apiserver WOULD have committed.
	DryRun bool

	// Confirm permits adopt to force ownership of any v4 gate label
	// or override annotation currently owned by another field manager.
	// Without Confirm=true, a foreign-owned key fails the apply with a
	// ConflictError rather than silently taking it over — even when
	// the live value matches the desired value, because the ownership
	// transfer itself is meaningful.
	Confirm bool
}

// ApplyResult is the outcome of one Apply (or one Undo — both reuse
// this shape since the diffs are symmetric).
type ApplyResult struct {
	// Patched is true when the apiserver accepted the apply (or would
	// have, in DryRun). False when the call was refused (plan verdict,
	// nothing-to-undo) or when no Patch round-trip was made.
	Patched bool

	// Before* are the live label/annotation maps at the moment adopt
	// read the PVC. After* reflect what the live cluster contains
	// (or would contain, under DryRun) after the patch.
	BeforeLabels      map[string]string
	AfterLabels       map[string]string
	BeforeAnnotations map[string]string
	AfterAnnotations  map[string]string

	// Wrote* are the exact key→value pairs adopt declared ownership
	// of in this call. Subset of plan.LabelsToWrite /
	// plan.AnnotationsToWrite for Apply; empty for Undo.
	WroteLabels      map[string]string
	WroteAnnotations map[string]string
}

// ConflictError is returned when Apply would step on a v4 gate label
// or override annotation owned by a different field manager and
// Confirm=false. Each FieldConflict names one contested key.
type ConflictError struct {
	Conflicts []FieldConflict
}

// FieldConflict identifies one contested metadata key.
type FieldConflict struct {
	// Path is "labels/<key>" or "annotations/<key>".
	Path string
	// OwnedBy is the foreign field manager name from managedFields.
	OwnedBy string
	// LiveValue is what's in the cluster today.
	LiveValue string
	// DesiredValue is what adopt would have written.
	DesiredValue string
}

func (e *ConflictError) Error() string {
	if len(e.Conflicts) == 0 {
		return "ownership conflict"
	}
	parts := make([]string, 0, len(e.Conflicts))
	for _, c := range e.Conflicts {
		parts = append(parts, fmt.Sprintf("%s owned by %q (live=%q desired=%q)",
			c.Path, c.OwnedBy, c.LiveValue, c.DesiredValue))
	}
	return "ownership conflict: " + strings.Join(parts, "; ") + "; pass Confirm=true to force ownership transfer"
}

// RefusedError is returned when Apply / Undo refuses without
// contacting the apiserver: blocked plan, already-adopted plan,
// nothing-to-undo, or PVC-gone-between-plan-and-apply.
type RefusedError struct {
	// Verdict, when non-empty, names the plan verdict that caused the
	// refusal.
	Verdict Verdict
	Reason  string
}

func (e *RefusedError) Error() string {
	if e.Verdict != "" {
		return fmt.Sprintf("refused: %s (verdict=%s)", e.Reason, e.Verdict)
	}
	return "refused: " + e.Reason
}

// v4 keys adopt may own. Pre-flight conflict detection walks
// managedFields looking for foreign ownership of any of these.
var ownedLabelKeys = []string{
	labels.LabelEnabled,
	labels.LabelTier,
	labels.LabelManageVolSync,
}

var ownedAnnotationKeys = []string{
	labels.AnnotationUID,
	labels.AnnotationGID,
	labels.AnnotationFSGroup,
	labels.AnnotationSnapshotClass,
	labels.AnnotationCacheCapacity,
	labels.AnnotationStorageClass,
}

// Apply writes plan.LabelsToWrite and plan.AnnotationsToWrite to the
// live PVC via server-side apply with field manager opts.FieldManager.
// Spec, finalizers, ownerReferences, and any non-listed metadata keys
// are structurally untouchable — the patch payload only contains the
// fields adopt explicitly populates.
//
// Refuses VerdictBlocked and VerdictAlreadyAdopted plans. Only
// VerdictSafeToAdopt and VerdictSafeToAdoptWithWarnings are accepted.
//
// Conflict semantics: if any owned key is currently owned by a
// different field manager (including same-value cases), Apply returns
// *ConflictError unless opts.Confirm == true.
//
// Dry-run semantics: opts.DryRun=true uses server-side dry-run; the
// apiserver runs the full admission chain and returns the would-be
// merged object without persisting. AfterLabels / AfterAnnotations
// reflect that response.
//
// Idempotency: same Apply twice is a clean no-op at the apiserver
// level. ApplyResult.Patched stays true; Before == After.
func Apply(ctx context.Context, c client.Client, plan Plan, opts ApplyOptions) (ApplyResult, error) {
	fm := opts.FieldManager
	if fm == "" {
		fm = DefaultFieldManager
	}

	// Refuse non-applicable verdicts BEFORE touching the apiserver.
	switch plan.Verdict {
	case VerdictBlocked:
		return ApplyResult{}, &RefusedError{
			Verdict: plan.Verdict,
			Reason:  "plan is blocked; resolve blockers before applying",
		}
	case VerdictAlreadyAdopted:
		return ApplyResult{}, &RefusedError{
			Verdict: plan.Verdict,
			Reason:  "plan reports already-adopted; nothing to apply",
		}
	case VerdictSafeToAdopt, VerdictSafeToAdoptWithWarnings:
		// proceed
	default:
		return ApplyResult{}, &RefusedError{
			Verdict: plan.Verdict,
			Reason:  fmt.Sprintf("unsupported verdict %q", plan.Verdict),
		}
	}

	// Re-fetch the PVC for BeforeLabels and conflict pre-flight.
	live := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: plan.PVC.Namespace, Name: plan.PVC.Name}
	if err := c.Get(ctx, key, live); err != nil {
		if apierrors.IsNotFound(err) {
			return ApplyResult{}, &RefusedError{
				Reason: fmt.Sprintf("PVC %s/%s not found at apply time", plan.PVC.Namespace, plan.PVC.Name),
			}
		}
		return ApplyResult{}, fmt.Errorf("re-read PVC %s/%s: %w", plan.PVC.Namespace, plan.PVC.Name, err)
	}

	result := ApplyResult{
		BeforeLabels:      copyMap(live.Labels),
		BeforeAnnotations: copyMap(live.Annotations),
		WroteLabels:       copyMap(plan.LabelsToWrite),
		WroteAnnotations:  copyMap(plan.AnnotationsToWrite),
	}

	// Pre-flight conflict detection. Walks managedFields looking for
	// any non-fm entry that owns one of the keys we'd write. Treats
	// same-value-different-FM as a conflict by design — ownership
	// transfer is meaningful.
	if !opts.Confirm {
		conflicts := detectConflicts(live, fm, plan)
		if len(conflicts) > 0 {
			return result, &ConflictError{Conflicts: conflicts}
		}
	}

	// Construct the patch payload: ONLY apiVersion, kind, namespace,
	// name, labels, and (when present) annotations. No spec, no
	// finalizers, no ownerReferences — what isn't in the payload
	// cannot be claimed or mutated by SSA.
	patch := buildApplyPayload(plan.PVC.Namespace, plan.PVC.Name, plan.LabelsToWrite, plan.AnnotationsToWrite)

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
		return result, fmt.Errorf("apply PVC %s/%s: %w", plan.PVC.Namespace, plan.PVC.Name, err)
	}

	// Populate After* from the apiserver response. For DryRun the
	// patch object itself contains the merged result. For live, do
	// one fresh Get so After* reflects committed state including any
	// admission-time mutations.
	if opts.DryRun {
		result.AfterLabels = copyMap(patch.GetLabels())
		result.AfterAnnotations = copyMap(patch.GetAnnotations())
	} else {
		after := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, key, after); err != nil {
			return result, fmt.Errorf("re-read PVC after apply: %w", err)
		}
		result.AfterLabels = copyMap(after.Labels)
		result.AfterAnnotations = copyMap(after.Annotations)
	}
	result.Patched = true
	return result, nil
}

// buildApplyPayload constructs the unstructured payload Apply / Undo
// sends to the apiserver. The returned object contains ONLY the fields
// adopt is allowed to write. Spec, status, finalizers, ownerReferences,
// uid, resourceVersion are all absent — SSA cannot claim ownership of
// what isn't in the payload.
//
// labels may be empty (signals "release all label keys currently owned
// by this FM"). annotations is included only when non-nil so an Apply
// that doesn't need annotations doesn't claim ownership of the
// annotations map at all.
func buildApplyPayload(namespace, name string, lbls, annots map[string]string) *unstructured.Unstructured {
	patch := &unstructured.Unstructured{}
	patch.SetAPIVersion("v1")
	patch.SetKind("PersistentVolumeClaim")
	patch.SetNamespace(namespace)
	patch.SetName(name)

	meta := map[string]interface{}{
		"namespace": namespace,
		"name":      name,
	}
	if lbls != nil {
		labelMap := make(map[string]interface{}, len(lbls))
		for k, v := range lbls {
			labelMap[k] = v
		}
		meta["labels"] = labelMap
	}
	if annots != nil {
		annMap := make(map[string]interface{}, len(annots))
		for k, v := range annots {
			annMap[k] = v
		}
		meta["annotations"] = annMap
	}
	patch.Object["metadata"] = meta
	return patch
}

// detectConflicts walks live.ManagedFields looking for any non-fm
// entry that owns one of the keys we'd write. Returns one
// FieldConflict per contested key. Same-value-different-FM is still a
// conflict by design.
func detectConflicts(live *corev1.PersistentVolumeClaim, fm string, plan Plan) []FieldConflict {
	if len(live.ManagedFields) == 0 {
		return nil
	}

	// Build the desired-write maps for value lookup.
	desiredLabels := plan.LabelsToWrite
	desiredAnnotations := plan.AnnotationsToWrite

	var conflicts []FieldConflict
	seen := map[string]struct{}{}

	for _, mf := range live.ManagedFields {
		if mf.Manager == fm {
			continue // our own ownership; no conflict with self
		}
		if mf.FieldsV1 == nil {
			continue
		}
		var fields map[string]interface{}
		if err := json.Unmarshal(mf.FieldsV1.Raw, &fields); err != nil {
			// Malformed managedFields entry — skip; not adopt's job
			// to fix the apiserver's internal state.
			continue
		}
		metaFields, _ := fields["f:metadata"].(map[string]interface{})
		if metaFields == nil {
			continue
		}

		// Label conflicts.
		labelFields, _ := metaFields["f:labels"].(map[string]interface{})
		if labelFields != nil && desiredLabels != nil {
			for k, desired := range desiredLabels {
				if _, owned := labelFields["f:"+k]; !owned {
					continue
				}
				path := "labels/" + k
				if _, dup := seen[path]; dup {
					continue
				}
				seen[path] = struct{}{}
				conflicts = append(conflicts, FieldConflict{
					Path:         path,
					OwnedBy:      mf.Manager,
					LiveValue:    live.Labels[k],
					DesiredValue: desired,
				})
			}
		}

		// Annotation conflicts.
		annotFields, _ := metaFields["f:annotations"].(map[string]interface{})
		if annotFields != nil && desiredAnnotations != nil {
			for k, desired := range desiredAnnotations {
				if _, owned := annotFields["f:"+k]; !owned {
					continue
				}
				path := "annotations/" + k
				if _, dup := seen[path]; dup {
					continue
				}
				seen[path] = struct{}{}
				conflicts = append(conflicts, FieldConflict{
					Path:         path,
					OwnedBy:      mf.Manager,
					LiveValue:    live.Annotations[k],
					DesiredValue: desired,
				})
			}
		}
	}

	// Deterministic order for stable test golden output.
	sort.Slice(conflicts, func(i, j int) bool { return conflicts[i].Path < conflicts[j].Path })
	return conflicts
}

func copyMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
