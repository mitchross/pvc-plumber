package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/mode"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// Phase 5 — Patch 3: V4AuditReconciler skeleton/orchestration.
//
// Orchestrates the Patch 1 (ParityEntry / Store) and Patch 2
// (ClassifyLabelSource / ComputeExpected / ClassifyOwner / DecideAction)
// pure helpers with the only cluster-touching code path Phase 5 owns:
// reading PVC + RS + RD via the embedded client.
//
// Contract:
//
//   - Performs zero writes. The reconciler never calls Create, Update,
//     Patch, Delete, or DeleteAllOf. In audit-mode production deployments
//     this is double-checked by the auditclient wrapper around the
//     embedded client, but the design intent is that this reconciler
//     would do no harm even given a raw write-capable client.
//
//   - Reads three things per Reconcile: the PVC itself (corev1), and the
//     expected RS and RD (volsync.backube/v1alpha1, as unstructured).
//
//   - Writes one Store entry per Reconcile. The entry describes the
//     full per-PVC parity verdict the /audit endpoint (Patch 4) will
//     return.
//
//   - On PVC NotFound: deletes the Store entry. Rationale: the Store
//     models the current cluster's PVC inventory. A deleted PVC is no
//     longer relevant to "what does the operator think SHOULD exist?";
//     keeping a tombstone would confuse operators reading the report.
//     If a future phase wants historical visibility, push that into a
//     separate audit log / event surface rather than the Store.
//
//   - System namespaces (kube-system, volsync-system, etc.) are
//     short-circuited at the top of Reconcile so they never appear in
//     the report. The PVCReconciler v3 does the same for the same
//     reason (defense-in-depth against webhook namespaceSelector drift).
//
// This reconciler is NOT registered by cmd/operator/main.go yet
// (Patch 5 does that). Currently it exists as a standalone testable unit.

// V4AuditReconciler watches PVCs and writes parity entries to the Store.
// In audit mode (the only mode Phase 5 cares about today) every Reconcile
// is a pure observe-and-classify operation.
type V4AuditReconciler struct {
	client.Client

	// Store is the in-memory parity registry shared with the /audit
	// HTTP handler (Patch 4). Must be non-nil; the reconciler will
	// panic on first use otherwise.
	Store *Store

	// NamingStrategy picks the convention used for ExpectedState.
	// Default (zero value) is naming.StrategyBareDst, which matches
	// the talos repo's inline pattern: RS=<pvc>, RD=<pvc>-dst.
	NamingStrategy naming.Strategy

	// DefaultRepoSecret is the kopia repository Secret name to expect
	// in the inline RS/RD when no per-PVC override is set. Typically
	// "volsync-kopia-repository". When empty, the reconciler falls
	// back to DefaultRepoSecretName at compute time.
	DefaultRepoSecret string

	// SystemNamespaces is the set of namespaces that are NEVER reconciled
	// (no Store entry produced). Membership is checked via
	// `_, ok := SystemNamespaces[req.Namespace]`. Defense-in-depth — the
	// audit reconciler should never even attempt to observe PVCs in
	// kube-system / volsync-system / etc.
	SystemNamespaces map[string]struct{}

	// OperatorMode is the string written to ParityEntry.Mode. Typically
	// "audit". Informational only — the reconciler's behavior does not
	// depend on it.
	OperatorMode string

	// Now is injected for deterministic tests. nil → time.Now.
	Now func() time.Time
}

// SetupWithManager registers the reconciler with the controller-runtime
// manager. The controller watches corev1.PersistentVolumeClaim only;
// RS / RD are read on demand in Reconcile, not watched (the v4 audit
// model doesn't need to react to RS/RD events — those would only
// indicate cluster drift that gets picked up on the next PVC reconcile
// anyway, and watching them would multiply the reconcile rate without
// benefit in audit mode).
func (r *V4AuditReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolumeClaim{}).
		Named("pvc-plumber-v4-audit").
		Complete(r)
}

// Reconcile is the entrypoint controller-runtime calls for each PVC event.
// Pure observation + Store update; never writes to the cluster.
//
// Flow:
//
//  1. System namespace?       → short-circuit, no Store change.
//  2. PVC Get NotFound?       → Store.Delete, return.
//  3. PVC Get other error?    → return error for requeue, Store unchanged.
//  4. Parse labels/annotations → labels.Spec.
//  5. Classify label source   → LabelSource.
//  6. Compute expected state  → ExpectedState (always, even for
//     not-opted-in PVCs — the report shows
//     what the v4 names WOULD be).
//  7. Observe current RS/RD   → CurrentState.
//  8. Classify owner          → OwnerClassification.
//  9. Decide action           → ActionDecision.
//  10. Assemble ParityEntry, Store.Set, return.
func (r *V4AuditReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pvc", req.NamespacedName)

	// Step 1: system-namespace short-circuit.
	if _, isSystem := r.SystemNamespaces[req.Namespace]; isSystem {
		logger.V(1).Info("v4 audit: system namespace, skipping")
		return ctrl.Result{}, nil
	}

	// Step 2 + 3: fetch the PVC.
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, req.NamespacedName, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			r.Store.Delete(req.Namespace, req.Name)
			logger.V(1).Info("v4 audit: PVC gone, removed Store entry")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get PVC %s/%s: %w", req.Namespace, req.Name, err)
	}

	// Step 4-5: parse + classify.
	spec := labels.Parse(pvc.GetLabels(), pvc.GetAnnotations())
	source := ClassifyLabelSource(spec)

	// Step 6: compute expected state. We compute even for not-opted-in
	// PVCs so the /audit report can show "if you were to opt this in,
	// this is what the v4 children would look like." DecideAction will
	// still classify the action as skipped-not-opted-in.
	expected := ComputeExpected(req.Namespace, req.Name, spec, r.NamingStrategy, r.DefaultRepoSecret)

	// Step 7: observe current RS/RD.
	current, err := r.observeCurrent(ctx, req.Namespace, expected)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Step 8-9: classify owner + decide action.
	owner := ClassifyOwner(current, expected)
	decision := DecideAction(spec, source, current, expected, owner)

	// Step 10: assemble + Store.
	entry := ParityEntry{
		Namespace:      req.Namespace,
		PVC:            req.Name,
		Mode:           r.modeStr(),
		Tier:           spec.Tier.String(),
		LabelSource:    source,
		BackupIdentity: expected.BackupIdentity,
		Expected:       expected,
		Current:        current,
		Owner:          owner,
		Action:         decision.Action,
		Blockers:       decision.Blockers,
	}
	if r.Now != nil {
		entry.EvaluatedAt = r.Now()
	}
	r.Store.Set(entry)

	logger.V(1).Info("v4 audit: parity entry written",
		"action", string(entry.Action),
		"owner", string(entry.Owner),
		"label_source", string(entry.LabelSource),
		"would_writes_in_pass", len(decision.Blockers),
	)

	// NOTE: decision.Notes is dropped at the Store boundary because
	// ParityEntry doesn't currently carry a Notes field (Patch 1 design).
	// A future patch can extend ParityEntry to include Notes if the
	// /audit endpoint needs them; the helpers already produce the data.

	return ctrl.Result{}, nil
}

func (r *V4AuditReconciler) modeStr() string {
	if r.OperatorMode == "" {
		return mode.Audit.String()
	}
	return r.OperatorMode
}

// observeCurrent reads the expected RS and RD from the cluster and
// translates them into a CurrentState struct. Pure read path.
//
// Tolerances:
//   - apierrors.IsNotFound: the resource doesn't exist → CurrentState
//     field stays at the false / empty zero value. Not an error.
//   - meta.IsNoMatchError: the VolSync CRD isn't installed in this
//     cluster → treat as not present, log at debug, no error. Lets the
//     audit reconciler run safely in clusters that haven't installed
//     VolSync yet (e.g., fresh bootstrap before Wave 1).
//   - Any other error: bubble up so controller-runtime retries with
//     backoff. Store entry stays at its prior value.
func (r *V4AuditReconciler) observeCurrent(ctx context.Context, namespace string, expected ExpectedState) (CurrentState, error) {
	logger := log.FromContext(ctx)
	var cur CurrentState

	rs := &unstructured.Unstructured{}
	rs.SetGroupVersionKind(rsGVK)
	rsKey := types.NamespacedName{Namespace: namespace, Name: expected.RSName}
	if err := r.Get(ctx, rsKey, rs); err == nil {
		cur.RSPresent = true
		cur.RSName = rs.GetName()
		cur.RSManagedBy = rs.GetLabels()[managedByLabel]
		cur.RSRepository, _, _ = unstructured.NestedString(rs.Object, "spec", "kopia", "repository")
		cur.RSSourcePVC, _, _ = unstructured.NestedString(rs.Object, "spec", "sourcePVC")
	} else if !apierrors.IsNotFound(err) {
		if meta.IsNoMatchError(err) {
			logger.V(1).Info("v4 audit: VolSync ReplicationSource CRD not installed; treating as not-present")
		} else {
			return cur, fmt.Errorf("get RS %s: %w", rsKey, err)
		}
	}

	rd := &unstructured.Unstructured{}
	rd.SetGroupVersionKind(rdGVK)
	rdKey := types.NamespacedName{Namespace: namespace, Name: expected.RDName}
	if err := r.Get(ctx, rdKey, rd); err == nil {
		cur.RDPresent = true
		cur.RDName = rd.GetName()
		cur.RDManagedBy = rd.GetLabels()[managedByLabel]
		cur.RDRepository, _, _ = unstructured.NestedString(rd.Object, "spec", "kopia", "repository")
	} else if !apierrors.IsNotFound(err) {
		if meta.IsNoMatchError(err) {
			logger.V(1).Info("v4 audit: VolSync ReplicationDestination CRD not installed; treating as not-present")
		} else {
			return cur, fmt.Errorf("get RD %s: %w", rdKey, err)
		}
	}

	return cur, nil
}
