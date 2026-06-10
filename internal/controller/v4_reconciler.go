package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/mitchross/pvc-plumber/internal/v4/executor"
	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/mode"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
	"github.com/mitchross/pvc-plumber/internal/v4/planner"
)

// volsyncBackupPVCLabel is the convention label the talos inline RS/RD
// pattern (and the builder) stamps with the source PVC name. Used as a
// fallback in the child→PVC reverse map when the operator's own
// source-namespace/source-pvc labels are absent (e.g. Argo-owned inline
// children).
const volsyncBackupPVCLabel = "volsync.backup/pvc"

// Phase 5 — Patch 3: V4AuditReconciler skeleton/orchestration.
// Phase 6 — Patch 6.7: bounded executor wired into the same Reconcile path.
//
// Orchestrates the Patch 1 (ParityEntry / Store) and Patch 2
// (ClassifyLabelSource / ComputeExpected / ClassifyOwner / DecideAction)
// pure helpers with the only cluster-touching code path Phase 5 owns:
// reading PVC + RS + RD via the embedded client.
//
// As of Patch 6.7 the reconciler is the v4 reconciler used by both audit
// AND permissive/enforce/strict modes — the Mode field on the struct
// switches between observe-only and bounded-write behavior. The
// V4AuditReconciler name is preserved for git history continuity; a
// cosmetic rename to V4Reconciler is a follow-up cleanup after the
// karakeep canary.
//
// IMPORTANT: cmd/operator/main.go still routes ONLY audit-mode traffic
// to this reconciler in Patch 6.7 (see reconcilerKindFor). The
// permissive route-flip is a separate sub-patch (6.7-wire). Patch 6.7
// proves the reconciler+executor pair work correctly under test;
// 6.7-wire makes the production binary actually use them.
//
// Contract (mode-gated):
//
//   - mode.Audit / mode.Unspecified: zero writes. The reconciler reads
//     the PVC, plans, then invokes executor.Execute which short-circuits
//     and records every planned op as Skipped. The auditclient wrapper
//     around the embedded client provides a second independent layer
//     of defense.
//
//   - mode.Permissive / mode.Enforce / mode.Strict: the executor applies
//     the planner's ops with its own GVK allow-list and ownership re-
//     check at the cluster boundary. Webhook deny / restore semantics
//     that differentiate enforce and strict from permissive are Phase 8
//     work, not Patch 6.7 — for executor mechanics these three modes
//     are identical.
//
//   - Reads three things per Reconcile: the PVC itself (corev1), and the
//     expected RS and RD (volsync.backube/v1alpha1, as unstructured).
//
//   - Writes one Store entry per Reconcile. The entry describes the
//     full per-PVC parity verdict the /audit endpoint (Patch 4) will
//     return. From 6.7 onward, the entry also carries an optional
//     ExecutionResult summarizing the executor's per-op verdicts
//     (populated only when the planner emitted at least one op; nil
//     for already-matches / skipped-* / write-gate-missing rows so the
//     report stays skimmable).
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
// As of Patch 6.7 the reconciler is registered for audit-mode traffic
// only (cmd/operator/main.go.reconcilerKindFor returns
// reconcilerKindV4Audit for audit and reconcilerKindV3 for everything
// else). Permissive/enforce/strict still hit the v3 chart-era
// PVCReconciler in production until 6.7-wire lands.

// V4AuditReconciler watches PVCs, writes parity entries to the Store, and
// (from Patch 6.7 onward) invokes the bounded executor to apply planner
// ops when Mode != audit. In audit mode every Reconcile is a pure
// observe-and-classify operation. The name keeps "Audit" for git history
// continuity; the body is the v4 reconciler used by all modes Patch 6.7
// reaches.
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

	// Mode gates executor.Execute. mode.Audit (or the zero value
	// mode.Unspecified, which executor.Execute treats identically)
	// short-circuits the executor: every planned op is recorded as
	// Skipped without any client.Create/Update/Delete call. In
	// mode.Permissive / Enforce / Strict the executor applies the
	// planner's ops with its own GVK allow-list and ownership re-checks
	// at the cluster boundary. Patch 6.7 wires only audit-mode traffic
	// into the production binary; the permissive route flip is a
	// separate sub-patch (6.7-wire). Tests exercise the executor's
	// permissive mechanics directly.
	Mode mode.Mode

	// Operator-wide defaults piped into planner.Inputs so the planner's
	// builder can render fully-formed RS/RD resources when proposing
	// create/update operations. Zero values are tolerated in audit mode
	// (the rendered ops are recorded as Skipped). For Mode=Permissive+
	// the operator binary should set these from its runtime config so
	// the executor writes resources with realistic snapshot class,
	// cache capacity, etc. Patch 6.7-wire (the main.go route flip)
	// wires the real defaults from cmd/operator/main.go.
	DefaultSnapshotClass string
	DefaultCacheCapacity string
	DefaultStorageClass  string
	DefaultUID           int64
	DefaultGID           int64
	DefaultFSGroup       int64

	// Now is injected for deterministic tests. nil → time.Now.
	Now func() time.Time

	// ResyncInterval, when > 0, makes every reconcile of a write-eligible
	// (enabled + manage-volsync) PVC requeue itself after the interval.
	// This is the belt-and-suspenders backstop to the RS/RD watch: even
	// if a child Delete event is ever missed (informer gap, leader-
	// election handoff, watch restart), a write-eligible PVC re-evaluates
	// within one interval and recreates any missing operator-owned child —
	// so the cluster can never silently sit with a pruned backup chain for
	// hours again (the 2026-05-28 nginx-example/storage incident). Zero
	// disables periodic requeue (the test default; production sets a few
	// minutes via cmd/operator/main.go).
	ResyncInterval time.Duration
}

// SetupWithManager registers the reconciler with the controller-runtime
// manager. The PVC remains the reconcile primary (For), but as of rc7 the
// controller ALSO watches the VolSync ReplicationSource / Replication
// Destination children and maps a child event back to its owning PVC.
//
// Why this matters (the 2026-05-28 nginx-example/storage incident): before
// rc7 the controller watched PVCs only. When Argo pruned the inline child
// RS/RD, the PVC object itself did not change, so no event fired, Reconcile
// never re-ran, and the operator left the PVC with no backup chain for ~15h
// while /audit served a stale "already-matches" snapshot. Watching the
// children — especially their Delete events — closes that gap: a prune now
// re-enqueues the owning PVC within seconds, the Store is refreshed (so
// /audit stops lying), and a write-eligible PVC's missing children are
// recreated.
//
// RS/RD are handled as unstructured (the operator has no go.mod dependency
// on the VolSync types — see pvc_controller.go). Watching an unstructured
// object requires only that its GVK be set; controller-runtime resolves the
// informer via the RESTMapper, NOT the scheme (Scheme.ObjectKinds reads the
// GVK straight off an unstructured object), which is why observeCurrent's
// cached unstructured Get has always worked without registering the VolSync
// types in the manager scheme. So no scheme change is needed here.
//
// We do NOT use Owns()/EnqueueRequestForOwner: pvc-plumber deliberately sets
// no ownerReference from a child back to its PVC (children must outlive
// transient PVC churn), and Argo-owned inline children carry no pvc-plumber
// ownerRef at all. Watches + a label/spec/name reverse-map (mapChildToPVC)
// is the only mechanism that covers BOTH owner classes — and the Argo-owned
// inline child is exactly the event class that caused the incident.
func (r *V4AuditReconciler) SetupWithManager(mgr ctrl.Manager) error {
	rs := &unstructured.Unstructured{}
	rs.SetGroupVersionKind(rsGVK)
	rd := &unstructured.Unstructured{}
	rd.SetGroupVersionKind(rdGVK)

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolumeClaim{}).
		Watches(rs, handler.EnqueueRequestsFromMapFunc(r.mapChildToPVC), builder.WithPredicates(childEventPredicate())).
		Watches(rd, handler.EnqueueRequestsFromMapFunc(r.mapChildToPVC), builder.WithPredicates(childEventPredicate())).
		Named("pvc-plumber-v4-audit").
		Complete(r)
}

// childEventPredicate filters the RS/RD watch event stream.
//
//   - Create / Delete: always pass. Delete is THE fix — the incident was a
//     prune (a Delete) that produced no reconcile. Create covers a child
//     reappearing with the wrong shape.
//   - Update: pass only when .metadata.generation changed. VolSync writes
//     RS/RD .status (lastSyncTime, latestMoverStatus) on every backup tick,
//     bumping resourceVersion but NOT generation; the reconciler reads only
//     spec/metadata, so status-only updates carry no parity value and would
//     otherwise amplify the reconcile rate needlessly.
//   - Generic: dropped.
//
// Unrelated-resource filtering is intentionally NOT done here (a label
// predicate is lossy on delete tombstones). mapChildToPVC is the
// authoritative filter: it only enqueues when it can resolve a source PVC
// that actually exists, so unrelated VolSync churn resolves to nothing.
func childEventPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return true
			}
			return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
		},
	}
}

// mapChildToPVC translates an RS/RD watch event into a reconcile request
// for the owning PVC. Returns nil (enqueue nothing) whenever the owning PVC
// cannot be resolved or does not exist — it never guesses.
//
// The existence check (a cache-backed Get; the manager already runs a PVC
// informer for For(&PVC{})) is what keeps unrelated cluster-wide VolSync
// churn from amplifying the reconcile rate: a child whose source PVC we
// can't find in the cluster is not ours to react to. It still fires for
// every real child-delete of a live PVC — the incident case.
func (r *V4AuditReconciler) mapChildToPVC(ctx context.Context, obj client.Object) []reconcile.Request {
	if obj == nil {
		return nil
	}
	// Mirror Reconcile's system-namespace short-circuit.
	if _, isSystem := r.SystemNamespaces[obj.GetNamespace()]; isSystem {
		return nil
	}
	ns, name := resolveSourcePVC(obj)
	if ns == "" || name == "" {
		return nil
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, pvc); err != nil {
		// NotFound (PVC gone or unrelated child) or any read error: do not
		// enqueue. A genuinely-deleted PVC is cleaned from the Store by its
		// own PVC Delete event, not here.
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}}
}

// resolveSourcePVC derives the owning PVC's (namespace, name) from an RS/RD
// object, in priority order. Pure (no client). Returns ("","") when no
// signal yields a PVC name — the caller must then enqueue nothing.
//
// Priority:
//  1. operator source labels pvc-plumber.io/source-namespace +
//     /source-pvc — the canonical pair the builder stamps on every
//     pvc-plumber-created child.
//  2. spec-derived — RS.spec.sourcePVC, RD.spec.kopia.sourceIdentity.
//     sourcePVCName. Covers Argo-owned inline children that lack the
//     operator source labels (the nginx-example canary case). The child
//     is always co-located with its PVC, so its own namespace is used.
//  3. the volsync.backup/pvc convention label (name only).
//  4. name-derivation via the bare-dst naming strategy (RS name == <pvc>;
//     RD name == <pvc>-dst). Last resort for delete tombstones carrying
//     neither labels nor spec.
func resolveSourcePVC(obj client.Object) (namespace, name string) {
	lbls := obj.GetLabels()
	childNS := obj.GetNamespace()

	// Priority 1: operator-stamped source labels.
	if ns := lbls[labels.LabelSourceNamespace]; ns != "" {
		if pvc := lbls[labels.LabelSourcePVC]; pvc != "" {
			return ns, pvc
		}
	}

	kind := obj.GetObjectKind().GroupVersionKind().Kind

	// Priority 2: spec-derived (handles label-less Argo inline children).
	if u, ok := obj.(*unstructured.Unstructured); ok {
		switch kind {
		case rsGVK.Kind:
			if pvc, _, _ := unstructured.NestedString(u.Object, "spec", "sourcePVC"); pvc != "" {
				return childNS, pvc
			}
		case rdGVK.Kind:
			if pvc, _, _ := unstructured.NestedString(u.Object, "spec", "kopia", "sourceIdentity", "sourcePVCName"); pvc != "" {
				return childNS, pvc
			}
		}
	}

	// Priority 3: volsync.backup/pvc convention label.
	if pvc := lbls[volsyncBackupPVCLabel]; pvc != "" {
		return childNS, pvc
	}

	// Priority 4: name-derivation (bare-dst).
	objName := obj.GetName()
	if objName == "" || childNS == "" {
		return "", ""
	}
	switch kind {
	case rsGVK.Kind:
		return childNS, objName
	case rdGVK.Kind:
		if strings.HasSuffix(objName, "-dst") {
			return childNS, strings.TrimSuffix(objName, "-dst")
		}
	}
	return "", ""
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

	// Step 8: classify owner. (ClassifyOwner is still the source of truth
	// for OwnerClassification — the planner takes the classification as
	// input, not the raw labels, so the reconciler retains ownership of
	// the live-resource lookup and the controller-package wins on
	// classification logic.)
	owner := ClassifyOwner(current, expected)

	// Step 8.5 (v4.0.1): namespace write gate. Read the PVC's Namespace
	// and check the pvc-plumber.io/managed-namespace opt-in label. This is
	// the software boundary that lets the cluster use a single DRY
	// cluster-wide RS/RD write ClusterRoleBinding instead of per-namespace
	// RoleBindings: even with cluster-wide RBAC, the planner refuses to
	// write in namespaces that are not opted in.
	//
	// Fail-closed error handling:
	//   - NotFound  → unmanaged (nsManaged stays false). The planner
	//     gates writes off; reporting still proceeds.
	//   - any other error → return it so the reconcile RETRIES rather than
	//     recording a possibly-wrong verdict (or, worse, treating a
	//     transient apiserver blip on a managed namespace as a reason to
	//     stop managing it). We have not executed anything yet, so a retry
	//     is side-effect-free.
	nsManaged := false
	nsObj := &corev1.Namespace{}
	if err := r.Get(ctx, types.NamespacedName{Name: req.Namespace}, nsObj); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get namespace %s: %w", req.Namespace, err)
		}
		// NotFound: leave nsManaged=false (fail-closed) and continue.
	} else {
		nsManaged = labels.NamespaceManaged(nsObj.GetLabels())
	}

	// Step 9: build planner.Inputs and call PlanFor. The planner replaces
	// the old DecideAction call site as of Patch 6.5; it implements the
	// full v4 decision precedence (backup-exempt → parse errors → no
	// opt-in → manage-without-enabled → tier=disabled → ownership safety
	// → write gate → drift/create). DecideAction remains in the package
	// for v4_compare_test.go but is no longer wired into the reconcile
	// path.
	//
	// Type casting note: planner re-declares ActionKind /
	// OwnerClassification / LabelSource / CurrentState (mirroring this
	// package's enums) to keep the planner→controller import direction
	// one-way. The casts here are byte-identical — the underlying string
	// values match exactly.
	plan := planner.PlanFor(planner.Inputs{
		Namespace:            req.Namespace,
		PVCName:              req.Name,
		PVCCapacity:          pvcCapacity(pvc),
		PVCAccessModes:       pvcAccessModes(pvc),
		PVCStorageClass:      derefStringPtr(pvc.Spec.StorageClassName),
		Spec:                 spec,
		LabelSource:          planner.LabelSource(string(source)),
		Current:              toPlannerCurrent(current),
		Owner:                planner.OwnerClassification(string(owner)),
		NamespaceManaged:     nsManaged,
		NamingStrategy:       r.NamingStrategy,
		DefaultRepoSecret:    r.DefaultRepoSecret,
		DefaultSnapshotClass: r.DefaultSnapshotClass,
		DefaultCacheCapacity: r.DefaultCacheCapacity,
		DefaultStorageClass:  r.DefaultStorageClass,
		DefaultUID:           r.DefaultUID,
		DefaultGID:           r.DefaultGID,
		DefaultFSGroup:       r.DefaultFSGroup,
	})

	// Step 10: bounded executor. In audit / unspecified mode this
	// short-circuits inside executor.Execute and records every op as
	// Skipped without touching the cluster. In permissive+ modes the
	// executor enforces its own GVK allow-list (RS/RD only) and re-
	// checks ownership on Update/Delete at the cluster boundary —
	// independent defense-in-depth on top of the planner's rule 7
	// ownership gates and rule 6 write-fuse check. Per-op errors are
	// captured inside the Result; Execute never returns a Go error,
	// so we don't need to unwind the reconcile on apiserver failures.
	// The reconciler decides what to log + whether to surface the
	// failures in the Store entry.
	execResult := executor.Execute(ctx, r.Client, r.Mode, plan)

	// Step 11: assemble + Store. PlannedOps is reduced to a compact
	// summary (Kind / GVK / Namespace / Name) so the /audit response
	// stays human-skimmable while still proving the planner only ever
	// targets ReplicationSource / ReplicationDestination.
	//
	// ExecutionResult is populated only when the planner emitted ops.
	// already-matches / skipped-* / write-gate-missing rows leave the
	// field nil so /audit stays terse for the bulk of the cluster's
	// PVCs.
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
		Action:         ActionKind(string(plan.Action)),
		Blockers:       plan.Blockers,
		Notes:          plan.Notes,
		PlannedOps:     toPlannedOpSummaries(plan.Ops),
	}
	if len(plan.Ops) > 0 {
		summary := toExecutionResultSummary(execResult)
		entry.ExecutionResult = &summary
		// Log apiserver error details for failed ops. /audit
		// deliberately omits raw Err strings (stable JSON shape), so
		// without this loop a timeout / RBAC denial / conflict would
		// only appear as Status=failed with no diagnostic context.
		// Refusals are skipped — they're well-defined safety outcomes
		// ("forbidden-kind", "not-owned", "exists") visible in
		// ExecutionResult.Outcomes already.
		for _, op := range execResult.Attempted {
			if op.Status == executor.OpFailed && op.Err != nil {
				logger.Error(op.Err, "v4 executor: op failed",
					"kind", op.Kind,
					"gvk", op.GVK,
					"namespace", op.Namespace,
					"name", op.Name,
					"reason", op.Reason,
				)
			}
		}
	}
	if r.Now != nil {
		entry.EvaluatedAt = r.Now()
	}
	r.Store.Set(entry)

	logger.V(1).Info("v4 audit: parity entry written",
		"action", string(entry.Action),
		"owner", string(entry.Owner),
		"label_source", string(entry.LabelSource),
		"blockers", len(plan.Blockers),
		"planned_ops", len(plan.Ops),
		"exec_skipped", execResult.Counts.Skipped,
		"exec_succeeded", execResult.Counts.Succeeded,
		"exec_refused", execResult.Counts.Refused,
		"exec_failed", execResult.Counts.Failed,
	)

	return r.resultFor(spec), nil
}

// resultFor returns the reconcile Result. Write-eligible PVCs are requeued
// after ResyncInterval (when set) so a missed RS/RD watch event self-heals
// within a bounded window; everything else returns the zero Result (event-
// driven only). See the ResyncInterval field doc.
func (r *V4AuditReconciler) resultFor(spec labels.Spec) ctrl.Result {
	if r.ResyncInterval > 0 && spec.Enabled && spec.ManageVolSync {
		return ctrl.Result{RequeueAfter: r.ResyncInterval}
	}
	return ctrl.Result{}
}

// toExecutionResultSummary translates the executor's Result into the
// /audit-friendly summary shape. Drops the raw Go error attached to
// failed ops; the reconciler logs error details via
// logExecutionFailures so /audit consumers see a stable JSON shape
// without opaque error strings.
func toExecutionResultSummary(r executor.Result) ExecutionResultSummary {
	out := ExecutionResultSummary{Counts: r.Counts}
	if len(r.Attempted) == 0 {
		return out
	}
	out.Outcomes = make([]ExecutionOpOutcome, 0, len(r.Attempted))
	for _, op := range r.Attempted {
		out.Outcomes = append(out.Outcomes, ExecutionOpOutcome{
			Kind:      op.Kind,
			GVK:       op.GVK,
			Namespace: op.Namespace,
			Name:      op.Name,
			Status:    string(op.Status),
			Reason:    op.Reason,
		})
	}
	return out
}

// toPlannerCurrent translates the controller-package CurrentState into
// the planner's mirrored type. RSSchedule is captured since v4.0.2 so
// the planner's schedule-drift check (shapeMatches) has real input —
// before that, tier changes on live operator-owned RS were silently
// ignored (2026-06-09 review finding).
func toPlannerCurrent(c CurrentState) planner.CurrentState {
	return planner.CurrentState{
		RSPresent:    c.RSPresent,
		RSName:       c.RSName,
		RSManagedBy:  c.RSManagedBy,
		RSRepository: c.RSRepository,
		RSSourcePVC:  c.RSSourcePVC,
		RSSchedule:   c.RSSchedule,
		RDPresent:    c.RDPresent,
		RDName:       c.RDName,
		RDManagedBy:  c.RDManagedBy,
		RDRepository: c.RDRepository,
	}
}

// toPlannedOpSummaries lifts the planner's full unstructured Ops into the
// /audit-friendly summary shape. Each op's GVK is rendered as the
// canonical "group/version/Kind" string so the cutover runbook can grep
// for "volsync.backube/v1alpha1/ReplicationSource" without parsing JSON
// structure.
func toPlannedOpSummaries(ops []planner.PlannedOp) []PlannedOpSummary {
	if len(ops) == 0 {
		return nil
	}
	out := make([]PlannedOpSummary, 0, len(ops))
	for _, op := range ops {
		gvk := op.Resource.GroupVersionKind()
		out = append(out, PlannedOpSummary{
			Kind:      string(op.Kind),
			GVK:       gvk.GroupVersion().String() + "/" + gvk.Kind,
			Namespace: op.Resource.GetNamespace(),
			Name:      op.Resource.GetName(),
		})
	}
	return out
}

// pvcCapacity returns the PVC's requested storage capacity as a string
// suitable for builder.Inputs.PVCCapacity, or "" if not set. The builder
// uses this when rendering the VolumeSnapshotClass'd cache PVC inside
// the rendered RS resource.
func pvcCapacity(pvc *corev1.PersistentVolumeClaim) string {
	if pvc == nil {
		return ""
	}
	if q, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		return q.String()
	}
	return ""
}

// pvcAccessModes returns the PVC's access modes as a flat []string. nil
// when the PVC has none declared (rare; controller-runtime defaults to
// ReadWriteOnce at apply time, but we pass through what the user set).
func pvcAccessModes(pvc *corev1.PersistentVolumeClaim) []string {
	if pvc == nil || len(pvc.Spec.AccessModes) == 0 {
		return nil
	}
	out := make([]string, 0, len(pvc.Spec.AccessModes))
	for _, m := range pvc.Spec.AccessModes {
		out = append(out, string(m))
	}
	return out
}

// derefStringPtr returns the underlying string or "" for a nil pointer.
func derefStringPtr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
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
		cur.RSSchedule, _, _ = unstructured.NestedString(rs.Object, "spec", "trigger", "schedule")
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
