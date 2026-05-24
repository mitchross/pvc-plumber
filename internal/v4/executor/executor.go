// Package executor turns a planner.Plan into bounded cluster writes.
//
// Phase 6 / Patch 6.6 of docs/pvc-plumber-v4-prd.md.
//
// Mode discipline:
//
//   - mode=Audit (or Unspecified, defensively): every op is recorded
//     as Skipped. No client calls are made. The executor short-circuits
//     at the top of Execute — this is the FIRST line of defense against
//     accidental writes. The auditclient wrapper that the operator's
//     production binary installs around the controller-runtime client
//     is the SECOND.
//
//   - mode=Permissive / Enforce / Strict: each op is attempted with
//     ownership and GVK safety re-checks at the executor layer. The
//     three modes share identical executor mechanics in Patch 6.6 — the
//     webhook/deny semantics that differentiate enforce/strict from
//     permissive are Phase 8 (admission), not Phase 6 (reconcile-write).
//
// Per-op safety rails (ENFORCED INDEPENDENTLY of the planner):
//
//  1. GVK allow-list. The op's resource MUST be a VolSync RS or RD.
//     Anything else — Secret, ExternalSecret, PVC, webhook config, SA,
//     Pod — is Refused with reason "forbidden-kind". Defense against a
//     future planner bug or compromised input.
//
//  2. Ownership re-check on Update/Delete. The executor reads the live
//     resource and verifies app.kubernetes.io/managed-by=pvc-plumber.
//     If the label is missing, or carries any other value (most
//     critically "argocd"), the op is Refused with reason "not-owned".
//     This is a race-safety net: if the planner's ClassifyOwner output
//     was based on a stale cache or the resource was relabeled between
//     plan and execute, the executor catches the inconsistency.
//
//  3. No adoption on Create. If Create returns AlreadyExists, the
//     executor refuses with reason "exists". It does NOT relabel or
//     patch the existing resource — adoption is explicitly out of
//     scope for Phase 6 per the PRD's "no-adoption" posture. The
//     reconciler's next pass will re-plan against the new live state.
//
// Return shape: a single Result with per-op outcomes plus aggregate
// counts. Execute never returns a Go error — every per-op failure is
// captured in an OpOutcome with Status=OpFailed and the apiserver error
// in Err. The reconciler (Patch 6.7) inspects Counts.Failed to decide
// whether to requeue.
//
// Read-then-overwrite Update strategy: the executor reads live, copies
// live's resourceVersion+UID onto the planner's desired object, and
// calls Update with the desired body. Spec, labels, and annotations are
// fully replaced with what the planner produced. SSA / fieldManager
// ownership is deliberately not used in Patch 6.6 — simpler, easier to
// reason about, and matches the existing v3 reconciler's mutation style.
package executor

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mitchross/pvc-plumber/internal/v4/mode"
	"github.com/mitchross/pvc-plumber/internal/v4/planner"
)

// =============================================================================
// Result types
// =============================================================================

// OpStatus enumerates the four possible terminal outcomes for a single
// planned op after Execute considers it.
type OpStatus string

const (
	// OpSkipped: the op was never attempted because the executor's mode
	// is audit/unspecified. No client call was made for this op.
	OpSkipped OpStatus = "skipped"

	// OpSucceeded: the apiserver accepted the operation. Includes the
	// idempotent "delete on a NotFound resource" case (treated as
	// success-because-already-absent).
	OpSucceeded OpStatus = "succeeded"

	// OpRefused: the executor's safety rails rejected the op before
	// (or instead of) calling the apiserver. Reasons:
	//   - "forbidden-kind"  — GVK is not RS/RD
	//   - "exists"          — Create returned AlreadyExists; no adoption
	//   - "not-owned"       — Update/Delete live resource has wrong managed-by
	//   - "absent"          — Update target doesn't exist
	//   - "nil-resource"    — planner emitted a PlannedOp with nil Resource
	//   - "unknown-op-kind" — planner emitted an op with an unrecognized Kind
	OpRefused OpStatus = "refused"

	// OpFailed: the apiserver returned a non-recoverable error
	// (timeout, conflict, RBAC denial, …). The original error is in
	// OpOutcome.Err so the reconciler can log it loudly.
	OpFailed OpStatus = "failed"
)

// OpOutcome is the per-op record produced by Execute. Stable JSON-able
// shape so future patches can surface this in /audit alongside the plan.
type OpOutcome struct {
	// Kind echoes planner.OpKind ("create" | "update" | "delete").
	Kind string

	// GVK is the canonical "group/version/Kind" string of the target
	// resource. For ops produced by the planner this will always be
	// volsync.backube/v1alpha1/{ReplicationSource,ReplicationDestination};
	// foreign-kind ops are recorded with the original GVK so post-hoc
	// debugging can show what was refused.
	GVK string

	// Namespace + Name identify the target resource.
	Namespace string
	Name      string

	// Status is the terminal outcome (see OpStatus constants).
	Status OpStatus

	// Reason is a short human-readable explanation for Refused / Failed
	// outcomes. Empty for OpSucceeded and OpSkipped (status is enough).
	Reason string

	// Err carries the apiserver error for OpFailed outcomes. Nil
	// otherwise.
	Err error
}

// Counts aggregates per-status totals across all ops Execute considered.
// Exposed as a flat struct (not a map) so the reconciler can branch on
// e.g. Counts.Failed > 0 without map nil-checks.
type Counts struct {
	Skipped   int
	Succeeded int
	Refused   int
	Failed    int
}

// Result is the full execution outcome of one Execute call.
//
// Attempted preserves the order ops appeared in plan.Ops, so callers
// can correlate by index. Counts is the sum across Attempted.
type Result struct {
	Attempted []OpOutcome
	Counts    Counts
}

// =============================================================================
// Execute — the entry point
// =============================================================================

// Execute applies plan.Ops against the cluster, gated by mode.
//
// Empty plan (Ops nil or empty) returns a zero Result with no client
// calls. Otherwise, see the package doc for the full mode + safety
// contract.
//
// Execute never returns a Go error. Per-op failures are captured in
// the Result's OpOutcome slice; the reconciler decides what to do
// about them.
func Execute(ctx context.Context, c client.Client, m mode.Mode, plan planner.Plan) Result {
	res := Result{}
	if len(plan.Ops) == 0 {
		return res
	}

	// Audit-first: in audit mode the executor performs no client work
	// at all. Every op is recorded as Skipped. This is intentional
	// per the Patch 6.6 contract — "executor does nothing in audit
	// mode," cleaner than relying on the auditclient wrapper alone.
	// Defense in depth: even if a future caller bypasses this branch,
	// the auditclient wrapper still blocks Create/Update/Patch/Delete
	// at the client layer.
	if m == mode.Audit || m == mode.Unspecified {
		for _, op := range plan.Ops {
			res.Attempted = append(res.Attempted, makeOutcome(op, OpSkipped, "mode=audit", nil))
			res.Counts.Skipped++
		}
		return res
	}

	// Permissive / Enforce / Strict: identical executor mechanics in
	// Patch 6.6. The webhook deny / restore-time differences between
	// these modes belong to later phases.
	for _, op := range plan.Ops {
		out := executeOne(ctx, c, op)
		res.Attempted = append(res.Attempted, out)
		switch out.Status {
		case OpSucceeded:
			res.Counts.Succeeded++
		case OpRefused:
			res.Counts.Refused++
		case OpFailed:
			res.Counts.Failed++
		case OpSkipped:
			// Defensive — should never happen outside the audit branch.
			res.Counts.Skipped++
		}
	}
	return res
}

// =============================================================================
// Per-op execution (dispatch + safety)
// =============================================================================

// executeOne dispatches a single op through GVK safety and per-kind
// handlers. Pre-conditions: caller must NOT call this with mode=Audit
// (the audit branch in Execute handles that case explicitly).
func executeOne(ctx context.Context, c client.Client, op planner.PlannedOp) OpOutcome {
	if op.Resource == nil {
		return makeOutcome(op, OpRefused, "nil-resource", nil)
	}

	// GVK allow-list is the executor's most important defense. Even
	// if the planner ever emits a Secret/PVC/webhook op (bug or
	// compromise), the executor refuses before reaching the apiserver.
	gvk := op.Resource.GroupVersionKind()
	if !IsAllowedGVK(gvk) {
		return makeOutcome(op, OpRefused, "forbidden-kind", nil)
	}

	switch op.Kind {
	case planner.OpCreate:
		return execCreate(ctx, c, op)
	case planner.OpUpdate:
		return execUpdate(ctx, c, op)
	case planner.OpDelete:
		return execDelete(ctx, c, op)
	default:
		return makeOutcome(op, OpRefused, fmt.Sprintf("unknown-op-kind:%s", op.Kind), nil)
	}
}

// execCreate attempts to create a fresh RS/RD. The planner's contract
// (rule 6c) only emits a Create when the live state for that resource
// is absent, but races can happen between plan and execute — Argo could
// have synced an inline RS/RD in the interim, or another reconciler
// instance could have created the operator-owned copy. In both cases
// AlreadyExists is the correct refusal: we don't adopt, we don't patch,
// and the next reconcile pass will see the live state and re-plan.
func execCreate(ctx context.Context, c client.Client, op planner.PlannedOp) OpOutcome {
	desired := op.Resource.DeepCopy()
	err := c.Create(ctx, desired)
	if err == nil {
		return makeOutcome(op, OpSucceeded, "", nil)
	}
	if apierrors.IsAlreadyExists(err) {
		return makeOutcome(op, OpRefused, "exists", nil)
	}
	return makeOutcome(op, OpFailed, "create-failed", err)
}

// execUpdate read-then-overwrites the live resource with the planner's
// desired body. Strict ownership gating: only pvc-plumber-owned live
// objects can be updated.
func execUpdate(ctx context.Context, c client.Client, op planner.PlannedOp) OpOutcome {
	desired := op.Resource.DeepCopy()
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(desired.GroupVersionKind())

	key := types.NamespacedName{Namespace: desired.GetNamespace(), Name: desired.GetName()}
	if err := c.Get(ctx, key, live); err != nil {
		if apierrors.IsNotFound(err) {
			return makeOutcome(op, OpRefused, "absent", nil)
		}
		return makeOutcome(op, OpFailed, "get-failed", err)
	}

	if !IsOperatorOwned(live) {
		return makeOutcome(op, OpRefused, "not-owned", nil)
	}

	// Read-then-overwrite: preserve the live resourceVersion + UID
	// (required for a successful Update) and let the planner's desired
	// body replace spec/labels/annotations wholesale. Server-side apply
	// is intentionally not used in Patch 6.6 (see package doc).
	desired.SetResourceVersion(live.GetResourceVersion())
	desired.SetUID(live.GetUID())

	if err := c.Update(ctx, desired); err != nil {
		return makeOutcome(op, OpFailed, "update-failed", err)
	}
	return makeOutcome(op, OpSucceeded, "", nil)
}

// execDelete removes operator-owned RS/RD when the planner emits a
// tier=disabled (or similar) tear-down. NotFound is idempotent success.
func execDelete(ctx context.Context, c client.Client, op planner.PlannedOp) OpOutcome {
	desired := op.Resource
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(desired.GroupVersionKind())

	key := types.NamespacedName{Namespace: desired.GetNamespace(), Name: desired.GetName()}
	if err := c.Get(ctx, key, live); err != nil {
		if apierrors.IsNotFound(err) {
			// Already gone — idempotent success.
			return makeOutcome(op, OpSucceeded, "already-gone", nil)
		}
		return makeOutcome(op, OpFailed, "get-failed", err)
	}

	if !IsOperatorOwned(live) {
		return makeOutcome(op, OpRefused, "not-owned", nil)
	}

	if err := c.Delete(ctx, live); err != nil {
		if apierrors.IsNotFound(err) {
			// Raced with another deletion — still success.
			return makeOutcome(op, OpSucceeded, "already-gone", nil)
		}
		return makeOutcome(op, OpFailed, "delete-failed", err)
	}
	return makeOutcome(op, OpSucceeded, "", nil)
}

// makeOutcome packages a single op + status into an OpOutcome. Safe to
// call with op.Resource == nil; GVK / Namespace / Name fields are
// populated only when a Resource is present.
func makeOutcome(op planner.PlannedOp, status OpStatus, reason string, err error) OpOutcome {
	var gvk schema.GroupVersionKind
	var ns, name string
	if op.Resource != nil {
		gvk = op.Resource.GroupVersionKind()
		ns = op.Resource.GetNamespace()
		name = op.Resource.GetName()
	}
	return OpOutcome{
		Kind:      string(op.Kind),
		GVK:       gvkString(gvk),
		Namespace: ns,
		Name:      name,
		Status:    status,
		Reason:    reason,
		Err:       err,
	}
}

// gvkString renders a GroupVersionKind as the canonical
// "group/version/Kind" string used everywhere in /audit output. The
// core-group ("") renders as "v1/Kind" because GroupVersion's String
// method drops the empty group; that's the same format the planner
// uses in PlannedOpSummary.GVK.
func gvkString(gvk schema.GroupVersionKind) string {
	return gvk.GroupVersion().String() + "/" + gvk.Kind
}
