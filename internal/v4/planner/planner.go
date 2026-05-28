// Package planner is the pure decision engine that turns a parsed
// PVC + observed cluster state into a v4 audit verdict plus the
// concrete operations the executor would apply in permissive+ modes.
//
// Phase 6 / Patch 6.4 of docs/pvc-plumber-v4-prd.md.
//
// Purity contract: no Kubernetes client, no I/O, no time / random
// (transitive — the builder package the planner calls is also pure).
// The same Inputs always produce the same Plan. The audit-mode
// reconciler can call this safely from a hot path; the permissive-mode
// executor can rely on the Ops slice being a stable preview of what
// it will apply.
//
// Type duplication note: ActionKind / OwnerClassification / LabelSource
// / CurrentState are intentionally re-declared here rather than imported
// from internal/controller. The controller package will import this
// package in Patch 6.5 (V4AuditReconciler consumes planner output), so
// planner→controller would form a cycle. The string values mirror the
// controller's exactly; reconciler-side conversion in Patch 6.5 is a
// trivial cast.
//
// Decision precedence (top → bottom; first rule that fires wins):
//
//  1. backup-exempt valid                          → SkippedExempt
//  2. backup-exempt with missing reason            → NeedsHumanReview
//  3. Spec.Errors non-empty                        → NeedsHumanReview
//  4. no opt-in (no enabled, no manage, no legacy) → SkippedNotOptedIn
//  5. manage-volsync=true but enabled=false        → SkippedNotOptedIn + blocker
//  6. write-eligible (Enabled + ManageVolSync):
//     a. tier=disabled + operator-owned current   → WouldDelete + delete ops
//     b. tier=disabled + non-operator current     → AlreadyMatches + note
//     c. tier!=disabled + no current              → WouldCreate + create ops
//     d. tier!=disabled + operator-owned matches  → AlreadyMatches
//     e. tier!=disabled + operator-owned drifts   → WouldUpdate + update ops
//     f. inline-argo/unmanaged matches            → AlreadyMatches
//     g. inline-argo drifts                       → InlineArgoObserved
//     h. unmanaged drifts                         → NeedsHumanReview
//  7. not write-eligible (legacy-only OR enabled-only):
//     a. no current                                → WriteGateMissing + blocker
//     b. current matches expected shape           → AlreadyMatches + note (gate respected)
//     c. inline-argo drifts                        → InlineArgoObserved
//     d. unmanaged drifts                          → NeedsHumanReview
//     e. operator-owned matches                    → AlreadyMatches
//     f. operator-owned drifts                     → AlreadyMatches + note (can't update without gate)
//
// The planner only ever produces operations on
// ReplicationSource / ReplicationDestination kinds. A paranoia test in
// planner_test.go walks every Op of every table case and proves no
// Secret, ExternalSecret, ClusterExternalSecret, PVC, or webhook GVK
// ever appears in Ops.
package planner

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/mitchross/pvc-plumber/internal/v4/builder"
	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// =============================================================================
// Public type aliases (mirror internal/controller/v4_report.go)
// =============================================================================

// ActionKind mirrors controller.ActionKind. Same wire strings so a
// reconciler-side cast (Patch 6.5) is byte-identical.
type ActionKind string

const (
	ActionAlreadyMatches     ActionKind = "already-matches"
	ActionWouldCreate        ActionKind = "would-create"
	ActionWouldUpdate        ActionKind = "would-update"
	ActionWouldDelete        ActionKind = "would-delete"
	ActionInlineArgoObserved ActionKind = "inline-argo-observed"
	ActionWriteGateMissing   ActionKind = "write-gate-missing"
	ActionSkippedExempt      ActionKind = "skipped-exempt"
	ActionSkippedNotOptedIn  ActionKind = "skipped-not-opted-in"
	ActionNeedsHumanReview   ActionKind = "needs-human-review"
)

// OwnerClassification mirrors controller.OwnerClassification.
type OwnerClassification string

const (
	OwnerNone                      OwnerClassification = "none"
	OwnerPVCPlumber                OwnerClassification = "managed-by-pvc-plumber"
	OwnerInlineArgo                OwnerClassification = "inline-argo"
	OwnerUnmanagedOrGitopsObserved OwnerClassification = "unmanaged-or-gitops-observed"
	OwnerUnknown                   OwnerClassification = "unknown"
)

// LabelSource mirrors controller.LabelSource.
type LabelSource string

const (
	LabelSourceNone   LabelSource = "none"
	LabelSourceV4     LabelSource = "v4"
	LabelSourceLegacy LabelSource = "legacy"
	LabelSourceBoth   LabelSource = "both"
)

// CurrentState is a snapshot of the observed RS/RD pair the reconciler
// found when last walking the cluster. All fields zero-valued means
// no current resources exist.
type CurrentState struct {
	RSPresent    bool
	RSName       string
	RSManagedBy  string
	RSRepository string
	RSSourcePVC  string
	RSSchedule   string // optional; only populated if reconciler captured it

	RDPresent    bool
	RDName       string
	RDManagedBy  string
	RDRepository string
}

// =============================================================================
// Plan types
// =============================================================================

// OpKind enumerates the apply verbs the executor (Patch 6.6) supports.
type OpKind string

const (
	OpCreate OpKind = "create"
	OpUpdate OpKind = "update"
	OpDelete OpKind = "delete"
)

// PlannedOp is one operation the executor would perform in
// permissive+ modes. Resource is the exact unstructured object
// produced by the builder; executor must treat as immutable
// (deep-copy if it needs to mutate before Apply).
type PlannedOp struct {
	Kind     OpKind
	Resource *unstructured.Unstructured
}

// Plan is the planner's verdict for a single PVC.
type Plan struct {
	// Action is the parity verdict that lands in the /audit report.
	Action ActionKind
	// Ops are the operations the executor would apply in permissive+
	// modes. Empty for read-only verdicts (already-matches, observed-*,
	// skipped-*, write-gate-missing, needs-human-review).
	Ops []PlannedOp
	// Blockers are human-readable explanations of why a write was
	// suppressed (gate missing, ownership conflict, parse error, …).
	Blockers []string
	// Notes are informational context the /audit endpoint surfaces
	// alongside Blockers (e.g. "tier=disabled suppresses VolSync
	// resources").
	Notes []string
}

// =============================================================================
// Inputs
// =============================================================================

// Inputs is the total context PlanFor needs. Built by the reconciler
// from the PVC, the parsed labels.Spec, and the observed CurrentState.
type Inputs struct {
	// Identity.
	Namespace string
	PVCName   string

	// PVC spec snapshot, passed through to builder when an Op is
	// produced.
	PVCCapacity     string
	PVCAccessModes  []string
	PVCStorageClass string

	// Parsed labels and current cluster state.
	Spec        labels.Spec
	LabelSource LabelSource
	Current     CurrentState
	Owner       OwnerClassification

	// Naming + shared-resource references.
	NamingStrategy    naming.Strategy
	DefaultRepoSecret string

	// Cluster-wide defaults the operator config carries.
	DefaultSnapshotClass string
	DefaultCacheCapacity string
	DefaultStorageClass  string
	DefaultUID           int64
	DefaultGID           int64
	DefaultFSGroup       int64
}

// =============================================================================
// PlanFor — the entry point
// =============================================================================

// PlanFor evaluates the decision precedence and returns the verdict.
// Pure: same Inputs → same Plan, always. No I/O.
func PlanFor(in Inputs) Plan {
	// Rule 1: backup-exempt valid wins over EVERYTHING. Even if the
	// PVC carries enabled+manage labels and there are operator-owned
	// stale resources, exempt suppresses any write. Cleaning those
	// stale resources is a manual operator decision, not a planner
	// auto-action.
	if in.Spec.ExemptKind == labels.ExemptValid {
		notes := []string{"backup-exempt=true with valid reason annotation; nothing to plan"}
		if in.Owner == OwnerPVCPlumber && (in.Current.RSPresent || in.Current.RDPresent) {
			notes = append(notes, "WARNING: operator-owned RS/RD exist for this exempt PVC; manual cleanup may be required")
		}
		return Plan{Action: ActionSkippedExempt, Notes: notes}
	}

	// Rule 2: backup-exempt with no FQ reason is a contract violation
	// (the bare reason key was historically a silent-fail DR landmine).
	// Report it but do nothing.
	if in.Spec.ExemptKind == labels.ExemptMissingReason {
		return Plan{
			Action: ActionNeedsHumanReview,
			Blockers: []string{
				"backup-exempt=true is set but storage.vanillax.dev/backup-exempt-reason annotation is missing or empty",
			},
		}
	}

	// Rule 3: any parser error blocks the planner. The reconciler
	// surfaces these as blockers so the operator can fix the label
	// before the next reconcile.
	if len(in.Spec.Errors) > 0 {
		blockers := make([]string, 0, len(in.Spec.Errors))
		for _, e := range in.Spec.Errors {
			blockers = append(blockers, e.Error())
		}
		return Plan{Action: ActionNeedsHumanReview, Blockers: blockers}
	}

	// Rule 4: no opt-in at all. Reported for visibility (operator can
	// confirm a PVC was intentionally left out) but no action.
	if !in.Spec.Enabled && !in.Spec.ManageVolSync && in.Spec.Origin == labels.OriginNone {
		return Plan{Action: ActionSkippedNotOptedIn}
	}

	// Rule 5: manage-volsync=true without enabled=true is a partial /
	// invalid intent — the PVC has not actually entered the v4 contract
	// (no LabelEnabled), so the operator must NOT write. Make the
	// half-state visible via a blocker; this catches typos and stale
	// merges where the enabled label was supposed to land alongside
	// manage-volsync but didn't.
	if in.Spec.ManageVolSync && !in.Spec.Enabled && in.Spec.Origin == labels.OriginNone {
		return Plan{
			Action:   ActionSkippedNotOptedIn,
			Blockers: []string{"pvc-plumber.io/manage-volsync=true is set, but pvc-plumber.io/enabled=true is missing; PVC is not write-eligible"},
		}
	}

	// Past this point: the PVC is opted in to at least the reporting
	// surface (legacy backup label OR enabled=true). Build the
	// expected state and branch on write eligibility.
	writeEligible := in.Spec.Enabled && in.Spec.ManageVolSync

	if writeEligible {
		return planWriteEligible(in)
	}
	return planNotWriteEligible(in)
}

// planWriteEligible covers rules 6a–6h. The PVC has both
// `pvc-plumber.io/enabled: "true"` and `pvc-plumber.io/manage-volsync:
// "true"`. The operator may write IF the ownership of any current
// resources allows it.
func planWriteEligible(in Inputs) Plan {
	// Rule 6a/6b: tier=disabled. The operator is explicitly told NOT
	// to back up this PVC, but the v4 contract still applies. Only
	// pvc-plumber-owned stale resources are eligible for deletion;
	// inline-argo/unmanaged resources are observed and left alone.
	if in.Spec.Tier == labels.TierDisabled {
		if in.Owner == OwnerPVCPlumber && (in.Current.RSPresent || in.Current.RDPresent) {
			return Plan{
				Action: ActionWouldDelete,
				Ops:    deleteOps(in),
				Notes:  []string{"tier=disabled suppresses VolSync resources; pvc-plumber-owned RS/RD are stale and will be deleted"},
			}
		}
		// No operator-owned current to delete. Anything else (inline,
		// unmanaged, none) is observe-only.
		return Plan{
			Action: ActionAlreadyMatches,
			Notes:  []string{"tier=disabled; no operator-owned VolSync resources to manage"},
		}
	}

	// Rule 6c-6h: real tier (hourly/daily/weekly/manual/unspecified).
	// Decide based on the ownership of the current resources.
	switch in.Owner {
	case OwnerNone:
		// No current RS/RD at all. Plan the full creation.
		return Plan{
			Action: ActionWouldCreate,
			Ops:    createOps(in),
		}

	case OwnerPVCPlumber:
		// Operator owns the current resources. We MAY update or
		// delete here.
		//
		// Partial state check FIRST: if one of RS/RD is missing,
		// plan a create for just the missing child rather than an
		// update (shapeMatches would return false simply because of
		// the missing presence flag, which would otherwise misroute
		// to WouldUpdate).
		if !in.Current.RSPresent || !in.Current.RDPresent {
			return Plan{
				Action: ActionWouldCreate,
				Ops:    createOpsForMissing(in),
				Notes:  []string{"partial operator-owned state; creating missing child"},
			}
		}
		// Both children present. Check for spec drift.
		if !shapeMatches(in) {
			return Plan{
				Action: ActionWouldUpdate,
				Ops:    updateOps(in),
			}
		}
		return Plan{Action: ActionAlreadyMatches}

	case OwnerInlineArgo:
		// Argo / GitOps owns these. NEVER patch or delete.
		//
		// rc7 partial-state guard (confirmed policy): if exactly one child
		// is present and it is inline-argo-owned while its sibling is
		// missing, this is an anomalous half-state — e.g. an Argo prune
		// removed one inline child but not the other, or a hand-edit. The
		// operator MUST NOT create the missing operator-equivalent sibling:
		// that would put two owners on a single name (pvc-plumber vs argocd)
		// and start a sync tug-of-war. There is exactly one valid name per
		// child, so the two owners cannot coexist. Surface it loudly as
		// needs-human-review with a distinct blocker so it is not silently
		// conflated with steady-state inline-argo drift. Zero ops. The
		// resolution is a human decision: either restore the missing inline
		// child via Git/Argo, or finish the handoff by removing the
		// surviving inline child from Git (which lands OwnerNone → create).
		if !in.Current.RSPresent || !in.Current.RDPresent {
			missing, surviving := kindRS, kindRD
			if in.Current.RSPresent {
				missing, surviving = kindRD, kindRS
			}
			return Plan{
				Action: ActionNeedsHumanReview,
				Blockers: []string{fmt.Sprintf(
					"partial inline-argo state: %s is missing while inline-argo-owned %s is still present; "+
						"pvc-plumber will not create a conflicting child — restore the missing resource via Git/Argo, "+
						"or complete the handoff by removing the surviving inline %s so the operator can take full ownership",
					missing, surviving, surviving)},
			}
		}
		// Both children present and inline-argo-owned. Verdict is
		// matches-or-observed; never a write.
		if shapeMatches(in) {
			return Plan{
				Action: ActionAlreadyMatches,
				Notes:  []string{"inline-argo owns RS/RD; shape matches v4 expected, writes gated by ownership"},
			}
		}
		return Plan{
			Action:   ActionInlineArgoObserved,
			Blockers: []string{"inline-argo RS/RD differs from v4 expected shape; pvc-plumber will not patch GitOps-owned resources"},
		}

	case OwnerUnmanagedOrGitopsObserved:
		if shapeMatches(in) {
			return Plan{Action: ActionAlreadyMatches}
		}
		return Plan{
			Action:   ActionNeedsHumanReview,
			Blockers: []string{"unmanaged RS/RD (no managed-by label) differs from v4 expected shape; cannot safely adopt or update without explicit owner"},
		}

	case OwnerUnknown:
		return Plan{
			Action:   ActionNeedsHumanReview,
			Blockers: []string{fmt.Sprintf("RS/RD ownership classification is %q; cannot safely plan a write", in.Owner)},
		}
	}

	// Defensive fall-through (every OwnerClassification value is
	// handled above; reaching here would be a future enum addition
	// the planner doesn't know how to classify yet).
	return Plan{
		Action:   ActionNeedsHumanReview,
		Blockers: []string{fmt.Sprintf("unhandled OwnerClassification %q", in.Owner)},
	}
}

// planNotWriteEligible covers rules 7a-7f. The PVC is opted in to
// reporting (legacy label, or enabled-only) but the operator is
// gated off from writes — either because manage-volsync is missing
// or because the only opt-in is the legacy `backup:` label.
func planNotWriteEligible(in Inputs) Plan {
	gateBlocker := writeGateBlocker(in)

	switch in.Owner {
	case OwnerNone:
		// Reported but write-gated. The operator could create here
		// if the write gate were on; instead, surface the blocker.
		return Plan{Action: ActionWriteGateMissing, Blockers: []string{gateBlocker}}

	case OwnerPVCPlumber:
		// Operator owns the resources but the PVC is no longer
		// write-eligible. We do NOT delete or update — that would be
		// a destructive action on a PVC whose owner just removed
		// the manage gate. Surface the situation.
		if shapeMatches(in) {
			return Plan{
				Action: ActionAlreadyMatches,
				Notes:  []string{"operator-owned RS/RD present, but write gate is off (manage-volsync missing); no further writes will be applied"},
			}
		}
		return Plan{
			Action: ActionAlreadyMatches,
			Notes:  []string{"operator-owned RS/RD differs from v4 expected; write gate is off so no update is planned"},
		}

	case OwnerInlineArgo:
		if shapeMatches(in) {
			return Plan{
				Action: ActionAlreadyMatches,
				Notes:  []string{"inline-argo RS/RD matches expected shape; reporting-only (write gate off, ownership belongs to GitOps)"},
			}
		}
		return Plan{
			Action:   ActionInlineArgoObserved,
			Blockers: []string{"inline-argo RS/RD differs from v4 expected shape; pvc-plumber will not patch GitOps-owned resources"},
		}

	case OwnerUnmanagedOrGitopsObserved:
		if shapeMatches(in) {
			return Plan{Action: ActionAlreadyMatches}
		}
		return Plan{
			Action:   ActionNeedsHumanReview,
			Blockers: []string{"unmanaged RS/RD (no managed-by label) differs from v4 expected shape; cannot safely adopt"},
		}

	case OwnerUnknown:
		return Plan{
			Action:   ActionNeedsHumanReview,
			Blockers: []string{fmt.Sprintf("RS/RD ownership classification is %q; cannot safely plan a write", in.Owner)},
		}
	}

	return Plan{
		Action:   ActionNeedsHumanReview,
		Blockers: []string{fmt.Sprintf("unhandled OwnerClassification %q", in.Owner)},
	}
}

// writeGateBlocker returns the operator-friendly text explaining why
// a write was suppressed. The text differs based on which gate is
// missing (no enabled vs. no manage-volsync vs. legacy-only).
func writeGateBlocker(in Inputs) string {
	switch {
	case in.Spec.Enabled && !in.Spec.ManageVolSync:
		return "pvc-plumber.io/enabled=true is set, but pvc-plumber.io/manage-volsync=true is missing; PVC is reportable but not write-eligible"
	case !in.Spec.Enabled && in.Spec.LegacyTier != labels.TierUnspecified:
		return "legacy `backup:` label is reporting opt-in only; add pvc-plumber.io/enabled=true and pvc-plumber.io/manage-volsync=true to make this PVC write-eligible"
	default:
		// Defensive: planNotWriteEligible is only called when the
		// PVC is reportable (rules 4-5 short-circuit before this).
		// Provide a generic message so a future code-path change
		// doesn't silently produce an empty blocker.
		return "PVC is reportable but not write-eligible; check pvc-plumber.io/enabled and pvc-plumber.io/manage-volsync labels"
	}
}

// =============================================================================
// Shape matching + Op construction
// =============================================================================

// shapeMatches reports whether the observed CurrentState aligns with
// what the builder would produce for this PVC. Intentionally
// conservative: missing RS/RD never matches; differing repository or
// sourcePVC never matches. Schedule drift is only checked when
// RSSchedule is non-empty (caller controls whether to populate it).
func shapeMatches(in Inputs) bool {
	if !in.Current.RSPresent || !in.Current.RDPresent {
		return false
	}
	expectedRSName := in.PVCName
	expectedRDName := in.PVCName + "-dst"
	expectedRepo := in.DefaultRepoSecret
	if expectedRepo == "" {
		expectedRepo = naming.DefaultRepoSecretName
	}

	if in.Current.RSName != expectedRSName {
		return false
	}
	if in.Current.RDName != expectedRDName {
		return false
	}
	if in.Current.RSRepository != expectedRepo {
		return false
	}
	if in.Current.RDRepository != expectedRepo {
		return false
	}
	if in.Current.RSSourcePVC != "" && in.Current.RSSourcePVC != in.PVCName {
		return false
	}
	// Only check schedule drift for operator-owned resources — the
	// reconciler only populates RSSchedule in that case, and we don't
	// want incidental schedule differences on inline-argo resources to
	// count as drift (we'd never patch them anyway).
	if in.Owner == OwnerPVCPlumber && in.Current.RSSchedule != "" {
		expectedSchedule := builder.ScheduleFor(in.Namespace, in.PVCName, in.Spec.Tier)
		if in.Current.RSSchedule != expectedSchedule {
			return false
		}
	}
	return true
}

// createOps returns the full create plan for both RS and RD.
func createOps(in Inputs) []PlannedOp {
	bin := toBuilderInputs(in)
	return []PlannedOp{
		{Kind: OpCreate, Resource: builder.BuildRS(bin)},
		{Kind: OpCreate, Resource: builder.BuildRD(bin)},
	}
}

// createOpsForMissing returns create ops only for the children that
// are not currently present. Used by the partial-state branch of
// planWriteEligible / OwnerPVCPlumber.
func createOpsForMissing(in Inputs) []PlannedOp {
	bin := toBuilderInputs(in)
	ops := make([]PlannedOp, 0, 2)
	if !in.Current.RSPresent {
		ops = append(ops, PlannedOp{Kind: OpCreate, Resource: builder.BuildRS(bin)})
	}
	if !in.Current.RDPresent {
		ops = append(ops, PlannedOp{Kind: OpCreate, Resource: builder.BuildRD(bin)})
	}
	return ops
}

// updateOps returns the full update plan for both RS and RD. The
// executor is responsible for choosing the apply strategy
// (server-side apply, patch, etc.); the planner only carries the
// desired-state object.
func updateOps(in Inputs) []PlannedOp {
	bin := toBuilderInputs(in)
	return []PlannedOp{
		{Kind: OpUpdate, Resource: builder.BuildRS(bin)},
		{Kind: OpUpdate, Resource: builder.BuildRD(bin)},
	}
}

// deleteOps returns identifier-only unstructured objects for the
// executor to call Delete against. Full spec rendering is not
// needed for delete — namespace + name + GVK are sufficient.
func deleteOps(in Inputs) []PlannedOp {
	ops := make([]PlannedOp, 0, 2)
	if in.Current.RSPresent {
		ops = append(ops, PlannedOp{
			Kind:     OpDelete,
			Resource: makeIdentifier(rsGVK, in.Namespace, in.PVCName),
		})
	}
	if in.Current.RDPresent {
		ops = append(ops, PlannedOp{
			Kind:     OpDelete,
			Resource: makeIdentifier(rdGVK, in.Namespace, in.PVCName+"-dst"),
		})
	}
	return ops
}

func makeIdentifier(gvk schema.GroupVersionKind, namespace, name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(namespace)
	u.SetName(name)
	return u
}

func toBuilderInputs(in Inputs) builder.Inputs {
	return builder.Inputs{
		Namespace:            in.Namespace,
		PVCName:              in.PVCName,
		PVCCapacity:          in.PVCCapacity,
		PVCAccessModes:       in.PVCAccessModes,
		PVCStorageClass:      in.PVCStorageClass,
		Spec:                 in.Spec,
		NamingStrategy:       in.NamingStrategy,
		DefaultRepoSecret:    in.DefaultRepoSecret,
		DefaultSnapshotClass: in.DefaultSnapshotClass,
		DefaultCacheCapacity: in.DefaultCacheCapacity,
		DefaultStorageClass:  in.DefaultStorageClass,
		DefaultUID:           in.DefaultUID,
		DefaultGID:           in.DefaultGID,
		DefaultFSGroup:       in.DefaultFSGroup,
	}
}

// VolSync GVKs the planner ever emits. Kept here (not imported from
// builder) so the paranoia test in planner_test.go can compare
// without depending on builder internals.
const (
	volsyncGroup   = "volsync.backube"
	volsyncVersion = "v1alpha1"
	kindRS         = "ReplicationSource"
	kindRD         = "ReplicationDestination"
)

var (
	rsGVK = schema.GroupVersionKind{Group: volsyncGroup, Version: volsyncVersion, Kind: kindRS}
	rdGVK = schema.GroupVersionKind{Group: volsyncGroup, Version: volsyncVersion, Kind: kindRD}
)
