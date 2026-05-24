package controller

import (
	"fmt"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
)

// Phase 5 — Patch 2: ownership classification + action decision.
//
// Pure functions over data — no Kubernetes client, no I/O. The future
// V4AuditReconciler (Patch 3) will read RS/RD from the cluster, populate
// CurrentState, and call these helpers to produce the per-PVC ParityEntry.

// Managed-by label values the audit reconciler recognizes. These are
// case-sensitive matches against `app.kubernetes.io/managed-by`.
//
// ManagedByPVCPlumberLabelValue is the marker the v4 reconciler will
// stamp on resources it creates (Phase 6 will write this); it's also
// what the v3.1 reconciler used historically. Resources carrying this
// value are the ONLY ones eligible for would-adopt / would-update /
// would-delete by the v4 reconciler.
const (
	ManagedByPVCPlumberLabelValue = "pvc-plumber"
	ManagedByArgoCDLabelValue     = "argocd"
)

// ClassifyOwner inspects the observed CurrentState (specifically the
// managed-by labels and a structural match against Expected) to determine
// which OwnerClassification applies. Pure.
//
// Rules (ordered):
//
//  1. No current RS and no current RD              → OwnerNone
//  2. Either current resource carries managed-by=
//     pvc-plumber                                  → OwnerPVCPlumber
//  3. Either current resource carries managed-by=
//     argocd                                       → OwnerInlineArgo
//  4. Neither marker; names + repository + sourcePVC
//     match Expected                               → OwnerUnmanagedOrGitopsObserved
//  5. Otherwise                                    → OwnerUnknown
//
// Rule 4 is the catch-all that protects Helm `extraDeploy:` shapes and
// any GitOps path that doesn't carry app.kubernetes.io/managed-by: the
// resource is observed and never a delete candidate.
func ClassifyOwner(current CurrentState, expected ExpectedState) OwnerClassification {
	if !current.RSPresent && !current.RDPresent {
		return OwnerNone
	}
	if current.RSManagedBy == ManagedByPVCPlumberLabelValue ||
		current.RDManagedBy == ManagedByPVCPlumberLabelValue {
		return OwnerPVCPlumber
	}
	if current.RSManagedBy == ManagedByArgoCDLabelValue ||
		current.RDManagedBy == ManagedByArgoCDLabelValue {
		return OwnerInlineArgo
	}
	if currentShapeMatchesExpected(current, expected) {
		return OwnerUnmanagedOrGitopsObserved
	}
	return OwnerUnknown
}

// currentShapeMatchesExpected is the strict equality test used by
// ClassifyOwner (to decide unmanaged-or-gitops vs unknown) and by
// DecideAction (to decide already-matches vs would-update / observed-
// drift). Both current resources must be present, named correctly, and
// reference the expected repository.
func currentShapeMatchesExpected(current CurrentState, expected ExpectedState) bool {
	if !current.RSPresent || !current.RDPresent {
		return false
	}
	if current.RSName != expected.RSName {
		return false
	}
	if current.RDName != expected.RDName {
		return false
	}
	// RSRepository is the load-bearing field; RDRepository is checked when
	// reported (some fake-client paths may not surface it).
	if current.RSRepository != expected.RepositorySecret {
		return false
	}
	if current.RDRepository != "" && current.RDRepository != expected.RepositorySecret {
		return false
	}
	// sourcePVC, when reported, must equal the PVC name (which equals
	// expected.RSName in bare-dst).
	if current.RSSourcePVC != "" && current.RSSourcePVC != expected.RSName {
		return false
	}
	return true
}

// ActionDecision is the structured output of DecideAction. The Action
// field is the high-level verdict; Blockers carry contract violations
// that prevent a clean decision (typically paired with ActionNeedsHumanReview);
// Notes carry human-readable observations that show up in the /audit
// report for any action kind.
type ActionDecision struct {
	Action   ActionKind
	Blockers []string
	Notes    []string
}

// DecideAction is the parity-decision engine. Pure: returns the verdict
// for a single PVC given its parsed Spec, classified LabelSource, observed
// CurrentState, computed ExpectedState, and OwnerClassification.
//
// Order of branches (deliberately documented because subtle):
//
//	(1) Backup-exempt with VALID FQ reason → skipped-exempt.
//	    (Wins over backup labels per Phase 5 contract #3.)
//	(2) Backup-exempt with missing reason → needs-human-review (blocker).
//	(3) Spec parse errors → needs-human-review (blockers from spec.Errors).
//	(4) Not opted in (LabelSourceNone) → skipped-not-opted-in.
//	(5) Skip-restore without reason → needs-human-review.
//	(6) Owner-driven branches:
//	    a. OwnerNone           → would-create
//	    b. OwnerPVCPlumber + shape matches → already-matches
//	    c. OwnerPVCPlumber + shape differs → would-update (with diff notes)
//	    d. OwnerInlineArgo + shape matches → already-matches
//	    e. OwnerInlineArgo + shape differs → inline-argo-observed (notes)
//	    f. OwnerUnmanagedOrGitopsObserved (shape matches by classification)
//	                                       → already-matches
//	    g. OwnerUnknown        → needs-human-review (blocker)
//
// LabelSourceLegacy / LabelSourceBoth append migration-progress notes to
// any verdict so the /audit report surfaces "still on legacy labels" /
// "v4 wins over legacy" without changing the verdict itself.
func DecideAction(
	spec labels.Spec,
	source LabelSource,
	current CurrentState,
	expected ExpectedState,
	owner OwnerClassification,
) ActionDecision {
	out := ActionDecision{}

	// (1) Backup-exempt wins.
	if spec.ExemptKind == labels.ExemptValid {
		out.Action = ActionSkippedExempt
		if spec.ExemptReason != "" {
			out.Notes = append(out.Notes, "backup-exempt: "+spec.ExemptReason)
		}
		return out
	}

	// (2) Backup-exempt label present but FQ reason missing — known DR
	// landmine. Refuse to act without it.
	if spec.ExemptKind == labels.ExemptMissingReason {
		out.Action = ActionNeedsHumanReview
		out.Blockers = append(out.Blockers,
			"backup-exempt=true is set but annotation "+
				labels.LegacyAnnotationBackupExemptReasonFQ+
				" is missing or empty")
		return out
	}

	// (3) Spec parse errors (bad tier, bad UID/GID, malformed min-age, …)
	if len(spec.Errors) > 0 {
		out.Action = ActionNeedsHumanReview
		for _, e := range spec.Errors {
			out.Blockers = append(out.Blockers, e.Error())
		}
		return out
	}

	// (4) Not opted in.
	if source == LabelSourceNone {
		out.Action = ActionSkippedNotOptedIn
		return out
	}

	// (5) Skip-restore contract violation.
	if spec.SkipRestore && spec.SkipRestoreReason == "" {
		out.Action = ActionNeedsHumanReview
		out.Blockers = append(out.Blockers,
			"pvc-plumber.io/skip-restore=true requires "+
				"pvc-plumber.io/skip-restore-reason to be set and non-empty")
		return out
	}
	if spec.SkipRestore {
		out.Notes = append(out.Notes,
			"skip-restore acknowledged: "+spec.SkipRestoreReason)
	}

	// (6) Owner-driven branches.
	shapeOK := currentShapeMatchesExpected(current, expected)

	switch owner {
	case OwnerNone:
		out.Action = ActionWouldCreate
		out.Notes = append(out.Notes,
			fmt.Sprintf("no current RS %q or RD %q present; would create both",
				expected.RSName, expected.RDName))

	case OwnerPVCPlumber:
		if shapeOK {
			out.Action = ActionAlreadyMatches
		} else {
			out.Action = ActionWouldUpdate
			out.Notes = append(out.Notes, describeShapeDiffs(current, expected)...)
		}

	case OwnerInlineArgo:
		if shapeOK {
			out.Action = ActionAlreadyMatches
		} else {
			out.Action = ActionInlineArgoObserved
			out.Notes = append(out.Notes,
				"current resources owned by argocd; observed only (not a delete candidate)")
			out.Notes = append(out.Notes, describeShapeDiffs(current, expected)...)
		}

	case OwnerUnmanagedOrGitopsObserved:
		// ClassifyOwner only returns this when shape already matches.
		// Defensive guard for callers constructing ActionDecision inputs
		// by hand.
		if shapeOK {
			out.Action = ActionAlreadyMatches
			out.Notes = append(out.Notes,
				"current resources match expected v4 shape; no managed-by label observed — treated as GitOps-managed by an unrecognized path")
		} else {
			out.Action = ActionInlineArgoObserved
			out.Notes = append(out.Notes,
				"current resources lack managed-by label and shape differs from expected; observed only (not a delete candidate)")
			out.Notes = append(out.Notes, describeShapeDiffs(current, expected)...)
		}

	case OwnerUnknown:
		out.Action = ActionNeedsHumanReview
		out.Blockers = append(out.Blockers,
			"existing resources do not carry a recognized managed-by label and their shape does not match the v4 expected state")
		out.Notes = append(out.Notes, describeShapeDiffs(current, expected)...)

	default:
		// Unknown enum value — defensive.
		out.Action = ActionNeedsHumanReview
		out.Blockers = append(out.Blockers,
			"unrecognized owner classification: "+string(owner))
	}

	// Migration-progress notes (do not change the verdict).
	switch source {
	case LabelSourceLegacy:
		out.Notes = append(out.Notes,
			"opted in via legacy label `backup: …` — migration target: pvc-plumber.io/enabled=true")
	case LabelSourceBoth:
		out.Notes = append(out.Notes,
			"both v4 and legacy labels are set; v4 wins for tier/mode resolution")
	}

	return out
}

// describeShapeDiffs returns human-readable notes describing how the
// observed CurrentState differs from the computed ExpectedState. Used
// for would-update notes (operator-owned drift) and inline-argo-observed
// notes (drift the operator must not touch).
func describeShapeDiffs(current CurrentState, expected ExpectedState) []string {
	var out []string

	if !current.RSPresent {
		out = append(out, "RS "+expected.RSName+" not present")
	} else {
		if current.RSName != expected.RSName {
			out = append(out, fmt.Sprintf(
				"RS name differs: current=%q expected=%q",
				current.RSName, expected.RSName))
		}
		if current.RSRepository != "" && current.RSRepository != expected.RepositorySecret {
			out = append(out, fmt.Sprintf(
				"RS repository differs: current=%q expected=%q",
				current.RSRepository, expected.RepositorySecret))
		}
		if current.RSSourcePVC != "" && current.RSSourcePVC != expected.RSName {
			out = append(out, fmt.Sprintf(
				"RS sourcePVC differs: current=%q expected=%q",
				current.RSSourcePVC, expected.RSName))
		}
	}

	if !current.RDPresent {
		out = append(out, "RD "+expected.RDName+" not present")
	} else {
		if current.RDName != expected.RDName {
			out = append(out, fmt.Sprintf(
				"RD name differs: current=%q expected=%q",
				current.RDName, expected.RDName))
		}
		if current.RDRepository != "" && current.RDRepository != expected.RepositorySecret {
			out = append(out, fmt.Sprintf(
				"RD repository differs: current=%q expected=%q",
				current.RDRepository, expected.RepositorySecret))
		}
	}

	if len(out) == 0 {
		return nil
	}
	return out
}
