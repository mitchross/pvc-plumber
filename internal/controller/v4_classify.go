package controller

import (
	"github.com/mitchross/pvc-plumber/internal/v4/labels"
)

// Phase 5 — Patch 2: classification helpers.
//
// Pure functions over parsed label data. No Kubernetes client, no I/O.
// These are the building blocks the future V4AuditReconciler (Patch 3)
// will use to populate the LabelSource field on each ParityEntry and to
// decide whether a PVC is in scope for audit-mode parity reporting.

// ClassifyLabelSource translates the labels.Origin classification into
// the LabelSource enum used by the parity report. Maps:
//
//	OriginNew         → LabelSourceV4
//	OriginLegacyOnly  → LabelSourceLegacy
//	OriginBoth        → LabelSourceBoth
//	OriginNone (or any other) → LabelSourceNone
//
// Future admission webhooks still match only on pvc-plumber.io/enabled=true
// (per PRD constraint). This function is for the audit reconciler, which
// must include legacy-labeled PVCs in its report so the current talos
// repo's state is visible.
func ClassifyLabelSource(spec labels.Spec) LabelSource {
	switch spec.Origin {
	case labels.OriginNew:
		return LabelSourceV4
	case labels.OriginLegacyOnly:
		return LabelSourceLegacy
	case labels.OriginBoth:
		return LabelSourceBoth
	default:
		return LabelSourceNone
	}
}

// IsAuditOptedIn reports whether a PVC should have expected RS/RD
// computed by the audit reconciler. True when the LabelSource is
// anything other than None — i.e., either the new v4 label, the legacy
// `backup: …` label, or both are present.
//
// This is deliberately broader than future admission webhooks:
//   - Webhooks (Phase 8) will match only LabelSourceV4 / LabelSourceBoth
//     so legacy-labeled PVCs cannot be mutated/denied during the migration.
//   - The audit reconciler (Phase 5) must include LabelSourceLegacy too
//     so the parity report covers the current talos repo, which still
//     uses legacy labels on the bulk of its PVCs.
//
// Backup-exempt status is handled separately by DecideAction; IsAuditOptedIn
// answers "should we even compute expected state for this PVC?", not
// "what action should we take?". An exempt PVC may still be opted in by
// label (LabelSourceLegacy/V4/Both) — DecideAction's exempt branch wins
// before any expected/current comparison runs.
func IsAuditOptedIn(source LabelSource) bool {
	return source != LabelSourceNone
}
