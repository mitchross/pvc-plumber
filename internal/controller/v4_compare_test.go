package controller

import (
	"errors"
	"strings"
	"testing"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// helper: a default ExpectedState matching the inline pattern in the
// talos repo (PVC name "storage" in namespace "ns").
func defaultExpected() ExpectedState {
	return ComputeExpected("ns", testPVCStorageName, labels.Spec{}, naming.StrategyBareDst, DefaultRepoSecretName)
}

// helper: a default opted-in v4 spec for an enabled PVC.
func defaultV4Spec() labels.Spec {
	return labels.Spec{
		Origin:  labels.OriginNew,
		Enabled: true,
		Tier:    labels.TierDaily,
	}
}

// helper: a legacy-only spec (backup: daily).
func defaultLegacySpec() labels.Spec {
	return labels.Spec{
		Origin:     labels.OriginLegacyOnly,
		LegacyTier: labels.TierDaily,
		Tier:       labels.TierDaily, // parser propagates legacy tier
	}
}

// helper: CurrentState representing the talos inline pattern matching
// the default Expected (RS=storage managed-by=argocd, RD=storage-dst
// managed-by=argocd, both pointing at the shared repo).
func currentInlineArgoMatching() CurrentState {
	return CurrentState{
		RSPresent:    true,
		RSName:       testPVCStorageName,
		RSManagedBy:  ManagedByArgoCDLabelValue,
		RSRepository: testRepoSecretShare,
		RSSourcePVC:  testPVCStorageName,
		RDPresent:    true,
		RDName:       testPVCStorageName + "-dst",
		RDManagedBy:  ManagedByArgoCDLabelValue,
		RDRepository: testRepoSecretShare,
	}
}

// =============================================================================
// ClassifyOwner
// =============================================================================

func TestClassifyOwner_NoCurrentResources(t *testing.T) {
	got := ClassifyOwner(CurrentState{}, defaultExpected())
	if got != OwnerNone {
		t.Errorf("empty CurrentState: got %q, want %q", got, OwnerNone)
	}
}

func TestClassifyOwner_ManagedByPVCPlumber(t *testing.T) {
	cur := currentInlineArgoMatching()
	cur.RSManagedBy = ManagedByPVCPlumberLabelValue
	got := ClassifyOwner(cur, defaultExpected())
	if got != OwnerPVCPlumber {
		t.Errorf("managed-by=pvc-plumber on RS: got %q, want %q", got, OwnerPVCPlumber)
	}

	// Also when only RD carries the marker.
	cur = currentInlineArgoMatching()
	cur.RDManagedBy = ManagedByPVCPlumberLabelValue
	got = ClassifyOwner(cur, defaultExpected())
	if got != OwnerPVCPlumber {
		t.Errorf("managed-by=pvc-plumber on RD: got %q, want %q", got, OwnerPVCPlumber)
	}
}

func TestClassifyOwner_ManagedByArgoCD(t *testing.T) {
	got := ClassifyOwner(currentInlineArgoMatching(), defaultExpected())
	if got != OwnerInlineArgo {
		t.Errorf("managed-by=argocd: got %q, want %q", got, OwnerInlineArgo)
	}
}

func TestClassifyOwner_UnmanagedShapeMatches(t *testing.T) {
	cur := currentInlineArgoMatching()
	cur.RSManagedBy = "" // no marker
	cur.RDManagedBy = ""
	got := ClassifyOwner(cur, defaultExpected())
	if got != OwnerUnmanagedOrGitopsObserved {
		t.Errorf("unmanaged matching shape: got %q, want %q", got, OwnerUnmanagedOrGitopsObserved)
	}
}

func TestClassifyOwner_UnmanagedShapeDiffers(t *testing.T) {
	cur := currentInlineArgoMatching()
	cur.RSManagedBy = ""
	cur.RDManagedBy = ""
	cur.RSRepository = "some-other-repo" // shape differs
	got := ClassifyOwner(cur, defaultExpected())
	if got != OwnerUnknown {
		t.Errorf("unmanaged shape mismatch: got %q, want %q", got, OwnerUnknown)
	}
}

func TestClassifyOwner_PVCPlumberWinsOverArgo(t *testing.T) {
	// If both labels somehow coexist (shouldn't happen in practice but
	// defensive), pvc-plumber wins because it's checked first.
	cur := currentInlineArgoMatching()
	cur.RSManagedBy = ManagedByPVCPlumberLabelValue
	cur.RDManagedBy = ManagedByArgoCDLabelValue
	got := ClassifyOwner(cur, defaultExpected())
	if got != OwnerPVCPlumber {
		t.Errorf("pvc-plumber + argocd: got %q, want pvc-plumber (first match wins)", got)
	}
}

// =============================================================================
// DecideAction — exempt branches (contract item #3)
// =============================================================================

// Phase 5 contract #3: backup-exempt wins over backup intent. Even with
// a legacy `backup: daily` label, valid exempt → skipped-exempt.
func TestDecideAction_ExemptValid_OverridesLegacyBackupLabel(t *testing.T) {
	spec := defaultLegacySpec()
	spec.ExemptKind = labels.ExemptValid
	spec.ExemptReason = testReasonNASBacked

	got := DecideAction(spec, LabelSourceLegacy, CurrentState{}, defaultExpected(), OwnerNone)
	if got.Action != ActionSkippedExempt {
		t.Fatalf("exempt + legacy backup: got %q, want %q", got.Action, ActionSkippedExempt)
	}
	if !containsNote(got.Notes, "NAS-backed") {
		t.Errorf("exempt reason not in Notes: %+v", got.Notes)
	}
}

func TestDecideAction_ExemptMissingReason_BlocksWithFQHint(t *testing.T) {
	spec := labels.Spec{
		Origin:     labels.OriginNew,
		Enabled:    true,
		ExemptKind: labels.ExemptMissingReason,
	}
	got := DecideAction(spec, LabelSourceV4, CurrentState{}, defaultExpected(), OwnerNone)
	if got.Action != ActionNeedsHumanReview {
		t.Fatalf("exempt missing reason: got %q, want %q", got.Action, ActionNeedsHumanReview)
	}
	if len(got.Blockers) == 0 {
		t.Fatal("expected blocker explaining the FQ-reason contract")
	}
	if !containsNote(got.Blockers, labels.LegacyAnnotationBackupExemptReasonFQ) {
		t.Errorf("blocker doesn't mention FQ annotation: %v", got.Blockers)
	}
}

// =============================================================================
// DecideAction — spec parse errors → NeedsHumanReview with blockers
// =============================================================================

func TestDecideAction_SpecParseErrors_PopulateBlockers(t *testing.T) {
	spec := defaultV4Spec()
	spec.Errors = []error{
		errors.New("pvc-plumber.io/tier: invalid tier \"monthly\""),
		errors.New("pvc-plumber.io/uid: not an integer: \"abc\""),
	}
	got := DecideAction(spec, LabelSourceV4, CurrentState{}, defaultExpected(), OwnerNone)
	if got.Action != ActionNeedsHumanReview {
		t.Fatalf("parse errors: got %q, want %q", got.Action, ActionNeedsHumanReview)
	}
	if len(got.Blockers) != 2 {
		t.Errorf("blockers: got %d, want 2 (%v)", len(got.Blockers), got.Blockers)
	}
}

// =============================================================================
// DecideAction — opt-in / not opted in
// =============================================================================

func TestDecideAction_NotOptedIn(t *testing.T) {
	got := DecideAction(labels.Spec{}, LabelSourceNone, CurrentState{}, ExpectedState{}, OwnerNone)
	if got.Action != ActionSkippedNotOptedIn {
		t.Errorf("not opted in: got %q, want %q", got.Action, ActionSkippedNotOptedIn)
	}
}

// Phase 5 contract #1: legacy-only labels still produce a real verdict
// (not skipped-not-opted-in).
func TestDecideAction_LegacyOnlyIsAuditOptedIn(t *testing.T) {
	got := DecideAction(defaultLegacySpec(), LabelSourceLegacy, CurrentState{}, defaultExpected(), OwnerNone)
	if got.Action == ActionSkippedNotOptedIn {
		t.Fatalf("legacy-only PVC must not be skipped-not-opted-in; got %q", got.Action)
	}
	if got.Action != ActionWouldCreate {
		t.Errorf("legacy + no current: got %q, want %q", got.Action, ActionWouldCreate)
	}
	if !containsNote(got.Notes, "legacy") {
		t.Errorf("legacy migration note missing: %+v", got.Notes)
	}
}

// =============================================================================
// DecideAction — skip-restore semantics
// =============================================================================

func TestDecideAction_SkipRestoreWithoutReason_NeedsHumanReview(t *testing.T) {
	spec := defaultV4Spec()
	spec.SkipRestore = true
	got := DecideAction(spec, LabelSourceV4, CurrentState{}, defaultExpected(), OwnerNone)
	if got.Action != ActionNeedsHumanReview {
		t.Fatalf("skip-restore no reason: got %q, want %q", got.Action, ActionNeedsHumanReview)
	}
	if len(got.Blockers) == 0 {
		t.Fatal("expected blocker about skip-restore-reason")
	}
}

func TestDecideAction_SkipRestoreWithReason_FlowsThroughWithNote(t *testing.T) {
	spec := defaultV4Spec()
	spec.SkipRestore = true
	spec.SkipRestoreReason = "DR drill 2026-06-01"
	got := DecideAction(spec, LabelSourceV4, CurrentState{}, defaultExpected(), OwnerNone)
	if got.Action != ActionWouldCreate {
		t.Fatalf("skip-restore with reason should fall through to normal flow; got %q", got.Action)
	}
	if !containsNote(got.Notes, "DR drill 2026-06-01") {
		t.Errorf("skip-restore reason missing from Notes: %+v", got.Notes)
	}
}

// =============================================================================
// DecideAction — owner-driven branches (contract items #5, #6, #7)
// =============================================================================

// Phase 5 contract #6: opted-in PVC with no current resources → would-create.
func TestDecideAction_OwnerNone_WouldCreate(t *testing.T) {
	got := DecideAction(defaultV4Spec(), LabelSourceV4, CurrentState{}, defaultExpected(), OwnerNone)
	if got.Action != ActionWouldCreate {
		t.Errorf("opted-in + no current: got %q, want %q", got.Action, ActionWouldCreate)
	}
}

// Phase 5 contract #5: inline-Argo resource matching expected shape →
// already-matches (NOT would-delete, NOT inline-argo-observed).
func TestDecideAction_OwnerInlineArgo_ShapeMatches_AlreadyMatches(t *testing.T) {
	got := DecideAction(
		defaultV4Spec(),
		LabelSourceV4,
		currentInlineArgoMatching(),
		defaultExpected(),
		OwnerInlineArgo,
	)
	if got.Action != ActionAlreadyMatches {
		t.Errorf("inline-argo shape matches: got %q, want %q", got.Action, ActionAlreadyMatches)
	}
}

// Phase 5 contract #7 (variant): inline-Argo resource with spec mismatch.
// Action is inline-argo-observed (NOT would-update — the operator must not
// touch GitOps-owned resources), with diff notes.
func TestDecideAction_OwnerInlineArgo_ShapeDiffers_InlineArgoObserved(t *testing.T) {
	cur := currentInlineArgoMatching()
	cur.RSRepository = "drifted-repo"

	got := DecideAction(defaultV4Spec(), LabelSourceV4, cur, defaultExpected(), OwnerInlineArgo)
	if got.Action != ActionInlineArgoObserved {
		t.Errorf("inline-argo drift: got %q, want %q", got.Action, ActionInlineArgoObserved)
	}
	if !containsAny(got.Notes, "RS repository differs") {
		t.Errorf("repository diff note missing: %+v", got.Notes)
	}
	if !containsAny(got.Notes, "not a delete candidate") {
		t.Errorf("non-delete-candidate disclaimer missing: %+v", got.Notes)
	}
}

// Phase 5 contract #5: unmanaged-or-gitops (no managed-by, shape matches)
// → already-matches.
func TestDecideAction_OwnerUnmanagedOrGitops_ShapeMatches_AlreadyMatches(t *testing.T) {
	cur := currentInlineArgoMatching()
	cur.RSManagedBy = ""
	cur.RDManagedBy = ""
	got := DecideAction(
		defaultV4Spec(),
		LabelSourceV4,
		cur,
		defaultExpected(),
		OwnerUnmanagedOrGitopsObserved,
	)
	if got.Action != ActionAlreadyMatches {
		t.Errorf("unmanaged-or-gitops matching: got %q, want %q", got.Action, ActionAlreadyMatches)
	}
}

// Defensive: OwnerUnmanagedOrGitopsObserved with shape diff (which
// ClassifyOwner wouldn't normally produce, but a hand-constructed input
// might) → inline-argo-observed, never would-delete.
func TestDecideAction_OwnerUnmanagedOrGitops_ShapeDiffers_Observed(t *testing.T) {
	cur := currentInlineArgoMatching()
	cur.RSManagedBy = ""
	cur.RDManagedBy = ""
	cur.RDName = "wrong-name" // hand-constructed shape mismatch

	got := DecideAction(
		defaultV4Spec(),
		LabelSourceV4,
		cur,
		defaultExpected(),
		OwnerUnmanagedOrGitopsObserved,
	)
	if got.Action == ActionWouldDelete {
		t.Errorf("unmanaged-or-gitops must never be ActionWouldDelete; got %q", got.Action)
	}
	if got.Action != ActionInlineArgoObserved {
		t.Errorf("unmanaged-or-gitops drift: got %q, want %q", got.Action, ActionInlineArgoObserved)
	}
}

// Phase 5 contract #5: only managed-by=pvc-plumber resources are eligible
// for adopt/update/delete actions.
func TestDecideAction_OwnerPVCPlumber_ShapeMatches_AlreadyMatches(t *testing.T) {
	cur := currentInlineArgoMatching()
	cur.RSManagedBy = ManagedByPVCPlumberLabelValue
	cur.RDManagedBy = ManagedByPVCPlumberLabelValue

	got := DecideAction(defaultV4Spec(), LabelSourceV4, cur, defaultExpected(), OwnerPVCPlumber)
	if got.Action != ActionAlreadyMatches {
		t.Errorf("pvc-plumber shape matches: got %q, want %q", got.Action, ActionAlreadyMatches)
	}
}

// Phase 5 contract #7: operator-owned resource with spec mismatch →
// would-update, with diff notes.
func TestDecideAction_OwnerPVCPlumber_ShapeDiffers_WouldUpdate(t *testing.T) {
	cur := currentInlineArgoMatching()
	cur.RSManagedBy = ManagedByPVCPlumberLabelValue
	cur.RDManagedBy = ManagedByPVCPlumberLabelValue
	cur.RDName = "storage-OLD-suffix" // drift

	got := DecideAction(defaultV4Spec(), LabelSourceV4, cur, defaultExpected(), OwnerPVCPlumber)
	if got.Action != ActionWouldUpdate {
		t.Errorf("pvc-plumber drift: got %q, want %q", got.Action, ActionWouldUpdate)
	}
	if !containsAny(got.Notes, "RD name differs") {
		t.Errorf("RD diff note missing: %+v", got.Notes)
	}
}

// Owner=unknown (shape mismatch, no recognized managed-by) →
// needs-human-review.
func TestDecideAction_OwnerUnknown_NeedsHumanReview(t *testing.T) {
	cur := currentInlineArgoMatching()
	cur.RSManagedBy = ""
	cur.RDManagedBy = ""
	cur.RSRepository = "totally-different-repo"

	got := DecideAction(defaultV4Spec(), LabelSourceV4, cur, defaultExpected(), OwnerUnknown)
	if got.Action != ActionNeedsHumanReview {
		t.Errorf("unknown owner: got %q, want %q", got.Action, ActionNeedsHumanReview)
	}
	if len(got.Blockers) == 0 {
		t.Fatal("expected at least one blocker for unknown owner")
	}
}

// Defensive: unrecognized OwnerClassification enum value falls through
// to NeedsHumanReview with a blocker, never silently treats the resource
// as deletable.
func TestDecideAction_UnrecognizedOwner_DefensiveNeedsReview(t *testing.T) {
	got := DecideAction(
		defaultV4Spec(),
		LabelSourceV4,
		currentInlineArgoMatching(),
		defaultExpected(),
		OwnerClassification("invented-value"),
	)
	if got.Action != ActionNeedsHumanReview {
		t.Errorf("unknown enum value: got %q, want %q", got.Action, ActionNeedsHumanReview)
	}
	if len(got.Blockers) == 0 {
		t.Fatal("expected a defensive blocker")
	}
}

// =============================================================================
// describeShapeDiffs — direct coverage
// =============================================================================

func TestDescribeShapeDiffs_MissingResources(t *testing.T) {
	exp := defaultExpected()
	notes := describeShapeDiffs(CurrentState{}, exp)
	if !containsAny(notes, "RS "+exp.RSName+" not present") {
		t.Errorf("RS-missing note: %+v", notes)
	}
	if !containsAny(notes, "RD "+exp.RDName+" not present") {
		t.Errorf("RD-missing note: %+v", notes)
	}
}

func TestDescribeShapeDiffs_NoDiffsReturnsNil(t *testing.T) {
	notes := describeShapeDiffs(currentInlineArgoMatching(), defaultExpected())
	if notes != nil {
		t.Errorf("expected nil when shape matches; got %+v", notes)
	}
}

// =============================================================================
// Contract item #1 + #2 + #3 integration sanity
// =============================================================================

// Phase 5 contract #1 + #6: a real-world talos repo PVC with `backup: daily`,
// no v4 label, no existing RS/RD → ActionWouldCreate with legacy migration
// note. This is the CURRENT state of most talos repo PVCs.
func TestDecideAction_TalosRepoTypical_LegacyOnlyPVCNoCurrentResources(t *testing.T) {
	spec := defaultLegacySpec()
	got := DecideAction(spec, LabelSourceLegacy, CurrentState{}, defaultExpected(), OwnerNone)

	if got.Action != ActionWouldCreate {
		t.Errorf("legacy-only PVC, no current: got %q, want %q", got.Action, ActionWouldCreate)
	}
	if !containsAny(got.Notes, "legacy") {
		t.Errorf("legacy-label migration note missing: %+v", got.Notes)
	}
}

// Phase 5 contract #5: a real-world talos repo PVC with `backup: daily`
// AND inline-Argo RS/RD matching expected shape → already-matches with
// legacy migration note. This is the state migrated PVCs are in.
func TestDecideAction_TalosRepoMigrated_LegacyPVCWithInlineArgo(t *testing.T) {
	spec := defaultLegacySpec()
	got := DecideAction(
		spec,
		LabelSourceLegacy,
		currentInlineArgoMatching(),
		defaultExpected(),
		OwnerInlineArgo,
	)

	if got.Action != ActionAlreadyMatches {
		t.Errorf("legacy + inline-Argo matching: got %q, want %q", got.Action, ActionAlreadyMatches)
	}
	if !containsAny(got.Notes, "legacy") {
		t.Errorf("legacy migration note missing: %+v", got.Notes)
	}
}

// Phase 5 contract #2: when both v4 and legacy labels are present, the
// notes flag "v4 wins for tier/mode resolution".
func TestDecideAction_BothLabelSources_NoteMentionsV4Wins(t *testing.T) {
	spec := labels.Spec{
		Origin:     labels.OriginBoth,
		Enabled:    true,
		Tier:       labels.TierHourly,
		LegacyTier: labels.TierDaily,
	}
	got := DecideAction(
		spec,
		LabelSourceBoth,
		currentInlineArgoMatching(),
		defaultExpected(),
		OwnerInlineArgo,
	)
	if got.Action != ActionAlreadyMatches {
		t.Errorf("both labels: got %q, want %q", got.Action, ActionAlreadyMatches)
	}
	if !containsAny(got.Notes, "v4 wins") {
		t.Errorf("v4-wins note missing: %+v", got.Notes)
	}
}

// =============================================================================
// helpers
// =============================================================================

func containsNote(notes []string, substr string) bool {
	return containsAny(notes, substr)
}

func containsAny(notes []string, substr string) bool {
	for _, n := range notes {
		if strings.Contains(n, substr) {
			return true
		}
	}
	return false
}
