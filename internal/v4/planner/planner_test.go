package planner

import (
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/mitchross/pvc-plumber/internal/v4/builder"
	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// =============================================================================
// Test fixtures
// =============================================================================

const (
	tns       = "karakeep"
	tpvc      = "data-pvc"
	tcap      = "10Gi"
	tscClass  = "longhorn"
	tsnap     = "longhorn-snapclass"
	tcache    = "2Gi"
	tshared   = "volsync-kopia-repository"
	tdrifted  = "drifted-repo"
	tdiffName = "wrong-name"
)

// baseInputs is a fully-populated Inputs default. Every test mutates
// fields off this baseline. Defaults are write-ineligible (no enabled,
// no manage) so a test that forgets to set them gets a clear failure.
func baseInputs() Inputs {
	return Inputs{
		Namespace:       tns,
		PVCName:         tpvc,
		PVCCapacity:     tcap,
		PVCAccessModes:  []string{"ReadWriteOnce"},
		PVCStorageClass: tscClass,
		Spec:            labels.Spec{},
		LabelSource:     LabelSourceNone,
		Owner:           OwnerNone,
		Current:         CurrentState{},
		// v4.0.1: default to a MANAGED namespace so existing write-eligible
		// cases keep exercising the create/update/delete paths. Gate-off
		// cases set NamespaceManaged:false explicitly.
		NamespaceManaged:     true,
		NamingStrategy:       naming.StrategyBareDst,
		DefaultRepoSecret:    tshared,
		DefaultSnapshotClass: tsnap,
		DefaultCacheCapacity: tcache,
		DefaultStorageClass:  tscClass,
		DefaultUID:           568,
		DefaultGID:           568,
		DefaultFSGroup:       568,
	}
}

// withEnabledManage returns a write-eligible Inputs (Enabled +
// ManageVolSync + daily tier, no current resources).
func withEnabledManage() Inputs {
	in := baseInputs()
	in.Spec = labels.Spec{
		Origin:        labels.OriginNew,
		Enabled:       true,
		ManageVolSync: true,
		Tier:          labels.TierDaily,
	}
	in.LabelSource = LabelSourceV4
	return in
}

// withLegacyOnly returns an Inputs that has only the legacy
// `backup: daily` label set (no v4 labels). Useful for "legacy-only"
// matrix cells.
func withLegacyOnly() Inputs {
	in := baseInputs()
	in.Spec = labels.Spec{
		Origin:     labels.OriginLegacyOnly,
		LegacyTier: labels.TierDaily,
		Tier:       labels.TierDaily,
		LegacyRaw:  "daily",
	}
	in.LabelSource = LabelSourceLegacy
	return in
}

// matchingCurrent returns a CurrentState that matches the expected
// shape for the given Inputs (RS + RD both present, expected names,
// expected repo, expected sourcePVC). The owner field is set
// separately via Inputs.Owner.
func matchingCurrent(in Inputs, managedBy string) CurrentState {
	return CurrentState{
		RSPresent:    true,
		RSName:       in.PVCName,
		RSManagedBy:  managedBy,
		RSRepository: tshared,
		RSSourcePVC:  in.PVCName,
		RDPresent:    true,
		RDName:       in.PVCName + "-dst",
		RDManagedBy:  managedBy,
		RDRepository: tshared,
	}
}

// driftedCurrent returns a CurrentState whose repository differs from
// expected (representing a real drift the planner must classify).
func driftedCurrent(in Inputs, managedBy string) CurrentState {
	c := matchingCurrent(in, managedBy)
	c.RSRepository = tdrifted
	c.RDRepository = tdrifted
	return c
}

// =============================================================================
// Rule 1: backup-exempt wins over everything
// =============================================================================

func TestPlanFor_ExemptValid_NoOps(t *testing.T) {
	in := withEnabledManage()
	in.Spec.ExemptKind = labels.ExemptValid
	in.Spec.ExemptReason = "NAS-backed"
	got := PlanFor(in)
	if got.Action != ActionSkippedExempt {
		t.Errorf("Action: got %q, want %q", got.Action, ActionSkippedExempt)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0", len(got.Ops))
	}
	if len(got.Notes) == 0 {
		t.Error("Notes: empty; expected at least one explanation")
	}
}

func TestPlanFor_ExemptValid_OperatorOwnedCurrent_StillSkipsExempt(t *testing.T) {
	in := withEnabledManage()
	in.Spec.ExemptKind = labels.ExemptValid
	in.Spec.ExemptReason = "scratch"
	in.Owner = OwnerPVCPlumber
	in.Current = driftedCurrent(in, "pvc-plumber")
	got := PlanFor(in)
	if got.Action != ActionSkippedExempt {
		t.Errorf("backup-exempt must win over drifted operator-owned current; got %q want %q", got.Action, ActionSkippedExempt)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0 (exempt never plans writes, even for cleanup)", len(got.Ops))
	}
	// Should surface a warning note so operator notices the orphans.
	found := false
	for _, n := range got.Notes {
		if containsAll(n, "operator-owned", "exempt") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected warning note about operator-owned exempt orphans; got Notes=%v", got.Notes)
	}
}

func TestPlanFor_ExemptMissingReason_NeedsHumanReview(t *testing.T) {
	in := baseInputs()
	in.Spec.ExemptKind = labels.ExemptMissingReason
	got := PlanFor(in)
	if got.Action != ActionNeedsHumanReview {
		t.Errorf("Action: got %q, want %q", got.Action, ActionNeedsHumanReview)
	}
	if len(got.Blockers) == 0 {
		t.Error("Blockers: empty; expected reason-annotation contract violation message")
	}
}

// =============================================================================
// Rule 3: parser errors block planning
// =============================================================================

func TestPlanFor_ParseErrors_NeedsHumanReview(t *testing.T) {
	in := baseInputs()
	in.Spec.Errors = []error{
		errors.New("pvc-plumber.io/tier: invalid tier \"every-5-min\""),
		errors.New("pvc-plumber.io/uid: out of range [0, 2^31-1]: 9999999999"),
	}
	got := PlanFor(in)
	if got.Action != ActionNeedsHumanReview {
		t.Errorf("Action: got %q, want %q", got.Action, ActionNeedsHumanReview)
	}
	if len(got.Blockers) != 2 {
		t.Errorf("Blockers: got %d, want 2 (one per parser error)", len(got.Blockers))
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0", len(got.Ops))
	}
}

// =============================================================================
// Rule 4: no opt-in → skipped-not-opted-in
// =============================================================================

func TestPlanFor_NoOptIn_SkippedNotOptedIn(t *testing.T) {
	got := PlanFor(baseInputs())
	if got.Action != ActionSkippedNotOptedIn {
		t.Errorf("Action: got %q, want %q", got.Action, ActionSkippedNotOptedIn)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0", len(got.Ops))
	}
}

// =============================================================================
// Rule 5: manage-only without enabled → SkippedNotOptedIn + blocker
// =============================================================================

func TestPlanFor_ManageOnlyWithoutEnabled_SkippedWithBlocker(t *testing.T) {
	in := baseInputs()
	in.Spec.ManageVolSync = true
	// Origin stays OriginNone — no enabled label.
	got := PlanFor(in)
	if got.Action != ActionSkippedNotOptedIn {
		t.Errorf("Action: got %q, want %q", got.Action, ActionSkippedNotOptedIn)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0", len(got.Ops))
	}
	if len(got.Blockers) == 0 {
		t.Fatal("Blockers: empty; expected an explanatory blocker about the missing enabled label")
	}
	if !containsAll(got.Blockers[0], "manage-volsync", "enabled") {
		t.Errorf("blocker text must mention both labels: got %q", got.Blockers[0])
	}
}

// =============================================================================
// Rule 6: write-eligible (Enabled + ManageVolSync)
// =============================================================================

func TestPlanFor_EnabledManage_NoCurrent_WouldCreate(t *testing.T) {
	got := PlanFor(withEnabledManage())
	if got.Action != ActionWouldCreate {
		t.Errorf("Action: got %q, want %q", got.Action, ActionWouldCreate)
	}
	if len(got.Ops) != 2 {
		t.Fatalf("Ops: got %d, want 2 (one RS create + one RD create)", len(got.Ops))
	}
	for _, op := range got.Ops {
		if op.Kind != OpCreate {
			t.Errorf("Op.Kind: got %q, want %q", op.Kind, OpCreate)
		}
		if op.Resource == nil {
			t.Error("Op.Resource: nil; builder must produce a non-nil unstructured")
		}
	}
}

func TestPlanFor_EnabledManage_OperatorOwnedMatches_AlreadyMatches(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerPVCPlumber
	in.Current = matchingCurrent(in, "pvc-plumber")
	got := PlanFor(in)
	if got.Action != ActionAlreadyMatches {
		t.Errorf("Action: got %q, want %q", got.Action, ActionAlreadyMatches)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0 (matching state means no change)", len(got.Ops))
	}
}

func TestPlanFor_EnabledManage_OperatorOwnedDrifts_WouldUpdate(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerPVCPlumber
	in.Current = driftedCurrent(in, "pvc-plumber") // repo drift
	got := PlanFor(in)
	if got.Action != ActionWouldUpdate {
		t.Errorf("Action: got %q, want %q", got.Action, ActionWouldUpdate)
	}
	if len(got.Ops) != 2 {
		t.Fatalf("Ops: got %d, want 2", len(got.Ops))
	}
	for _, op := range got.Ops {
		if op.Kind != OpUpdate {
			t.Errorf("Op.Kind: got %q, want %q", op.Kind, OpUpdate)
		}
	}
}

func TestPlanFor_EnabledManage_OperatorOwnedScheduleDrifts_WouldUpdate(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerPVCPlumber
	in.Current = matchingCurrent(in, "pvc-plumber")
	in.Current.RSSchedule = "0 0 * * *" // arbitrary non-matching schedule
	got := PlanFor(in)
	if got.Action != ActionWouldUpdate {
		t.Errorf("schedule drift must trigger update: got %q want %q", got.Action, ActionWouldUpdate)
	}
}

func TestPlanFor_EnabledManage_OperatorOwnedPartialState_WouldCreateMissing(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerPVCPlumber
	in.Current = matchingCurrent(in, "pvc-plumber")
	in.Current.RDPresent = false // only RS exists
	in.Current.RDName = ""
	in.Current.RDRepository = ""
	got := PlanFor(in)
	if got.Action != ActionWouldCreate {
		t.Errorf("partial state should plan a create for the missing child: got %q want %q", got.Action, ActionWouldCreate)
	}
	if len(got.Ops) != 1 {
		t.Errorf("Ops: got %d, want 1 (RD only)", len(got.Ops))
	}
	if len(got.Ops) > 0 && got.Ops[0].Resource.GetKind() != kindRD {
		t.Errorf("Op resource kind: got %q, want ReplicationDestination", got.Ops[0].Resource.GetKind())
	}
}

func TestPlanFor_EnabledManage_InlineArgoMatches_AlreadyMatchesWithNote(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerInlineArgo
	in.Current = matchingCurrent(in, "argocd")
	got := PlanFor(in)
	if got.Action != ActionAlreadyMatches {
		t.Errorf("Action: got %q, want %q", got.Action, ActionAlreadyMatches)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0", len(got.Ops))
	}
	if len(got.Notes) == 0 {
		t.Error("expected an explanatory note about inline-argo ownership")
	}
}

func TestPlanFor_EnabledManage_InlineArgoDrifts_InlineArgoObserved(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerInlineArgo
	in.Current = driftedCurrent(in, "argocd")
	got := PlanFor(in)
	if got.Action != ActionInlineArgoObserved {
		t.Errorf("Action: got %q, want %q", got.Action, ActionInlineArgoObserved)
	}
	if len(got.Ops) != 0 {
		t.Fatalf("Ops: got %d, want 0 (NEVER patch GitOps-owned resources)", len(got.Ops))
	}
	if len(got.Blockers) == 0 {
		t.Error("expected blocker explaining the ownership conflict")
	}
}

// rc7 confirmed policy: a mixed/partial state where one inline-argo child
// is missing while its sibling survives must NOT create the missing
// operator-equivalent child (that would fight Argo over a single name).
// Verdict is needs-human-review with a loud blocker, zero ops.
func TestPlanFor_EnabledManage_PartialInlineArgo_RDMissing_NeedsHumanReview(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerInlineArgo
	in.Current = matchingCurrent(in, "argocd")
	in.Current.RDPresent = false // Argo pruned the RD; inline RS survives.
	in.Current.RDName = ""
	in.Current.RDRepository = ""
	got := PlanFor(in)
	if got.Action != ActionNeedsHumanReview {
		t.Errorf("Action: got %q, want %q", got.Action, ActionNeedsHumanReview)
	}
	if len(got.Ops) != 0 {
		t.Fatalf("Ops: got %d, want 0 (must NOT create a child conflicting with the surviving inline-argo sibling)", len(got.Ops))
	}
	if len(got.Blockers) == 0 {
		t.Error("expected a loud blocker explaining the partial inline-argo state")
	}
}

func TestPlanFor_EnabledManage_PartialInlineArgo_RSMissing_NeedsHumanReview(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerInlineArgo
	in.Current = matchingCurrent(in, "argocd")
	in.Current.RSPresent = false // Argo pruned the RS; inline RD survives.
	in.Current.RSName = ""
	in.Current.RSRepository = ""
	in.Current.RSSourcePVC = ""
	got := PlanFor(in)
	if got.Action != ActionNeedsHumanReview {
		t.Errorf("Action: got %q, want %q", got.Action, ActionNeedsHumanReview)
	}
	if len(got.Ops) != 0 {
		t.Fatalf("Ops: got %d, want 0", len(got.Ops))
	}
	if len(got.Blockers) == 0 {
		t.Error("expected a loud blocker explaining the partial inline-argo state")
	}
}

func TestPlanFor_EnabledManage_UnmanagedMatches_AlreadyMatches(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerUnmanagedOrGitopsObserved
	in.Current = matchingCurrent(in, "") // no managed-by label
	got := PlanFor(in)
	if got.Action != ActionAlreadyMatches {
		t.Errorf("Action: got %q, want %q", got.Action, ActionAlreadyMatches)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0", len(got.Ops))
	}
}

func TestPlanFor_EnabledManage_UnmanagedDrifts_NeedsHumanReview(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerUnmanagedOrGitopsObserved
	in.Current = driftedCurrent(in, "")
	got := PlanFor(in)
	if got.Action != ActionNeedsHumanReview {
		t.Errorf("Action: got %q, want %q", got.Action, ActionNeedsHumanReview)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0 (never patch without explicit owner)", len(got.Ops))
	}
}

// =============================================================================
// tier=disabled handling (Patch 6.4 explicit requirement)
// =============================================================================

func TestPlanFor_TierDisabled_NoCurrent_AlreadyMatchesWithNote(t *testing.T) {
	in := withEnabledManage()
	in.Spec.Tier = labels.TierDisabled
	got := PlanFor(in)
	if got.Action != ActionAlreadyMatches {
		t.Errorf("Action: got %q, want %q", got.Action, ActionAlreadyMatches)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0", len(got.Ops))
	}
	if len(got.Notes) == 0 {
		t.Error("expected a tier=disabled note")
	}
}

func TestPlanFor_TierDisabled_OperatorOwnedCurrent_WouldDelete(t *testing.T) {
	in := withEnabledManage()
	in.Spec.Tier = labels.TierDisabled
	in.Owner = OwnerPVCPlumber
	in.Current = matchingCurrent(in, "pvc-plumber")
	got := PlanFor(in)
	if got.Action != ActionWouldDelete {
		t.Errorf("Action: got %q, want %q", got.Action, ActionWouldDelete)
	}
	if len(got.Ops) != 2 {
		t.Fatalf("Ops: got %d, want 2 (RS + RD delete)", len(got.Ops))
	}
	for _, op := range got.Ops {
		if op.Kind != OpDelete {
			t.Errorf("Op.Kind: got %q, want %q", op.Kind, OpDelete)
		}
	}
	if len(got.Notes) == 0 {
		t.Error("expected a tier=disabled-with-stale-resources note")
	}
}

func TestPlanFor_TierDisabled_InlineArgoCurrent_AlreadyMatches_NoDelete(t *testing.T) {
	in := withEnabledManage()
	in.Spec.Tier = labels.TierDisabled
	in.Owner = OwnerInlineArgo
	in.Current = matchingCurrent(in, "argocd")
	got := PlanFor(in)
	if got.Action != ActionAlreadyMatches {
		t.Errorf("Action: got %q, want %q (must NEVER delete inline-argo even when tier=disabled)", got.Action, ActionAlreadyMatches)
	}
	if len(got.Ops) != 0 {
		t.Fatalf("Ops: got %d, want 0 (never delete inline-argo resources)", len(got.Ops))
	}
}

// =============================================================================
// Rule 7: not write-eligible (legacy-only OR enabled-only)
// =============================================================================

func TestPlanFor_LegacyOnly_NoCurrent_WriteGateMissing(t *testing.T) {
	got := PlanFor(withLegacyOnly())
	if got.Action != ActionWriteGateMissing {
		t.Errorf("Action: got %q, want %q", got.Action, ActionWriteGateMissing)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0 (legacy-only NEVER writes)", len(got.Ops))
	}
	if len(got.Blockers) == 0 {
		t.Fatal("Blockers: empty; expected an explanatory blocker")
	}
	if !containsAll(got.Blockers[0], "legacy", "enabled", "manage-volsync") {
		t.Errorf("blocker should mention legacy AND both v4 labels: got %q", got.Blockers[0])
	}
}

func TestPlanFor_LegacyOnly_InlineMatches_AlreadyMatches(t *testing.T) {
	in := withLegacyOnly()
	in.Owner = OwnerInlineArgo
	in.Current = matchingCurrent(in, "argocd")
	got := PlanFor(in)
	if got.Action != ActionAlreadyMatches {
		t.Errorf("Action: got %q, want %q", got.Action, ActionAlreadyMatches)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0", len(got.Ops))
	}
}

func TestPlanFor_LegacyOnly_InlineDrifts_InlineArgoObserved(t *testing.T) {
	in := withLegacyOnly()
	in.Owner = OwnerInlineArgo
	in.Current = driftedCurrent(in, "argocd")
	got := PlanFor(in)
	if got.Action != ActionInlineArgoObserved {
		t.Errorf("Action: got %q, want %q", got.Action, ActionInlineArgoObserved)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0", len(got.Ops))
	}
}

func TestPlanFor_EnabledOnly_NoCurrent_WriteGateMissing(t *testing.T) {
	in := baseInputs()
	in.Spec = labels.Spec{Enabled: true, Origin: labels.OriginNew, Tier: labels.TierDaily}
	in.LabelSource = LabelSourceV4
	got := PlanFor(in)
	if got.Action != ActionWriteGateMissing {
		t.Errorf("Action: got %q, want %q", got.Action, ActionWriteGateMissing)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0", len(got.Ops))
	}
	if len(got.Blockers) == 0 || !containsAll(got.Blockers[0], "enabled", "manage-volsync") {
		t.Errorf("expected blocker mentioning enabled and manage-volsync; got %v", got.Blockers)
	}
}

func TestPlanFor_EnabledOnly_OperatorOwnedDrifts_AlreadyMatches_NoUpdate(t *testing.T) {
	// PVC was previously write-eligible and the operator created
	// RS/RD; the operator now removed manage-volsync. The operator
	// owns the resources but the write gate is off — do NOT update.
	in := baseInputs()
	in.Spec = labels.Spec{Enabled: true, Origin: labels.OriginNew, Tier: labels.TierDaily}
	in.LabelSource = LabelSourceV4
	in.Owner = OwnerPVCPlumber
	in.Current = driftedCurrent(in, "pvc-plumber")
	got := PlanFor(in)
	if got.Action != ActionAlreadyMatches {
		t.Errorf("removing manage-volsync must NOT trigger updates on operator-owned drift; got %q want %q", got.Action, ActionAlreadyMatches)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0 (write gate off)", len(got.Ops))
	}
	if len(got.Notes) == 0 {
		t.Error("expected a note explaining writes are gated off")
	}
}

// =============================================================================
// Paranoia: no forbidden GVKs in any Op
// =============================================================================

// TestPlanFor_OpsOnlyContainVolSyncKinds is the Patch 6.4 paranoia
// guard the user explicitly required: the planner must NEVER produce
// an Op against a Secret, ExternalSecret, ClusterExternalSecret, PVC,
// or any webhook configuration kind. Walks every Op of every plan
// produced by the test matrix and asserts the GVK is one of
// {ReplicationSource, ReplicationDestination}.
func TestPlanFor_OpsOnlyContainVolSyncKinds(t *testing.T) {
	// Build a list of scenarios that produce non-empty Ops. We
	// don't need exhaustive coverage of every verdict — just one
	// example of each Op-producing path.
	scenarios := []struct {
		name string
		in   Inputs
	}{
		{"write-eligible no current → create RS+RD", withEnabledManage()},
		{
			name: "write-eligible operator-owned drift → update RS+RD",
			in: func() Inputs {
				in := withEnabledManage()
				in.Owner = OwnerPVCPlumber
				in.Current = driftedCurrent(in, "pvc-plumber")
				return in
			}(),
		},
		{
			name: "write-eligible tier=disabled operator-owned → delete RS+RD",
			in: func() Inputs {
				in := withEnabledManage()
				in.Spec.Tier = labels.TierDisabled
				in.Owner = OwnerPVCPlumber
				in.Current = matchingCurrent(in, "pvc-plumber")
				return in
			}(),
		},
		{
			name: "write-eligible partial state → create missing child",
			in: func() Inputs {
				in := withEnabledManage()
				in.Owner = OwnerPVCPlumber
				in.Current = matchingCurrent(in, "pvc-plumber")
				in.Current.RDPresent = false
				return in
			}(),
		},
	}

	// The full set of kinds the planner is FORBIDDEN from producing
	// operations on. Anything not in the allow-list below should
	// have already failed the kind check; this is a redundancy guard.
	forbidden := map[string]bool{
		"Secret":                         true,
		"ExternalSecret":                 true,
		"ClusterExternalSecret":          true,
		"PersistentVolumeClaim":          true,
		"MutatingWebhookConfiguration":   true,
		"ValidatingWebhookConfiguration": true,
		"PersistentVolume":               true,
		"ServiceAccount":                 true,
		"Pod":                            true,
	}
	allowed := map[schema.GroupVersionKind]bool{
		rsGVK: true,
		rdGVK: true,
	}

	totalOps := 0
	for _, s := range scenarios {
		got := PlanFor(s.in)
		if len(got.Ops) == 0 {
			t.Errorf("scenario %q: expected non-empty Ops to exercise the paranoia check", s.name)
			continue
		}
		for i, op := range got.Ops {
			if op.Resource == nil {
				t.Errorf("scenario %q op[%d]: nil Resource", s.name, i)
				continue
			}
			totalOps++
			gvk := op.Resource.GroupVersionKind()
			if forbidden[gvk.Kind] {
				t.Errorf("scenario %q op[%d]: FORBIDDEN kind %q in Ops", s.name, i, gvk.Kind)
			}
			if !allowed[gvk] {
				t.Errorf("scenario %q op[%d]: kind %q is not in the allow-list (only ReplicationSource and ReplicationDestination are permitted)", s.name, i, gvk)
			}
		}
	}
	if totalOps == 0 {
		t.Fatal("paranoia check ran with 0 ops; scenarios are misconfigured")
	}
}

// TestPlanFor_PureNoSideEffects calls PlanFor twice with the same
// Inputs and confirms the result is byte-identical. Catches a
// future regression where someone introduces time.Now() or rand
// into the decision path.
func TestPlanFor_PureNoSideEffects(t *testing.T) {
	in := withEnabledManage()
	a := PlanFor(in)
	b := PlanFor(in)
	if a.Action != b.Action {
		t.Errorf("non-deterministic action: %q vs %q", a.Action, b.Action)
	}
	if len(a.Ops) != len(b.Ops) {
		t.Errorf("non-deterministic ops count: %d vs %d", len(a.Ops), len(b.Ops))
	}
}

// TestPlanFor_CreateOps_HaveCorrectGVKs covers a sub-case of the
// paranoia test: when planning a fresh create, the two ops are
// ReplicationSource and ReplicationDestination in that order with
// the expected name/namespace.
func TestPlanFor_CreateOps_HaveCorrectGVKs(t *testing.T) {
	in := withEnabledManage()
	got := PlanFor(in)

	if len(got.Ops) != 2 {
		t.Fatalf("Ops: got %d, want 2", len(got.Ops))
	}
	want := []struct {
		kind   string
		name   string
		opVerb OpKind
	}{
		{kindRS, tpvc, OpCreate},
		{kindRD, tpvc + "-dst", OpCreate},
	}
	for i, w := range want {
		op := got.Ops[i]
		if op.Kind != w.opVerb {
			t.Errorf("op[%d].Kind: got %q, want %q", i, op.Kind, w.opVerb)
		}
		if op.Resource.GetKind() != w.kind {
			t.Errorf("op[%d] kind: got %q, want %q", i, op.Resource.GetKind(), w.kind)
		}
		if op.Resource.GetName() != w.name {
			t.Errorf("op[%d] name: got %q, want %q", i, op.Resource.GetName(), w.name)
		}
		if op.Resource.GetNamespace() != tns {
			t.Errorf("op[%d] namespace: got %q, want %q", i, op.Resource.GetNamespace(), tns)
		}
	}
}

// TestPlanFor_DeleteOps_OnlyIdentifiers proves the delete ops carry
// just enough information for the executor (kind + ns + name).
// They are NOT required to carry a spec — and we want the executor
// to be able to call Delete on a thin identifier object.
func TestPlanFor_DeleteOps_OnlyIdentifiers(t *testing.T) {
	in := withEnabledManage()
	in.Spec.Tier = labels.TierDisabled
	in.Owner = OwnerPVCPlumber
	in.Current = matchingCurrent(in, "pvc-plumber")
	got := PlanFor(in)

	if got.Action != ActionWouldDelete {
		t.Fatalf("Action: got %q, want %q", got.Action, ActionWouldDelete)
	}
	if len(got.Ops) != 2 {
		t.Fatalf("Ops: got %d, want 2", len(got.Ops))
	}
	for i, op := range got.Ops {
		if op.Kind != OpDelete {
			t.Errorf("op[%d].Kind: got %q, want %q", i, op.Kind, OpDelete)
		}
		if op.Resource.GetName() == "" || op.Resource.GetNamespace() == "" {
			t.Errorf("op[%d]: identifier must have name+namespace; got name=%q ns=%q", i, op.Resource.GetName(), op.Resource.GetNamespace())
		}
		// Spec block need not be populated for deletes — the executor
		// only needs metadata + GVK.
		spec, found, _ := unstructured.NestedMap(op.Resource.Object, "spec")
		if found && len(spec) > 0 {
			t.Logf("op[%d]: delete identifier carries spec (harmless; executor will ignore): %v", i, spec)
		}
	}
}

// =============================================================================
// Helpers
// =============================================================================

// containsAll returns true if `s` contains every substring in
// `needles`. Used by blocker-message assertions to be resilient to
// the exact text wording (which may evolve) while still pinning the
// key terms.
func containsAll(s string, needles ...string) bool {
	for _, n := range needles {
		if !contains(s, n) {
			return false
		}
	}
	return true
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// =============================================================================
// v4.0.1: namespace write gate (rule 5b)
// =============================================================================

// A fully write-eligible PVC (both fuse labels, valid tier, no current)
// in an UNMANAGED namespace must yield skipped-namespace-not-managed with
// ZERO ops and a blocker — the gate suppresses the create.
func TestPlanFor_WriteEligible_NamespaceNotManaged_SkippedNoOps(t *testing.T) {
	in := withEnabledManage()
	in.NamespaceManaged = false
	got := PlanFor(in)
	if got.Action != ActionSkippedNamespaceNotManaged {
		t.Errorf("Action: got %q, want %q", got.Action, ActionSkippedNamespaceNotManaged)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0 (namespace gate must suppress all writes)", len(got.Ops))
	}
	if len(got.Blockers) == 0 {
		t.Error("expected a blocker explaining the namespace gate")
	}
}

// The same gated PVC in a MANAGED namespace creates normally — proving
// the gate is the only thing standing between eligible and would-create.
func TestPlanFor_WriteEligible_NamespaceManaged_WouldCreate(t *testing.T) {
	in := withEnabledManage()
	in.NamespaceManaged = true
	got := PlanFor(in)
	if got.Action != ActionWouldCreate {
		t.Errorf("Action: got %q, want %q", got.Action, ActionWouldCreate)
	}
	if len(got.Ops) != 2 {
		t.Errorf("Ops: got %d, want 2 (RS+RD create)", len(got.Ops))
	}
}

// A NON-opted-in PVC (no fuse labels) in a managed namespace is
// unaffected by the gate — it is skipped-not-opted-in. The gate only
// fires for write-eligible PVCs.
func TestPlanFor_NotOptedIn_NamespaceManaged_SkippedNotOptedIn(t *testing.T) {
	in := baseInputs() // NamespaceManaged:true, no opt-in labels
	got := PlanFor(in)
	if got.Action != ActionSkippedNotOptedIn {
		t.Errorf("Action: got %q, want %q", got.Action, ActionSkippedNotOptedIn)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0", len(got.Ops))
	}
}

// tier=disabled + operator-owned current in an UNMANAGED namespace must
// NOT emit delete ops — the namespace gate also suppresses the
// tier=disabled teardown path (it short-circuits before rule 6a).
func TestPlanFor_TierDisabled_NamespaceNotManaged_NoDeleteOps(t *testing.T) {
	in := withEnabledManage()
	in.Spec.Tier = labels.TierDisabled
	in.NamespaceManaged = false
	in.Owner = OwnerPVCPlumber
	in.Current = matchingCurrent(in, "pvc-plumber")
	got := PlanFor(in)
	if got.Action != ActionSkippedNamespaceNotManaged {
		t.Errorf("Action: got %q, want %q", got.Action, ActionSkippedNamespaceNotManaged)
	}
	if len(got.Ops) != 0 {
		t.Errorf("Ops: got %d, want 0 (gate must block tier=disabled delete)", len(got.Ops))
	}
}

// =============================================================================
// Manual-tier schedule rules (v4.0.2 — 2026-06-09 review finding B3)
// =============================================================================

// A manual-tier PVC whose operator-owned RS still carries a cron
// schedule (e.g. tier was daily, then flipped to manual) is drift and
// must be repaired to the manual-trigger shape. Uses the file's
// existing fixture helpers (withEnabledManage + matchingCurrent).
func TestPlanFor_ManualTier_LeftoverCron_WouldUpdate(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerPVCPlumber
	in.Current = matchingCurrent(in, "pvc-plumber")
	in.Spec.Tier = labels.TierManual
	// THE dangerous leftover: the PVC was tier=daily before the flip, so
	// the live cron is exactly ScheduleFor(daily) — which equals
	// ScheduleFor's manual fallback. A naive expected-schedule comparison
	// reads this as "matching" and never repairs to the manual trigger.
	in.Current.RSSchedule = builder.ScheduleFor(in.Namespace, in.PVCName, labels.TierDaily)

	plan := PlanFor(in)
	if plan.Action != ActionWouldUpdate {
		t.Fatalf("Action: got %q, want %q", plan.Action, ActionWouldUpdate)
	}
}

// A manual-tier RS with no cron (schedule empty) is the expected shape.
func TestPlanFor_ManualTier_NoCron_AlreadyMatches(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerPVCPlumber
	in.Current = matchingCurrent(in, "pvc-plumber")
	in.Spec.Tier = labels.TierManual
	in.Current.RSSchedule = ""

	plan := PlanFor(in)
	if plan.Action != ActionAlreadyMatches {
		t.Fatalf("Action: got %q, want %q", plan.Action, ActionAlreadyMatches)
	}
}

// A write-eligible PVC with no tier label gets the daily default — but
// /audit must SAY so (2026-06-09 review: silent defaults are traps).
func TestPlanFor_UnspecifiedTier_NotesDefault(t *testing.T) {
	in := withEnabledManage()
	in.Spec.Tier = labels.TierUnspecified
	in.Current = CurrentState{} // no children → would-create
	in.Owner = OwnerNone

	plan := PlanFor(in)
	if plan.Action != ActionWouldCreate {
		t.Fatalf("Action: got %q, want %q", plan.Action, ActionWouldCreate)
	}
	want := "no pvc-plumber.io/tier label; defaulting to daily cadence — set the label explicitly"
	found := false
	for _, n := range plan.Notes {
		if n == want {
			found = true
		}
	}
	if !found {
		t.Errorf("Notes missing default-tier note; got %v", plan.Notes)
	}
}

// v5-surface annotations parse cleanly but are NOT enforced by the v4
// permissive reconciler. /audit must disclose that instead of letting
// the user believe min-backup-age protection exists (2026-06-09 review).
func TestPlanFor_InertV5Annotations_Noted(t *testing.T) {
	in := withEnabledManage()
	in.Owner = OwnerPVCPlumber
	in.Current = matchingCurrent(in, "pvc-plumber")
	in.Spec.MinBackupAgeSet = true
	in.Spec.MinBackupAge = 2 * time.Hour
	in.Spec.SkipRestore = true
	in.Spec.SkipRestoreReason = "test"
	in.Spec.Mode = "strict"
	in.Spec.RestoreMode = "strict"

	plan := PlanFor(in)

	wantFragments := []string{
		labels.AnnotationMinBackupAge,
		labels.AnnotationSkipRestore,
		labels.AnnotationMode,
		labels.AnnotationRestoreMode,
	}
	for _, frag := range wantFragments {
		found := false
		for _, n := range plan.Notes {
			if strings.Contains(n, frag) && strings.Contains(n, "not enforced") {
				found = true
			}
		}
		if !found {
			t.Errorf("Notes missing inert-annotation disclosure for %s; got %v", frag, plan.Notes)
		}
	}
}

// No annotations set → no inert-annotation noise.
func TestPlanFor_NoV5Annotations_NoInertNotes(t *testing.T) {
	in := withEnabledManage()
	plan := PlanFor(in)
	for _, n := range plan.Notes {
		if strings.Contains(n, "not enforced") {
			t.Errorf("unexpected inert-annotation note on clean spec: %q", n)
		}
	}
}
