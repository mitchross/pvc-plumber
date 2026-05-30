package controller

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

// fixedTime returns a deterministic time.Now substitute. Tests inject
// this via store.now so EvaluatedAt / GeneratedAt fields don't drift.
func fixedTime() time.Time {
	return time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
}

// Test-scope constants. backupDaily / backupHourly already exist in the
// package (pvc_controller.go) and goconst suggests reusing them; we do
// the same for new fixtures unique to the report tests.
const (
	testModeAudit       = "audit"
	testRepoSecretShare = "volsync-kopia-repository"
	testNSOpenWebUI     = "open-webui"
	testNSMyapp         = "myapp"
	testPVCStorageName  = "storage"
	testPVCLibrary      = "library"
	testIdentityImmich  = "immich-library"
)

func TestActionKindValues(t *testing.T) {
	want := map[ActionKind]string{
		ActionAlreadyMatches:     "already-matches",
		ActionWouldCreate:        "would-create",
		ActionWouldUpdate:        "would-update",
		ActionWouldAdopt:         "would-adopt",
		ActionWouldDelete:        "would-delete",
		ActionInlineArgoObserved: "inline-argo-observed",
		ActionSkippedExempt:      "skipped-exempt",
		ActionSkippedNotOptedIn:  "skipped-not-opted-in",
		ActionNeedsHumanReview:   "needs-human-review",
		ActionWriteGateMissing:   actionWriteGateMissingStr,

		ActionSkippedNamespaceNotManaged: "skipped-namespace-not-managed",
	}
	for k, s := range want {
		if string(k) != s {
			t.Errorf("ActionKind: got %q, want %q", string(k), s)
		}
	}

	// AllActionKinds must cover every defined constant exactly once.
	seen := map[ActionKind]bool{}
	for _, k := range AllActionKinds() {
		if seen[k] {
			t.Errorf("AllActionKinds: duplicate entry for %q", k)
		}
		seen[k] = true
	}
	if len(seen) != len(want) {
		t.Errorf("AllActionKinds returned %d items, expected %d", len(seen), len(want))
	}
}

// actionWriteGateMissingStr is the canonical wire string for
// ActionWriteGateMissing. Defined once at test scope so the
// TestActionKind_WriteGateMissing_String guard, the enum-coverage
// test, and the round-trip test all reference the same literal — a
// drift between the constant and the wire-format JSON would otherwise
// be a silent contract break.
const actionWriteGateMissingStr = "write-gate-missing"

// TestActionKind_WriteGateMissing_String pins the exact JSON wire
// string for the Patch 6.2 verdict. The name is the dashed,
// lower-case form `write-gate-missing` — NOT snake_case, NOT
// camelCase. Operators reading the /audit JSON depend on this
// literal, and dashboards / Prometheus joins / Slack alerts will
// pin against it once the planner starts emitting it (Patch 6.4).
func TestActionKind_WriteGateMissing_String(t *testing.T) {
	if got := string(ActionWriteGateMissing); got != actionWriteGateMissingStr {
		t.Errorf("ActionWriteGateMissing string: got %q, want %q", got, actionWriteGateMissingStr)
	}
}

// TestAllActionKinds_IncludesWriteGateMissingOnce proves the new
// ActionKind appears in AllActionKinds() — without it, the v4
// audit Snapshot would not seed the zero-count bucket for the
// new verdict and JSON consumers would see the taxonomy regress
// when no PVC currently lands in the bucket. The "exactly once"
// guard catches accidental double-insert in future refactors of
// AllActionKinds.
func TestAllActionKinds_IncludesWriteGateMissingOnce(t *testing.T) {
	count := 0
	for _, k := range AllActionKinds() {
		if k == ActionWriteGateMissing {
			count++
		}
	}
	if count != 1 {
		t.Errorf("AllActionKinds() entries for ActionWriteGateMissing: got %d, want 1", count)
	}
}

// TestStoreSnapshot_SummaryIncludesWriteGateMissingBucket proves
// that an empty Store.Snapshot() still surfaces the zero bucket for
// the new verdict. Full-taxonomy coverage is part of the /audit
// contract: operators iterating the JSON should always see every
// defined ActionKind, even when no PVC currently lands in it.
func TestStoreSnapshot_SummaryIncludesWriteGateMissingBucket(t *testing.T) {
	s := NewStore(testModeAudit, "bare-dst", testRepoSecretShare)
	s.now = fixedTime
	rep := s.Snapshot()

	got, ok := rep.Summary.ByAction[ActionWriteGateMissing]
	if !ok {
		t.Fatalf("Snapshot.Summary.ByAction missing bucket %q", actionWriteGateMissingStr)
	}
	if got != 0 {
		t.Errorf("Snapshot.Summary.ByAction[%q]: got %d, want 0 (empty Store)", actionWriteGateMissingStr, got)
	}
}

// TestParityEntry_JSONRoundTrip_WriteGateMissing proves the new
// verdict survives a json.Marshal → json.Unmarshal round trip
// without losing its wire representation. Belt-and-braces on top
// of TestActionKind_WriteGateMissing_String: if a future
// MarshalJSON / UnmarshalJSON hook on ActionKind ever shows up,
// this test ensures the wire format stays "write-gate-missing"
// rather than the Go identifier.
func TestParityEntry_JSONRoundTrip_WriteGateMissing(t *testing.T) {
	original := ParityEntry{
		Namespace:   testNSMyapp,
		PVC:         testPVCName,
		Mode:        testModeAudit,
		Tier:        backupHourly,
		LabelSource: LabelSourceV4,
		Owner:       OwnerNone,
		Action:      ActionWriteGateMissing,
		Blockers:    []string{"missing pvc-plumber.io/manage-volsync label"},
	}

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Wire string check before decoding — catches any future
	// MarshalJSON drift that would silently rename the verdict.
	if !strings.Contains(string(raw), `"action":"`+actionWriteGateMissingStr+`"`) {
		t.Errorf("marshaled JSON missing %q in action field; body=%s", actionWriteGateMissingStr, string(raw))
	}

	var decoded ParityEntry
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Action != ActionWriteGateMissing {
		t.Errorf("Action after round-trip: got %q, want %q", decoded.Action, ActionWriteGateMissing)
	}
	if len(decoded.Blockers) != 1 || decoded.Blockers[0] != "missing pvc-plumber.io/manage-volsync label" {
		t.Errorf("Blockers after round-trip: got %v", decoded.Blockers)
	}
}

func TestOwnerClassificationValues(t *testing.T) {
	want := map[OwnerClassification]string{
		OwnerNone:                      "none",
		OwnerPVCPlumber:                "managed-by-pvc-plumber",
		OwnerInlineArgo:                "inline-argo",
		OwnerUnmanagedOrGitopsObserved: "unmanaged-or-gitops-observed",
		OwnerUnknown:                   "unknown",
	}
	for k, s := range want {
		if string(k) != s {
			t.Errorf("OwnerClassification: got %q, want %q", string(k), s)
		}
	}
	if got, expected := len(AllOwnerClassifications()), len(want); got != expected {
		t.Errorf("AllOwnerClassifications: got %d items, want %d", got, expected)
	}
}

func TestLabelSourceValues(t *testing.T) {
	want := map[LabelSource]string{
		LabelSourceNone:   "none",
		LabelSourceV4:     "v4",
		LabelSourceLegacy: "legacy",
		LabelSourceBoth:   "both",
	}
	for k, s := range want {
		if string(k) != s {
			t.Errorf("LabelSource: got %q, want %q", string(k), s)
		}
	}
	if got, expected := len(AllLabelSources()), len(want); got != expected {
		t.Errorf("AllLabelSources: got %d items, want %d", got, expected)
	}
}

func TestStore_BasicSetGetDeleteLen(t *testing.T) {
	s := NewStore(testModeAudit, "bare-dst", testRepoSecretShare)
	s.now = fixedTime

	if got := s.Len(); got != 0 {
		t.Errorf("empty Len: got %d, want 0", got)
	}

	e := ParityEntry{
		Namespace:   testNSMyapp,
		PVC:         testPVCName,
		Mode:        testModeAudit,
		Tier:        backupDaily,
		LabelSource: LabelSourceV4,
		Action:      ActionAlreadyMatches,
		Owner:       OwnerInlineArgo,
	}
	s.Set(e)

	if got := s.Len(); got != 1 {
		t.Errorf("Len after Set: got %d, want 1", got)
	}

	got, ok := s.Get(testNSMyapp, "data")
	if !ok {
		t.Fatalf("Get returned ok=false for present entry")
	}
	if got.Key() != "myapp/data" {
		t.Errorf("Key: got %q, want %q", got.Key(), "myapp/data")
	}
	if got.EvaluatedAt != fixedTime() {
		t.Errorf("EvaluatedAt: got %v, want %v (should have been set to s.now())", got.EvaluatedAt, fixedTime())
	}

	// Replace
	e.Tier = backupHourly
	s.Set(e)
	got, _ = s.Get(testNSMyapp, "data")
	if got.Tier != backupHourly {
		t.Errorf("Set replace: tier=%q, want %q", got.Tier, "hourly")
	}
	if got := s.Len(); got != 1 {
		t.Errorf("Len after replace: got %d, want 1", got)
	}

	s.Delete(testNSMyapp, "data")
	if _, ok := s.Get(testNSMyapp, "data"); ok {
		t.Errorf("Get after Delete: still present")
	}
	if got := s.Len(); got != 0 {
		t.Errorf("Len after Delete: got %d, want 0", got)
	}

	// Delete absent → no error / no panic
	s.Delete(testNSMyapp, "nonexistent")
}

func TestStore_PreservesExplicitEvaluatedAt(t *testing.T) {
	// When the caller sets EvaluatedAt explicitly, Set must NOT overwrite it.
	s := NewStore(testModeAudit, "bare-dst", testRepoSecretShare)
	s.now = func() time.Time { return time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC) }

	explicit := time.Date(2026, 5, 22, 18, 30, 0, 0, time.UTC)
	s.Set(ParityEntry{Namespace: "x", PVC: "y", EvaluatedAt: explicit})

	got, _ := s.Get("x", "y")
	if !got.EvaluatedAt.Equal(explicit) {
		t.Errorf("EvaluatedAt overwritten: got %v, want %v", got.EvaluatedAt, explicit)
	}
}

func TestSnapshot_Isolation(t *testing.T) {
	// Mutating the returned report MUST NOT affect the underlying store.
	s := NewStore(testModeAudit, "bare-dst", testRepoSecretShare)
	s.now = fixedTime

	s.Set(ParityEntry{Namespace: "a", PVC: "p1", Action: ActionAlreadyMatches, Owner: OwnerInlineArgo, LabelSource: LabelSourceV4})
	s.Set(ParityEntry{Namespace: "b", PVC: "p2", Action: ActionWouldCreate, Owner: OwnerNone, LabelSource: LabelSourceLegacy})

	rep := s.Snapshot()
	if len(rep.Entries) != 2 {
		t.Fatalf("snapshot entries: got %d, want 2", len(rep.Entries))
	}

	// Mutate the slice + nested maps in the returned report.
	rep.Entries[0].Action = ActionWouldDelete
	rep.Summary.ByAction[ActionAlreadyMatches] = 999
	rep.Summary.ByOwner[OwnerInlineArgo] = 999

	// The store must be unchanged.
	again := s.Snapshot()
	if again.Entries[0].Action != ActionAlreadyMatches {
		t.Errorf("snapshot mutation leaked back into store: Action=%q", again.Entries[0].Action)
	}
	if again.Summary.ByAction[ActionAlreadyMatches] != 1 {
		t.Errorf("summary mutation leaked back: ByAction[already-matches]=%d, want 1", again.Summary.ByAction[ActionAlreadyMatches])
	}
}

func TestSnapshot_DeterministicOrder(t *testing.T) {
	s := NewStore(testModeAudit, "bare-dst", testRepoSecretShare)
	s.now = fixedTime

	// Insert in mixed order; snapshot must return sorted by (ns, pvc).
	s.Set(ParityEntry{Namespace: "zeta", PVC: "z1"})
	s.Set(ParityEntry{Namespace: "alpha", PVC: "p2"})
	s.Set(ParityEntry{Namespace: "alpha", PVC: "p1"})
	s.Set(ParityEntry{Namespace: "beta", PVC: "p1"})

	rep := s.Snapshot()
	want := []string{"alpha/p1", "alpha/p2", "beta/p1", "zeta/z1"}
	for i, e := range rep.Entries {
		if e.Key() != want[i] {
			t.Errorf("entry %d: got %q, want %q (full order: %v)", i, e.Key(), want[i], extractKeys(rep.Entries))
		}
	}
}

func TestSnapshot_SummaryCountsAllBuckets(t *testing.T) {
	// Snapshot summary must include every defined ActionKind /
	// OwnerClassification / LabelSource bucket with a zero count when
	// no entries land in it. This guarantees JSON consumers always see
	// the full taxonomy.
	s := NewStore(testModeAudit, "bare-dst", testRepoSecretShare)
	s.now = fixedTime

	s.Set(ParityEntry{Namespace: "a", PVC: "p1", Action: ActionAlreadyMatches, Owner: OwnerInlineArgo, LabelSource: LabelSourceV4})
	rep := s.Snapshot()

	if rep.Summary.TotalPVCs != 1 {
		t.Errorf("TotalPVCs: got %d, want 1", rep.Summary.TotalPVCs)
	}
	if got := rep.Summary.ByAction[ActionAlreadyMatches]; got != 1 {
		t.Errorf("ByAction[already-matches]: got %d, want 1", got)
	}
	// Every other action must be present with a zero count.
	for _, k := range AllActionKinds() {
		if _, ok := rep.Summary.ByAction[k]; !ok {
			t.Errorf("ByAction missing bucket %q", k)
		}
	}
	for _, k := range AllOwnerClassifications() {
		if _, ok := rep.Summary.ByOwner[k]; !ok {
			t.Errorf("ByOwner missing bucket %q", k)
		}
	}
	for _, k := range AllLabelSources() {
		if _, ok := rep.Summary.BySource[k]; !ok {
			t.Errorf("BySource missing bucket %q", k)
		}
	}
}

func TestSnapshot_Metadata(t *testing.T) {
	s := NewStore(testModeAudit, "bare-dst", testRepoSecretShare)
	s.now = fixedTime
	rep := s.Snapshot()

	if rep.OperatorMode != testModeAudit {
		t.Errorf("OperatorMode: got %q, want audit", rep.OperatorMode)
	}
	if rep.NamingStrategy != "bare-dst" {
		t.Errorf("NamingStrategy: got %q, want bare-dst", rep.NamingStrategy)
	}
	if rep.DefaultRepoSecret != testRepoSecretShare {
		t.Errorf("DefaultRepoSecret: got %q", rep.DefaultRepoSecret)
	}
	if !rep.GeneratedAt.Equal(fixedTime()) {
		t.Errorf("GeneratedAt: got %v, want %v", rep.GeneratedAt, fixedTime())
	}
}

func TestParityEntry_JSONRoundTrip(t *testing.T) {
	original := ParityEntry{
		Namespace:      testNSOpenWebUI,
		PVC:            testPVCStorageName,
		Mode:           testModeAudit,
		Tier:           backupDaily,
		LabelSource:    LabelSourceLegacy,
		BackupIdentity: testNSOpenWebUI + "/" + testPVCStorageName,
		Expected: ExpectedState{
			RSName:           testPVCStorageName,
			RDName:           testPVCStorageName + "-dst",
			RepositorySecret: testRepoSecretShare,
			KopiaUsername:    testPVCStorageName,
			KopiaHostname:    testNSOpenWebUI,
			BackupIdentity:   testNSOpenWebUI + "/" + testPVCStorageName,
		},
		Current: CurrentState{
			RSPresent:    true,
			RSName:       testPVCStorageName,
			RSManagedBy:  ManagedByArgoCDLabelValue,
			RSRepository: testRepoSecretShare,
			RSSourcePVC:  testPVCStorageName,
			RDPresent:    true,
			RDName:       testPVCStorageName + "-dst",
			RDManagedBy:  ManagedByArgoCDLabelValue,
			RDRepository: testRepoSecretShare,
		},
		Owner:       OwnerInlineArgo,
		Action:      ActionAlreadyMatches,
		Blockers:    nil,
		ReasonCode:  "AllowedRestoreInjected",
		EvaluatedAt: fixedTime(),
	}

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got ParityEntry
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.Namespace != original.Namespace || got.PVC != original.PVC {
		t.Errorf("ns/pvc round-trip diff: %+v vs %+v", got, original)
	}
	if got.LabelSource != original.LabelSource {
		t.Errorf("LabelSource: got %q, want %q", got.LabelSource, original.LabelSource)
	}
	if got.Owner != original.Owner {
		t.Errorf("Owner: got %q, want %q", got.Owner, original.Owner)
	}
	if got.Action != original.Action {
		t.Errorf("Action: got %q, want %q", got.Action, original.Action)
	}
	if got.Expected != original.Expected {
		t.Errorf("Expected diff: got %+v, want %+v", got.Expected, original.Expected)
	}
	if got.Current != original.Current {
		t.Errorf("Current diff: got %+v, want %+v", got.Current, original.Current)
	}
	if !got.EvaluatedAt.Equal(original.EvaluatedAt) {
		t.Errorf("EvaluatedAt: got %v, want %v", got.EvaluatedAt, original.EvaluatedAt)
	}
}

func TestParityReport_JSONShape(t *testing.T) {
	s := NewStore(testModeAudit, "bare-dst", testRepoSecretShare)
	s.now = fixedTime
	s.Set(ParityEntry{
		Namespace: "open-webui", PVC: testPVCStorageName,
		Mode: testModeAudit, Tier: backupDaily,
		LabelSource: LabelSourceLegacy,
		Action:      ActionAlreadyMatches,
		Owner:       OwnerInlineArgo,
	})
	s.Set(ParityEntry{
		Namespace: "comfyui", PVC: "comfyui-storage",
		Mode: testModeAudit, Tier: "",
		LabelSource: LabelSourceNone,
		Action:      ActionSkippedExempt,
		Owner:       OwnerNone,
	})

	rep := s.Snapshot()
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	s2 := string(raw)
	// Spot-check that the top-level keys all appear in the JSON.
	for _, key := range []string{
		`"generated_at"`,
		`"operator_mode"`, `"` + testModeAudit + `"`,
		`"naming_strategy"`, `"bare-dst"`,
		`"default_repo_secret"`, `"` + testRepoSecretShare + `"`,
		`"summary"`, `"total_pvcs": 2`,
		`"by_action"`, `"already-matches": 1`, `"skipped-exempt": 1`,
		`"by_owner_classification"`, `"inline-argo": 1`,
		`"by_label_source"`, `"legacy": 1`, `"none": 1`,
		`"entries"`,
	} {
		if !strings.Contains(s2, key) {
			t.Errorf("JSON missing key %q\nFull output:\n%s", key, s2)
		}
	}
}

func TestParityReport_RoundTrip(t *testing.T) {
	s := NewStore(testModeAudit, "bare-dst", testRepoSecretShare)
	s.now = fixedTime
	s.Set(ParityEntry{
		Namespace: "x", PVC: "y",
		LabelSource: LabelSourceV4, Action: ActionWouldCreate,
		Owner: OwnerNone, EvaluatedAt: fixedTime(),
	})
	orig := s.Snapshot()
	raw, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var back ParityReport
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if back.OperatorMode != orig.OperatorMode {
		t.Errorf("OperatorMode: got %q, want %q", back.OperatorMode, orig.OperatorMode)
	}
	if len(back.Entries) != len(orig.Entries) {
		t.Errorf("entry count: got %d, want %d", len(back.Entries), len(orig.Entries))
	}
	if back.Summary.TotalPVCs != orig.Summary.TotalPVCs {
		t.Errorf("TotalPVCs: got %d, want %d", back.Summary.TotalPVCs, orig.Summary.TotalPVCs)
	}
	// Map equality on summary buckets
	for k, v := range orig.Summary.ByAction {
		if back.Summary.ByAction[k] != v {
			t.Errorf("ByAction[%q]: got %d, want %d", k, back.Summary.ByAction[k], v)
		}
	}
}

func TestStore_ConcurrentSet(t *testing.T) {
	// 100 goroutines × 100 Sets each; the store must end with the
	// expected count and no race.
	s := NewStore(testModeAudit, "bare-dst", testRepoSecretShare)
	s.now = fixedTime

	const goroutines = 100
	const perGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				e := ParityEntry{
					Namespace: nsName(g),
					PVC:       pvcName(i),
					Action:    ActionAlreadyMatches,
					Owner:     OwnerInlineArgo,
				}
				s.Set(e)
				_, _ = s.Get(e.Namespace, e.PVC)
			}
		}(g)
	}
	wg.Wait()

	if got := s.Len(); got != goroutines*perGoroutine {
		t.Errorf("after concurrent Set: Len()=%d, want %d", got, goroutines*perGoroutine)
	}
	// Snapshot under concurrent reads shouldn't crash.
	for i := 0; i < 10; i++ {
		go func() { _ = s.Snapshot() }()
	}
	rep := s.Snapshot()
	if rep.Summary.TotalPVCs != goroutines*perGoroutine {
		t.Errorf("snapshot TotalPVCs=%d, want %d", rep.Summary.TotalPVCs, goroutines*perGoroutine)
	}
}

func TestStore_ConcurrentSetDeleteGet(t *testing.T) {
	// Mixed read/write workload under -race.
	s := NewStore(testModeAudit, "bare-dst", testRepoSecretShare)
	s.now = fixedTime

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(3)
	go func() { // writer
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.Set(ParityEntry{Namespace: "ns", PVC: pvcName(i), Action: ActionAlreadyMatches})
			i++
		}
	}()
	go func() { // deleter
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.Delete("ns", pvcName(i))
			i++
		}
	}()
	go func() { // reader
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = s.Snapshot()
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// --- helpers ---

func extractKeys(es []ParityEntry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.Key()
	}
	return out
}

func nsName(g int) string  { return "ns-" + itoa(g) }
func pvcName(i int) string { return "pvc-" + itoa(i) }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	bp := len(b)
	for n > 0 {
		bp--
		b[bp] = byte('0' + n%10)
		n /= 10
	}
	return string(b[bp:])
}
