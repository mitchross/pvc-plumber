package controller

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	v4labels "github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/mode"
)

// rc7 — these tests cover the fix for the 2026-05-28 nginx-example/storage
// backup gap: before rc7 the controller watched PVCs only, so an Argo prune
// of the inline RS/RD fired no event, the operator never reconciled, and
// /audit served a stale "already-matches" snapshot for ~15h while the PVC
// had no backup chain. rc7 adds an RS/RD watch (mapChildToPVC), a periodic
// self-heal requeue, a partial inline-argo blocker, and /audit staleness
// surfacing.
//
// LAYER 1 (this file, fake-client / direct calls) proves the reverse-map
// LOGIC, the partial-state ownership policy, the resync result, and the
// staleness computation — everything that does not require a running
// manager. The end-to-end "a Delete event actually reaches mapChildToPVC
// through controller-runtime's informer" wiring is exercised by the
// envtest suite in v4_watch_envtest_test.go (build tag `envtest`), which
// runs in CI where apiserver assets + the VolSync CRDs are available.

// childRS builds an unstructured ReplicationSource with arbitrary labels
// and an optional spec.sourcePVC. Distinct from makeRS (which always sets
// managed-by + repository) so the reverse-map tests can model label-less
// Argo-owned inline children.
func childRS(ns, name string, lbls map[string]string, sourcePVC string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(rsGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	if lbls != nil {
		u.SetLabels(lbls)
	}
	if sourcePVC != "" {
		_ = unstructured.SetNestedField(u.Object, sourcePVC, "spec", "sourcePVC")
	}
	return u
}

// childRD builds an unstructured ReplicationDestination with arbitrary
// labels and an optional spec.kopia.sourceIdentity.sourcePVCName.
func childRD(ns, name string, lbls map[string]string, sourcePVCName string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(rdGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	if lbls != nil {
		u.SetLabels(lbls)
	}
	if sourcePVCName != "" {
		_ = unstructured.SetNestedField(u.Object, sourcePVCName, "spec", "kopia", "sourceIdentity", "sourcePVCName")
	}
	return u
}

// =============================================================================
// resolveSourcePVC — the pure reverse-map (no client)
// =============================================================================

func TestResolveSourcePVC(t *testing.T) {
	const ns, pvc = testNSMyapp, "data"

	cases := []struct {
		name    string
		obj     client.Object
		wantNS  string
		wantPVC string
	}{
		{
			name:    "RS with operator source labels (canonical)",
			obj:     childRS("child-ns", "anything", map[string]string{v4labels.LabelSourceNamespace: ns, v4labels.LabelSourcePVC: pvc}, "ignored"),
			wantNS:  ns,
			wantPVC: pvc,
		},
		{
			name:    "RD with operator source labels (canonical)",
			obj:     childRD("child-ns", "anything", map[string]string{v4labels.LabelSourceNamespace: ns, v4labels.LabelSourcePVC: pvc}, "ignored"),
			wantNS:  ns,
			wantPVC: pvc,
		},
		{
			name:    "Argo inline RS: no source labels, derive from spec.sourcePVC + own namespace",
			obj:     childRS(ns, pvc, map[string]string{managedByLabel: ManagedByArgoCDLabelValue}, pvc),
			wantNS:  ns,
			wantPVC: pvc,
		},
		{
			name:    "Argo inline RD: no source labels, derive from sourceIdentity.sourcePVCName",
			obj:     childRD(ns, pvc+"-dst", map[string]string{managedByLabel: ManagedByArgoCDLabelValue}, pvc),
			wantNS:  ns,
			wantPVC: pvc,
		},
		{
			name:    "RS via volsync.backup/pvc convention label only",
			obj:     childRS(ns, "weird-name", map[string]string{volsyncBackupPVCLabel: pvc}, ""),
			wantNS:  ns,
			wantPVC: pvc,
		},
		{
			name:    "RD tombstone: no labels, no spec, derive by stripping -dst",
			obj:     childRD(ns, pvc+"-dst", nil, ""),
			wantNS:  ns,
			wantPVC: pvc,
		},
		{
			name:    "RS tombstone: no labels, no spec, name == pvc under bare-dst",
			obj:     childRS(ns, pvc, nil, ""),
			wantNS:  ns,
			wantPVC: pvc,
		},
		{
			name:    "RD without -dst suffix and no labels/spec: unresolvable",
			obj:     childRD(ns, "no-suffix", nil, ""),
			wantNS:  "",
			wantPVC: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotNS, gotPVC := resolveSourcePVC(tc.obj)
			if gotNS != tc.wantNS || gotPVC != tc.wantPVC {
				t.Errorf("resolveSourcePVC = (%q, %q), want (%q, %q)", gotNS, gotPVC, tc.wantNS, tc.wantPVC)
			}
		})
	}
}

// =============================================================================
// childEventPredicate — delete always passes; status-only updates suppressed
// =============================================================================

func TestChildEventPredicate(t *testing.T) {
	p := childEventPredicate()

	rs := childRS(testNSMyapp, "data", nil, "data")
	if !p.Create(event.CreateEvent{Object: rs}) {
		t.Error("CreateFunc: child create must pass")
	}
	if !p.Delete(event.DeleteEvent{Object: rs}) {
		t.Error("DeleteFunc: child delete must pass (this is the rc7 fix)")
	}
	if p.Generic(event.GenericEvent{Object: rs}) {
		t.Error("GenericFunc: generic events must be dropped")
	}

	// Status-only update (generation unchanged) must be suppressed — this
	// is VolSync writing lastSyncTime/latestMoverStatus on every tick.
	oldObj := childRS(testNSMyapp, "data", nil, "data")
	oldObj.SetGeneration(3)
	statusOnly := childRS(testNSMyapp, "data", nil, "data")
	statusOnly.SetGeneration(3)
	if p.Update(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: statusOnly}) {
		t.Error("UpdateFunc: status-only update (generation unchanged) must be suppressed")
	}

	// Spec change (generation bumped) must pass.
	specChange := childRS(testNSMyapp, "data", nil, "data")
	specChange.SetGeneration(4)
	if !p.Update(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: specChange}) {
		t.Error("UpdateFunc: generation change must pass")
	}

	// Defensive: nil objects pass (let the mapper decide).
	if !p.Update(event.UpdateEvent{}) {
		t.Error("UpdateFunc: nil objects should pass through to the mapper")
	}
}

// =============================================================================
// mapChildToPVC — enqueue only when the resolved PVC exists
// =============================================================================

func TestV4MapChildToPVC_EnqueuesWhenPVCExists(t *testing.T) {
	pvc := makePVC(testNSMyapp, "rc7-map", labelsEnabledManage(), nil)
	f := newV4Fixture(t, pvc)

	// Argo-owned inline RS (no operator source labels) — the incident shape.
	rs := childRS(testNSMyapp, "rc7-map", map[string]string{managedByLabel: ManagedByArgoCDLabelValue}, "rc7-map")
	reqs := f.rec.mapChildToPVC(context.Background(), rs)
	if len(reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(reqs))
	}
	if reqs[0].Namespace != testNSMyapp || reqs[0].Name != "rc7-map" {
		t.Errorf("request = %s/%s, want %s/rc7-map", reqs[0].Namespace, reqs[0].Name, testNSMyapp)
	}
}

func TestV4MapChildToPVC_NoEnqueueWhenPVCMissing(t *testing.T) {
	f := newV4Fixture(t) // no PVC seeded

	rs := childRS("unrelated-ns", "whatever", nil, "whatever")
	if reqs := f.rec.mapChildToPVC(context.Background(), rs); len(reqs) != 0 {
		t.Fatalf("unrelated RS (no matching PVC) must not enqueue: got %+v", reqs)
	}
}

func TestV4MapChildToPVC_SkipsSystemNamespace(t *testing.T) {
	f := newV4Fixture(t)
	rs := childRS("kube-system", "x", map[string]string{volsyncBackupPVCLabel: "x"}, "x")
	if reqs := f.rec.mapChildToPVC(context.Background(), rs); len(reqs) != 0 {
		t.Fatalf("system-namespace child must not enqueue: got %+v", reqs)
	}
}

// =============================================================================
// Partial operator-owned state: recreate ONLY the missing child
// =============================================================================

func TestV4Reconcile_Rc7_ManagedRSMissing_RDPresent_RecreatesRS(t *testing.T) {
	pvc := makePVC(testNSMyapp, "rc7-rs-missing", labelsEnabledManage(), nil)
	rd := makeRD(testNSMyapp, "rc7-rs-missing-dst", ManagedByPVCPlumberLabelValue, testRepoSecretShare)
	f := newV4ModeFixture(t, mode.Permissive, pvc, rd)

	entry := f.reconcile(testNSMyapp, "rc7-rs-missing")

	if entry.Owner != OwnerPVCPlumber {
		t.Fatalf("Owner: got %q, want %q", entry.Owner, OwnerPVCPlumber)
	}
	if entry.Action != ActionWouldCreate {
		t.Fatalf("Action: got %q, want %q", entry.Action, ActionWouldCreate)
	}
	if !f.liveExists(rsGVK, testNSMyapp, "rc7-rs-missing") {
		t.Error("missing RS should have been recreated")
	}
	// Only the missing RS is created; the present RD is untouched.
	f.assertDidWriteByVerb(t, 1, 0, 0)
}

func TestV4Reconcile_Rc7_ManagedRDMissing_RSPresent_RecreatesRD(t *testing.T) {
	pvc := makePVC(testNSMyapp, "rc7-rd-missing", labelsEnabledManage(), nil)
	rs := makeRS(testNSMyapp, "rc7-rd-missing", ManagedByPVCPlumberLabelValue, testRepoSecretShare, "rc7-rd-missing")
	f := newV4ModeFixture(t, mode.Permissive, pvc, rs)

	entry := f.reconcile(testNSMyapp, "rc7-rd-missing")

	if entry.Action != ActionWouldCreate {
		t.Fatalf("Action: got %q, want %q", entry.Action, ActionWouldCreate)
	}
	if !f.liveExists(rdGVK, testNSMyapp, "rc7-rd-missing-dst") {
		t.Error("missing RD should have been recreated")
	}
	f.assertDidWriteByVerb(t, 1, 0, 0)
}

// =============================================================================
// Partial INLINE-ARGO state: never create a conflicting sibling
// =============================================================================

func TestV4Reconcile_Rc7_PartialInlineArgo_RDMissing_NeedsHumanReview(t *testing.T) {
	pvc := makePVC(testNSMyapp, "rc7-inline-partial-rd", labelsEnabledManage(), nil)
	// Inline RS owned by Argo present; RD pruned (the mixed half-state).
	rs := makeRS(testNSMyapp, "rc7-inline-partial-rd", ManagedByArgoCDLabelValue, testRepoSecretShare, "rc7-inline-partial-rd")
	f := newV4ModeFixture(t, mode.Permissive, pvc, rs)

	entry := f.reconcile(testNSMyapp, "rc7-inline-partial-rd")

	if entry.Owner != OwnerInlineArgo {
		t.Fatalf("Owner: got %q, want %q", entry.Owner, OwnerInlineArgo)
	}
	if entry.Action != ActionNeedsHumanReview {
		t.Fatalf("Action: got %q, want %q", entry.Action, ActionNeedsHumanReview)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0 (must not create a child conflicting with the surviving inline-argo RS)", len(entry.PlannedOps))
	}
	if len(entry.Blockers) == 0 {
		t.Error("expected a loud blocker for the partial inline-argo state")
	}
	f.assertDidWriteByVerb(t, 0, 0, 0)
}

func TestV4Reconcile_Rc7_PartialInlineArgo_RSMissing_NeedsHumanReview(t *testing.T) {
	pvc := makePVC(testNSMyapp, "rc7-inline-partial-rs", labelsEnabledManage(), nil)
	// Inline RD owned by Argo present; RS pruned.
	rd := makeRD(testNSMyapp, "rc7-inline-partial-rs-dst", ManagedByArgoCDLabelValue, testRepoSecretShare)
	f := newV4ModeFixture(t, mode.Permissive, pvc, rd)

	entry := f.reconcile(testNSMyapp, "rc7-inline-partial-rs")

	if entry.Owner != OwnerInlineArgo {
		t.Fatalf("Owner: got %q, want %q", entry.Owner, OwnerInlineArgo)
	}
	if entry.Action != ActionNeedsHumanReview {
		t.Fatalf("Action: got %q, want %q", entry.Action, ActionNeedsHumanReview)
	}
	f.assertDidWriteByVerb(t, 0, 0, 0)
}

// =============================================================================
// /audit freshness: the Store reflects a live child deletion on the next
// reconcile (the regression that produced the 15h stale /audit).
// =============================================================================

func TestV4Reconcile_Rc7_StoreReflectsLiveChildDeletion(t *testing.T) {
	pvc := makePVC(testNSMyapp, "rc7-stale", labelsEnabledManage(), nil)
	rs := makeRS(testNSMyapp, "rc7-stale", ManagedByPVCPlumberLabelValue, testRepoSecretShare, "rc7-stale")
	rd := makeRD(testNSMyapp, "rc7-stale-dst", ManagedByPVCPlumberLabelValue, testRepoSecretShare)
	// Audit mode so the executor does NOT recreate — isolates the Store's
	// reflection of live state from any executor write.
	f := newV4ModeFixture(t, mode.Audit, pvc, rs, rd)

	entry := f.reconcile(testNSMyapp, "rc7-stale")
	if !entry.Current.RSPresent || !entry.Current.RDPresent {
		t.Fatalf("precondition: both children observed present; got RS=%v RD=%v", entry.Current.RSPresent, entry.Current.RDPresent)
	}
	if entry.Action != ActionAlreadyMatches {
		t.Fatalf("precondition Action: got %q, want %q", entry.Action, ActionAlreadyMatches)
	}

	// Prune both children WITHOUT touching the PVC — exactly what Argo did.
	if err := f.fake.Delete(context.Background(), rs); err != nil {
		t.Fatalf("delete RS: %v", err)
	}
	if err := f.fake.Delete(context.Background(), rd); err != nil {
		t.Fatalf("delete RD: %v", err)
	}

	// In production the rc7 watch fires this reconcile automatically off the
	// Delete event; here we drive it directly to assert the Store's content.
	entry2 := f.reconcile(testNSMyapp, "rc7-stale")
	if entry2.Current.RSPresent || entry2.Current.RDPresent {
		t.Errorf("Store must reflect live deletion: got RS=%v RD=%v, want both false", entry2.Current.RSPresent, entry2.Current.RDPresent)
	}
	if entry2.Action != ActionWouldCreate {
		t.Errorf("after child deletion Action: got %q, want %q", entry2.Action, ActionWouldCreate)
	}
}

// =============================================================================
// Periodic self-heal requeue
// =============================================================================

func TestV4Reconcile_Rc7_ResyncRequeueForGatedPVC(t *testing.T) {
	pvc := makePVC(testNSMyapp, "rc7-resync", labelsEnabledManage(), nil)
	f := newV4ModeFixture(t, mode.Permissive, pvc)
	f.rec.ResyncInterval = 7 * time.Minute

	res, err := f.rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNSMyapp, Name: "rc7-resync"},
	})
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 7*time.Minute {
		t.Errorf("RequeueAfter: got %v, want 7m for write-eligible PVC", res.RequeueAfter)
	}
}

func TestV4Reconcile_Rc7_NoResyncForNonGatedPVC(t *testing.T) {
	// Legacy-only label: reportable but NOT write-eligible → no requeue.
	pvc := makePVC(testNSMyapp, "rc7-legacy", map[string]string{backupLabelKey: backupDaily}, nil)
	f := newV4Fixture(t, pvc)
	f.rec.ResyncInterval = 7 * time.Minute

	res, err := f.rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNSMyapp, Name: "rc7-legacy"},
	})
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter: got %v, want 0 (non-write-eligible PVC must not periodically requeue)", res.RequeueAfter)
	}
}

// =============================================================================
// /audit staleness surfacing (Store.Snapshot)
// =============================================================================

func staleStore(t *testing.T) *Store {
	t.Helper()
	s := NewStore("permissive", "bare-dst", testRepoSecretShare)
	s.now = fixedTime
	return s
}

func TestStore_Snapshot_FlagsStaleEntryBeyondMaxAge(t *testing.T) {
	s := staleStore(t)
	s.SetMaxAge(15 * time.Minute)
	s.Set(ParityEntry{
		Namespace: testNSMyapp, PVC: "stale-pvc",
		Action: ActionAlreadyMatches, Owner: OwnerInlineArgo, LabelSource: LabelSourceV4,
		EvaluatedAt: fixedTime().Add(-time.Hour), // 1h old → exceeds 15m
	})

	rep := s.Snapshot()
	if len(rep.Entries) != 1 {
		t.Fatalf("entries: got %d, want 1", len(rep.Entries))
	}
	e := rep.Entries[0]
	if !e.Stale {
		t.Error("entry should be Stale (age 1h > maxAge 15m)")
	}
	if e.AgeSeconds != 3600 {
		t.Errorf("AgeSeconds: got %d, want 3600", e.AgeSeconds)
	}
	if rep.Summary.EntriesStale != 1 {
		t.Errorf("Summary.EntriesStale: got %d, want 1", rep.Summary.EntriesStale)
	}
	if !rep.Summary.OldestEvaluatedAt.Equal(fixedTime().Add(-time.Hour)) {
		t.Errorf("Summary.OldestEvaluatedAt: got %v, want %v", rep.Summary.OldestEvaluatedAt, fixedTime().Add(-time.Hour))
	}
}

func TestStore_Snapshot_FreshEntryNotStale(t *testing.T) {
	s := staleStore(t)
	s.SetMaxAge(15 * time.Minute)
	s.Set(ParityEntry{
		Namespace: testNSMyapp, PVC: "fresh-pvc",
		Action: ActionAlreadyMatches, Owner: OwnerPVCPlumber, LabelSource: LabelSourceV4,
		EvaluatedAt: fixedTime(), // age 0
	})

	e := s.Snapshot().Entries[0]
	if e.Stale {
		t.Error("fresh entry must not be Stale")
	}
	if e.AgeSeconds != 0 {
		t.Errorf("AgeSeconds: got %d, want 0", e.AgeSeconds)
	}
}

func TestStore_Snapshot_MaxAgeZeroReportsAgeButNeverStale(t *testing.T) {
	s := staleStore(t) // maxAge unset (0)
	s.Set(ParityEntry{
		Namespace: testNSMyapp, PVC: "old-pvc",
		Action: ActionAlreadyMatches, Owner: OwnerInlineArgo, LabelSource: LabelSourceV4,
		EvaluatedAt: fixedTime().Add(-time.Hour),
	})

	rep := s.Snapshot()
	e := rep.Entries[0]
	if e.Stale {
		t.Error("maxAge=0 must never flag Stale")
	}
	if e.AgeSeconds != 3600 {
		t.Errorf("AgeSeconds still computed without maxAge: got %d, want 3600", e.AgeSeconds)
	}
	if rep.Summary.EntriesStale != 0 {
		t.Errorf("Summary.EntriesStale: got %d, want 0", rep.Summary.EntriesStale)
	}
}
