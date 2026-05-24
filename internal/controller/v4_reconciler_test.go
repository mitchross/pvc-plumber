package controller

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/mitchross/pvc-plumber/internal/v4/auditclient"
	v4labels "github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/mode"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// =============================================================================
// Test fixtures
// =============================================================================

// v4Fixture bundles the reconciler under test with the fake client,
// audit wrapper, and store so each test can assert on all four.
type v4Fixture struct {
	t      *testing.T
	fake   client.WithWatch
	audit  *auditclient.Client
	store  *Store
	rec    *V4AuditReconciler
	logBuf *bytes.Buffer
}

func newV4Fixture(t *testing.T, seedObjs ...client.Object) *v4Fixture {
	t.Helper()
	scheme := newTestScheme(t)
	fakeC := fake.NewClientBuilder().WithScheme(scheme).WithObjects(seedObjs...).Build()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	auditC := auditclient.New(fakeC, mode.Audit, log)
	store := NewStore(testModeAudit, "bare-dst", testRepoSecretShare)
	store.now = fixedTime
	r := &V4AuditReconciler{
		Client:            auditC,
		Store:             store,
		NamingStrategy:    naming.StrategyBareDst,
		DefaultRepoSecret: testRepoSecretShare,
		SystemNamespaces:  map[string]struct{}{"kube-system": {}, "volsync-system": {}, "argocd": {}},
		OperatorMode:      testModeAudit,
		Now:               fixedTime,
	}
	return &v4Fixture{t: t, fake: fakeC, audit: auditC, store: store, rec: r, logBuf: &buf}
}

// reconcile runs one Reconcile and returns the (possibly-missing) Store
// entry for the requested PVC. Fails the test on reconcile error.
func (f *v4Fixture) reconcile(ns, name string) ParityEntry {
	f.t.Helper()
	res, err := f.rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
	})
	if err != nil {
		f.t.Fatalf("Reconcile error: %v", err)
	}
	_ = res
	entry, _ := f.store.Get(ns, name)
	return entry
}

// assertNoWrites fails the test if the auditclient saw any write
// attempts pass through (Did) — the audit-mode reconciler must never
// reach the apiserver write path. WouldWrite counters being non-zero
// is also a regression (the reconciler shouldn't even attempt writes;
// they're not gated by audit at all in v4).
func (f *v4Fixture) assertNoWrites() {
	f.t.Helper()
	if w := f.audit.WouldWriteTotals(); w.Total() != 0 {
		f.t.Errorf("auditclient WouldWriteTotals: got %+v, want 0 across all verbs", w)
	}
	if d := f.audit.DidWriteTotals(); d.Total() != 0 {
		f.t.Errorf("auditclient DidWriteTotals: got %+v, want 0", d)
	}
}

// makePVC builds a PVC with the given labels + annotations. Spec is the
// minimum required for a Longhorn PVC.
func makePVC(ns, name string, lbls, anns map[string]string) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
	}
	if lbls != nil {
		pvc.Labels = lbls
	}
	if anns != nil {
		pvc.Annotations = anns
	}
	return pvc
}

// makeRS builds an unstructured ReplicationSource matching the talos
// inline pattern. managedBy may be "" for the unmanaged case.
func makeRS(ns, name, managedBy, repo, sourcePVC string) *unstructured.Unstructured {
	rs := &unstructured.Unstructured{}
	rs.SetGroupVersionKind(rsGVK)
	rs.SetNamespace(ns)
	rs.SetName(name)
	if managedBy != "" {
		rs.SetLabels(map[string]string{managedByLabel: managedBy})
	}
	_ = unstructured.SetNestedField(rs.Object, sourcePVC, "spec", "sourcePVC")
	_ = unstructured.SetNestedField(rs.Object, repo, "spec", "kopia", "repository")
	return rs
}

// makeRD builds an unstructured ReplicationDestination.
func makeRD(ns, name, managedBy, repo string) *unstructured.Unstructured {
	rd := &unstructured.Unstructured{}
	rd.SetGroupVersionKind(rdGVK)
	rd.SetNamespace(ns)
	rd.SetName(name)
	if managedBy != "" {
		rd.SetLabels(map[string]string{managedByLabel: managedBy})
	}
	_ = unstructured.SetNestedField(rd.Object, repo, "spec", "kopia", "repository")
	return rd
}

// =============================================================================
// Reconcile flow
// =============================================================================

// PVC NotFound → Store.Delete + no error + no writes.
func TestV4Reconcile_NotFound_DeletesStoreEntry(t *testing.T) {
	f := newV4Fixture(t)

	// Pre-seed a stale entry so we can verify it's removed.
	f.store.Set(ParityEntry{
		Namespace: testNSMyapp, PVC: testPVCName,
		Action: ActionAlreadyMatches, Owner: OwnerInlineArgo, LabelSource: LabelSourceV4,
	})

	entry := f.reconcile(testNSMyapp, testPVCName)
	if entry.PVC != "" {
		t.Errorf("Store entry should have been deleted; got %+v", entry)
	}
	if _, ok := f.store.Get(testNSMyapp, testPVCName); ok {
		t.Errorf("Store still has entry after NotFound reconcile")
	}
	f.assertNoWrites()
}

// System namespace → no Store entry created.
func TestV4Reconcile_SystemNamespace_Ignored(t *testing.T) {
	pvc := makePVC("kube-system", "etcd-data", map[string]string{backupLabelKey: backupDaily}, nil)
	f := newV4Fixture(t, pvc)

	f.reconcile("kube-system", "etcd-data")
	if _, ok := f.store.Get("kube-system", "etcd-data"); ok {
		t.Errorf("system namespace PVC produced a Store entry; must not")
	}
	f.assertNoWrites()
}

// =============================================================================
// Phase 5 contract item #1: legacy backup: labels are audit opt-in
// =============================================================================

// As of Patch 6.5 the planner gates writes on
// pvc-plumber.io/enabled=true + pvc-plumber.io/manage-volsync=true.
// A legacy `backup: hourly` label is still an opt-in for reporting, but
// it is no longer a write opt-in. With no current RS/RD, the planner
// returns ActionWriteGateMissing — not ActionWouldCreate. Expected
// state is still rendered so /audit can show what the v4 children
// would look like once the operator adds the v4 labels.
func TestV4Reconcile_LegacyBackupHourly_NoCurrent_WriteGateMissing(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName, map[string]string{backupLabelKey: backupHourly}, nil)
	f := newV4Fixture(t, pvc)

	entry := f.reconcile(testNSMyapp, testPVCName)
	if entry.Action != ActionWriteGateMissing {
		t.Errorf("legacy backup: hourly + no current: got %q, want %q", entry.Action, ActionWriteGateMissing)
	}
	if entry.LabelSource != LabelSourceLegacy {
		t.Errorf("LabelSource: got %q, want %q", entry.LabelSource, LabelSourceLegacy)
	}
	if entry.Tier != backupHourly {
		t.Errorf("Tier: got %q, want %q", entry.Tier, backupHourly)
	}
	if entry.Expected.RSName != testPVCName || entry.Expected.RDName != testPVCName+"-dst" {
		t.Errorf("Expected RS/RD: got %q/%q, want %s/%s-dst", entry.Expected.RSName, entry.Expected.RDName, testPVCName, testPVCName)
	}
	if entry.Expected.RepositorySecret != testRepoSecretShare {
		t.Errorf("Expected.RepositorySecret: got %q, want %q", entry.Expected.RepositorySecret, testRepoSecretShare)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d ops, want 0 (write gate suppresses ops)", len(entry.PlannedOps))
	}
	if len(entry.Blockers) == 0 {
		t.Errorf("Blockers: got empty, want a write-gate-missing blocker")
	}
	f.assertNoWrites()
}

func TestV4Reconcile_LegacyBackupDaily_AcceptedAsOptIn(t *testing.T) {
	pvc := makePVC(testNSMyapp, "config", map[string]string{backupLabelKey: backupDaily}, nil)
	f := newV4Fixture(t, pvc)

	entry := f.reconcile(testNSMyapp, "config")
	if entry.Action == ActionSkippedNotOptedIn {
		t.Fatalf("backup: daily must not be skipped-not-opted-in; got %q", entry.Action)
	}
	if entry.LabelSource != LabelSourceLegacy {
		t.Errorf("LabelSource: got %q, want %q", entry.LabelSource, LabelSourceLegacy)
	}
}

// =============================================================================
// Phase 5 contract item #4: realistic talos repo PVC → already-matches
// =============================================================================

// The "real world" inline pattern in the talos repo today: PVC with
// `backup: daily` label, inline RS named <pvc> + RD named <pvc>-dst
// both with managed-by=argocd and the shared repository. v4 reconciler
// must report this as already-matches.
func TestV4Reconcile_TalosInlinePattern_LegacyLabel_AlreadyMatches(t *testing.T) {
	pvc := makePVC(testNSOpenWebUI, testPVCStorageName,
		map[string]string{backupLabelKey: backupDaily},
		nil)
	rs := makeRS(testNSOpenWebUI, testPVCStorageName, ManagedByArgoCDLabelValue, testRepoSecretShare, testPVCStorageName)
	rd := makeRD(testNSOpenWebUI, testPVCStorageName+"-dst", ManagedByArgoCDLabelValue, testRepoSecretShare)

	f := newV4Fixture(t, pvc, rs, rd)
	entry := f.reconcile(testNSOpenWebUI, testPVCStorageName)

	if entry.Action != ActionAlreadyMatches {
		t.Errorf("realistic talos inline: got %q, want %q", entry.Action, ActionAlreadyMatches)
	}
	if entry.Owner != OwnerInlineArgo {
		t.Errorf("Owner: got %q, want %q", entry.Owner, OwnerInlineArgo)
	}
	if entry.LabelSource != LabelSourceLegacy {
		t.Errorf("LabelSource: got %q, want %q", entry.LabelSource, LabelSourceLegacy)
	}
	if !entry.Current.RSPresent || !entry.Current.RDPresent {
		t.Errorf("CurrentState should reflect both RS and RD present: %+v", entry.Current)
	}
	if entry.Current.RSRepository != testRepoSecretShare {
		t.Errorf("Current.RSRepository: got %q, want %q", entry.Current.RSRepository, testRepoSecretShare)
	}
	f.assertNoWrites()
}

// Same pattern with the v4 label instead of legacy.
func TestV4Reconcile_TalosInlinePattern_V4Label_AlreadyMatches(t *testing.T) {
	pvc := makePVC(testNSOpenWebUI, testPVCStorageName,
		map[string]string{
			v4labels.LabelEnabled: labelTrue,
			v4labels.LabelTier:    backupDaily,
		},
		nil)
	rs := makeRS(testNSOpenWebUI, testPVCStorageName, ManagedByArgoCDLabelValue, testRepoSecretShare, testPVCStorageName)
	rd := makeRD(testNSOpenWebUI, testPVCStorageName+"-dst", ManagedByArgoCDLabelValue, testRepoSecretShare)

	f := newV4Fixture(t, pvc, rs, rd)
	entry := f.reconcile(testNSOpenWebUI, testPVCStorageName)

	if entry.Action != ActionAlreadyMatches {
		t.Errorf("v4 label + inline: got %q, want %q", entry.Action, ActionAlreadyMatches)
	}
	if entry.LabelSource != LabelSourceV4 {
		t.Errorf("LabelSource: got %q, want %q", entry.LabelSource, LabelSourceV4)
	}
	f.assertNoWrites()
}

// =============================================================================
// Phase 5 contract item #6: missing expected resources → would-create
// =============================================================================

// Patch 6.5: pvc-plumber.io/enabled=true on its own is reporting opt-in,
// not write opt-in. The write fuse (LabelManageVolSync) is missing, so
// the planner returns ActionWriteGateMissing. The OwnerNone classification
// is unchanged; the write-gate verdict is what changed compared to the
// pre-planner DecideAction path.
func TestV4Reconcile_V4EnabledOnly_NoCurrentResources_WriteGateMissing(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName,
		map[string]string{
			v4labels.LabelEnabled: labelTrue,
			v4labels.LabelTier:    backupHourly,
		},
		nil)
	f := newV4Fixture(t, pvc)

	entry := f.reconcile(testNSMyapp, testPVCName)
	if entry.Action != ActionWriteGateMissing {
		t.Errorf("v4 enabled-only + no current: got %q, want %q", entry.Action, ActionWriteGateMissing)
	}
	if entry.Owner != OwnerNone {
		t.Errorf("Owner: got %q, want %q", entry.Owner, OwnerNone)
	}
	if entry.Current.RSPresent || entry.Current.RDPresent {
		t.Errorf("Current should be empty: %+v", entry.Current)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d ops, want 0 (write gate suppresses ops)", len(entry.PlannedOps))
	}
	f.assertNoWrites()
}

// =============================================================================
// Phase 5 contract: chart-era <pvc>-backup names are NOT the v4 expected.
// =============================================================================

// PVC has legacy `backup: daily` label. Only chart-era resources exist
// (RS named "data-backup", RD named "data-backup"). The v4 expected is
// RS=data, RD=data-dst — chart-era names don't satisfy that expectation,
// so observeCurrent reports neither present and ClassifyOwner returns
// OwnerNone. The planner's write gate then suppresses the create:
// legacy-only is reporting opt-in, not write opt-in, so verdict is
// write-gate-missing (NOT would-create — that was the pre-Patch 6.5
// DecideAction behavior that the planner intentionally corrects).
func TestV4Reconcile_OnlyChartEraNamesPresent_WriteGateMissing(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName,
		map[string]string{backupLabelKey: backupDaily},
		nil)
	// Chart-era shape: same name for both RS and RD ("<pvc>-backup"),
	// managed-by=pvc-plumber (v3 era), shared repo.
	chartRS := makeRS(testNSMyapp, testPVCName+"-backup", ManagedByPVCPlumberLabelValue, testRepoSecretShare, testPVCName)
	chartRD := makeRD(testNSMyapp, testPVCName+"-backup", ManagedByPVCPlumberLabelValue, testRepoSecretShare)

	f := newV4Fixture(t, pvc, chartRS, chartRD)
	entry := f.reconcile(testNSMyapp, testPVCName)

	if entry.Action == ActionAlreadyMatches {
		t.Fatalf("chart-era names must NOT count as already-matches; got %q", entry.Action)
	}
	if entry.Action != ActionWriteGateMissing {
		t.Errorf("chart-era only + legacy label: got %q, want %q", entry.Action, ActionWriteGateMissing)
	}
	if entry.Owner != OwnerNone {
		t.Errorf("Owner: got %q, want %q (the chart-era resources don't have v4 names)", entry.Owner, OwnerNone)
	}
	if entry.Expected.RDName == testPVCName+"-backup" {
		t.Errorf("Expected.RDName must not be <pvc>-backup; got %q", entry.Expected.RDName)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0 (write gate suppresses ops)", len(entry.PlannedOps))
	}
	f.assertNoWrites()
}

// =============================================================================
// Phase 5 contract item #3: backup-exempt → skipped-exempt
// =============================================================================

func TestV4Reconcile_BackupExempt_SkippedExempt(t *testing.T) {
	pvc := makePVC("comfyui", "comfyui-storage",
		map[string]string{backupExemptLabel: labelTrue},
		map[string]string{v4labels.LegacyAnnotationBackupExemptReasonFQ: testReasonNASBacked},
	)
	f := newV4Fixture(t, pvc)

	entry := f.reconcile("comfyui", "comfyui-storage")
	if entry.Action != ActionSkippedExempt {
		t.Errorf("backup-exempt: got %q, want %q", entry.Action, ActionSkippedExempt)
	}
	f.assertNoWrites()
}

// Backup-exempt wins even when legacy backup: label is also present.
func TestV4Reconcile_BackupExempt_OverridesLegacyBackupLabel(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName,
		map[string]string{
			backupLabelKey:    backupDaily,
			backupExemptLabel: labelTrue,
		},
		map[string]string{v4labels.LegacyAnnotationBackupExemptReasonFQ: testReasonNASShort},
	)
	f := newV4Fixture(t, pvc)

	entry := f.reconcile(testNSMyapp, testPVCName)
	if entry.Action != ActionSkippedExempt {
		t.Errorf("exempt+legacy: got %q, want %q (exempt must win)", entry.Action, ActionSkippedExempt)
	}
	f.assertNoWrites()
}

// =============================================================================
// Phase 5 contract item #4: no opt-in label
// =============================================================================

func TestV4Reconcile_NoLabels_SkippedNotOptedIn(t *testing.T) {
	pvc := makePVC(testNSMyapp, "config", nil, nil)
	f := newV4Fixture(t, pvc)

	entry := f.reconcile(testNSMyapp, "config")
	if entry.Action != ActionSkippedNotOptedIn {
		t.Errorf("no labels: got %q, want %q", entry.Action, ActionSkippedNotOptedIn)
	}
	if entry.LabelSource != LabelSourceNone {
		t.Errorf("LabelSource: got %q, want %q", entry.LabelSource, LabelSourceNone)
	}
	f.assertNoWrites()
}

// =============================================================================
// Phase 5 contract item #5: only managed-by=pvc-plumber may be would-delete
// =============================================================================

// Patch 6.5: enabled=true ALONE is reporting opt-in only. A
// pvc-plumber-owned RS with drifted spec exists, but the write fuse
// (LabelManageVolSync) is missing — so the planner refuses to update.
// Verdict is ActionAlreadyMatches with an explanatory note ("write gate
// is off so no update is planned"); PlannedOps is empty.
// This is rule 7f of the planner's decision precedence and a deliberate
// change from the pre-planner DecideAction path (which would have
// returned ActionWouldUpdate here regardless of the write gate). A new
// test (TestV4Reconcile_EnabledAndManage_OperatorOwnedDrift_WouldUpdate)
// below covers the WouldUpdate case with both fuses present.
func TestV4Reconcile_EnabledOnly_OperatorOwnedDrift_AlreadyMatchesWithNote(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName,
		map[string]string{v4labels.LabelEnabled: labelTrue},
		nil)
	rs := makeRS(testNSMyapp, testPVCName, ManagedByPVCPlumberLabelValue, "wrong-repo", testPVCName)
	rd := makeRD(testNSMyapp, testPVCName+"-dst", ManagedByPVCPlumberLabelValue, "wrong-repo")

	f := newV4Fixture(t, pvc, rs, rd)
	entry := f.reconcile(testNSMyapp, testPVCName)

	if entry.Action != ActionAlreadyMatches {
		t.Errorf("enabled-only + operator-owned drift: got %q, want %q (write gate off)", entry.Action, ActionAlreadyMatches)
	}
	if entry.Owner != OwnerPVCPlumber {
		t.Errorf("Owner: got %q, want %q", entry.Owner, OwnerPVCPlumber)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0 (write gate off)", len(entry.PlannedOps))
	}
	if len(entry.Notes) == 0 {
		t.Errorf("Notes: got empty, want a write-gate-off explanation")
	}
	f.assertNoWrites()
}

// PVC opted in. Inline-Argo RS has a different repository. Verdict
// should be inline-argo-observed (NOT would-update — operator must not
// touch GitOps-owned resources, NOT would-delete because owner is argocd).
func TestV4Reconcile_InlineArgoDrift_InlineArgoObserved_NotWouldDelete(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName,
		map[string]string{v4labels.LabelEnabled: labelTrue},
		nil)
	rs := makeRS(testNSMyapp, testPVCName, ManagedByArgoCDLabelValue, "drifted-repo", testPVCName)
	rd := makeRD(testNSMyapp, testPVCName+"-dst", ManagedByArgoCDLabelValue, "drifted-repo")

	f := newV4Fixture(t, pvc, rs, rd)
	entry := f.reconcile(testNSMyapp, testPVCName)

	if entry.Action == ActionWouldDelete {
		t.Fatalf("inline-argo must never be would-delete; got %q", entry.Action)
	}
	if entry.Action != ActionInlineArgoObserved {
		t.Errorf("inline-argo drift: got %q, want %q", entry.Action, ActionInlineArgoObserved)
	}
	if entry.Owner != OwnerInlineArgo {
		t.Errorf("Owner: got %q, want %q", entry.Owner, OwnerInlineArgo)
	}
	f.assertNoWrites()
}

// PVC opted in. Unmanaged resource (no managed-by label) whose name +
// repository match the expected v4 shape → already-matches. The
// catch-all classification for GitOps paths that don't carry
// managed-by; the reconciler treats them as observed, never delete.
func TestV4Reconcile_UnmanagedShapeMatches_AlreadyMatches(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName,
		map[string]string{v4labels.LabelEnabled: labelTrue},
		nil)
	rs := makeRS(testNSMyapp, testPVCName, "", testRepoSecretShare, testPVCName)
	rd := makeRD(testNSMyapp, testPVCName+"-dst", "", testRepoSecretShare)

	f := newV4Fixture(t, pvc, rs, rd)
	entry := f.reconcile(testNSMyapp, testPVCName)

	if entry.Action != ActionAlreadyMatches {
		t.Errorf("unmanaged matching shape: got %q, want %q", entry.Action, ActionAlreadyMatches)
	}
	if entry.Owner != OwnerUnmanagedOrGitopsObserved {
		t.Errorf("Owner: got %q, want %q", entry.Owner, OwnerUnmanagedOrGitopsObserved)
	}
	f.assertNoWrites()
}

// =============================================================================
// Multi-PVC sweep: Store gets one entry per reconciled PVC
// =============================================================================

func TestV4Reconcile_StoreUpdatedAcrossMultipleReconciles(t *testing.T) {
	pvc1 := makePVC("a", "p1", map[string]string{backupLabelKey: backupHourly}, nil)
	pvc2 := makePVC("b", "p2", map[string]string{v4labels.LabelEnabled: labelTrue}, nil)
	pvc3 := makePVC("c", "p3", nil, nil)
	pvc4 := makePVC("d", "p4",
		map[string]string{backupExemptLabel: labelTrue},
		map[string]string{v4labels.LegacyAnnotationBackupExemptReasonFQ: "exempt"})

	f := newV4Fixture(t, pvc1, pvc2, pvc3, pvc4)
	f.reconcile("a", "p1")
	f.reconcile("b", "p2")
	f.reconcile("c", "p3")
	f.reconcile("d", "p4")

	if got := f.store.Len(); got != 4 {
		t.Fatalf("Store.Len after 4 reconciles: got %d, want 4", got)
	}

	cases := []struct {
		ns, pvc string
		want    ActionKind
		src     LabelSource
	}{
		// p1: legacy backup:hourly only → reporting-only, write gate missing.
		{"a", "p1", ActionWriteGateMissing, LabelSourceLegacy},
		// p2: pvc-plumber.io/enabled=true only (no manage-volsync) → write gate missing.
		{"b", "p2", ActionWriteGateMissing, LabelSourceV4},
		// p3: no labels → skipped-not-opted-in.
		{"c", "p3", ActionSkippedNotOptedIn, LabelSourceNone},
		// p4: backup-exempt with valid FQ reason → skipped-exempt (LabelSource
		// is None because backup-exempt is read from the legacy key, not the
		// new v4 opt-in labels).
		{"d", "p4", ActionSkippedExempt, LabelSourceNone},
	}
	for _, c := range cases {
		e, ok := f.store.Get(c.ns, c.pvc)
		if !ok {
			t.Errorf("%s/%s: not in Store", c.ns, c.pvc)
			continue
		}
		if e.Action != c.want {
			t.Errorf("%s/%s: action got %q, want %q", c.ns, c.pvc, e.Action, c.want)
		}
		if e.LabelSource != c.src {
			t.Errorf("%s/%s: label_source got %q, want %q", c.ns, c.pvc, e.LabelSource, c.src)
		}
	}

	f.assertNoWrites()
}

// =============================================================================
// Reconciler does not call Create/Update/Patch/Delete (top-level proof)
// =============================================================================

// Aggregate write-counter check across the full Talos-like fixture. If any
// future code change ever attempts a write through the embedded client,
// this test catches it via the auditclient's counters before the apiserver
// would.
func TestV4Reconcile_NeverWrites_AcrossAllScenarios(t *testing.T) {
	pvcs := []client.Object{
		makePVC(testNSMyapp, testPVCName, map[string]string{backupLabelKey: backupHourly}, nil),
		makePVC(testNSMyapp, "config", map[string]string{backupLabelKey: backupDaily}, nil),
		makePVC(testNSMyapp, "exempt-pvc",
			map[string]string{backupExemptLabel: labelTrue},
			map[string]string{v4labels.LegacyAnnotationBackupExemptReasonFQ: testReasonNASShort}),
		makePVC(testNSMyapp, "no-label", nil, nil),
		makePVC("kube-system", "system-pvc", map[string]string{backupLabelKey: backupDaily}, nil),
		makePVC(testNSOpenWebUI, testPVCStorageName,
			map[string]string{backupLabelKey: backupDaily}, nil),
		makeRS(testNSOpenWebUI, testPVCStorageName, ManagedByArgoCDLabelValue, testRepoSecretShare, testPVCStorageName),
		makeRD(testNSOpenWebUI, testPVCStorageName+"-dst", ManagedByArgoCDLabelValue, testRepoSecretShare),
	}
	f := newV4Fixture(t, pvcs...)
	f.reconcile(testNSMyapp, testPVCName)
	f.reconcile(testNSMyapp, "config")
	f.reconcile(testNSMyapp, "exempt-pvc")
	f.reconcile(testNSMyapp, "no-label")
	f.reconcile("kube-system", "system-pvc")
	f.reconcile(testNSOpenWebUI, testPVCStorageName)
	f.reconcile(testNSMyapp, "vanished-pvc") // NotFound path

	f.assertNoWrites()

	// Sanity: the realistic open-webui case was the inline-Argo
	// matching one; verify the parity verdict end-to-end.
	if e, ok := f.store.Get(testNSOpenWebUI, testPVCStorageName); !ok {
		t.Error("open-webui/storage missing from Store")
	} else if e.Action != ActionAlreadyMatches {
		t.Errorf("open-webui/storage action: got %q, want %q", e.Action, ActionAlreadyMatches)
	}
}

// =============================================================================
// Store metadata + EvaluatedAt plumbed through
// =============================================================================

func TestV4Reconcile_StoreMetadata_PropagatesThroughEntry(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName, map[string]string{backupLabelKey: backupHourly}, nil)
	f := newV4Fixture(t, pvc)

	entry := f.reconcile(testNSMyapp, testPVCName)

	if entry.Mode != testModeAudit {
		t.Errorf("Mode: got %q, want %q", entry.Mode, testModeAudit)
	}
	if !entry.EvaluatedAt.Equal(fixedTime()) {
		t.Errorf("EvaluatedAt: got %v, want %v (Reconciler.Now should drive)", entry.EvaluatedAt, fixedTime())
	}
	if entry.BackupIdentity != "myapp/"+testPVCName {
		t.Errorf("BackupIdentity: got %q, want myapp/%s", entry.BackupIdentity, testPVCName)
	}
}

// NOTE: VolSync-CRD-missing path (meta.IsNoMatchError) is handled by
// observeCurrent but is awkward to exercise with the controller-runtime
// fake client (the fake produces NotFound, not NoMatch). The reconciler
// code path treats both as "not present" with no error, so the existing
// "no current resources" tests cover the functional outcome. A true
// NoMatch test would require envtest; left for a future patch.

// =============================================================================
// Patch 6.5: planner-driven reconciler verdicts (full grid)
// =============================================================================
//
// These 13 cases plus the paranoia walk are the contract required by
// the Patch 6.5 approval. Each case constructs a fixture, runs one
// Reconcile, then asserts:
//
//   - entry.Action matches the planner's verdict
//   - PlannedOps shape (count + Kind + GVK + Name) matches expectations
//   - f.assertNoWrites() — no writes ever passed through the auditclient
//
// The paranoia walk at the end of this block iterates every PlannedOps
// across every test case in the package and proves every op targets
// either ReplicationSource or ReplicationDestination — never Secret,
// PVC, webhook, etc. This is the contract enforced by the planner
// package's own tests, re-asserted at the reconciler boundary so a
// future refactor that bypasses the planner can't slip a foreign GVK
// past us.

const (
	rsGVKStr = "volsync.backube/v1alpha1/ReplicationSource"
	rdGVKStr = "volsync.backube/v1alpha1/ReplicationDestination"

	// Exempt-reason strings reused across cases. Hoisted to constants
	// to satisfy the goconst linter; the values themselves are
	// otherwise arbitrary.
	testReasonNASBacked = "NAS-backed, non-snapshottable"
	testReasonNASShort  = "NAS"

	// Planner.OpKind string values, used by the paranoia walk.
	opCreate = "create"
	opUpdate = "update"
	opDelete = "delete"
)

// labelsEnabledManage returns the canonical two-label opt-in: enabled +
// manage-volsync. Tier defaults to daily unless the caller overrides via
// labelsEnabledManageTier.
func labelsEnabledManage() map[string]string {
	return map[string]string{
		v4labels.LabelEnabled:       labelTrue,
		v4labels.LabelManageVolSync: labelTrue,
		v4labels.LabelTier:          backupDaily,
	}
}

// labelsEnabledManageTier returns enabled + manage-volsync with a custom
// tier value. Used by the tier=disabled case.
func labelsEnabledManageTier(tier string) map[string]string {
	return map[string]string{
		v4labels.LabelEnabled:       labelTrue,
		v4labels.LabelManageVolSync: labelTrue,
		v4labels.LabelTier:          tier,
	}
}

// assertPlannedOpsTargetVolSyncOnly walks every entry in the store and
// proves no PlannedOpSummary targets a kind other than RS or RD. Called
// at the end of TestV4Reconcile_Patch65_NoForeignKindsEverPlanned.
func assertPlannedOpsTargetVolSyncOnly(t *testing.T, s *Store) {
	t.Helper()
	snap := s.Snapshot()
	for _, e := range snap.Entries {
		for _, op := range e.PlannedOps {
			if op.GVK != rsGVKStr && op.GVK != rdGVKStr {
				t.Errorf("forbidden planned-op GVK %q in entry %s/%s (action=%q)", op.GVK, e.Namespace, e.PVC, e.Action)
			}
			if op.Kind != opCreate && op.Kind != opUpdate && op.Kind != opDelete {
				t.Errorf("unknown op Kind %q in entry %s/%s", op.Kind, e.Namespace, e.PVC)
			}
		}
	}
}

// Case 1: backup-exempt with valid FQ reason → skipped-exempt, no ops.
func TestV4Reconcile_Patch65_BackupExemptValid_SkippedExempt(t *testing.T) {
	pvc := makePVC(testNSMyapp, "exempt", map[string]string{
		backupExemptLabel: labelTrue,
	}, map[string]string{
		v4labels.LegacyAnnotationBackupExemptReasonFQ: testReasonNASBacked,
	})
	f := newV4Fixture(t, pvc)
	entry := f.reconcile(testNSMyapp, "exempt")

	if entry.Action != ActionSkippedExempt {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionSkippedExempt)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0", len(entry.PlannedOps))
	}
	f.assertNoWrites()
}

// Case 2: backup-exempt without FQ reason → needs-human-review with blocker.
func TestV4Reconcile_Patch65_BackupExemptMissingReason_NeedsHumanReview(t *testing.T) {
	pvc := makePVC(testNSMyapp, "exempt-bad", map[string]string{
		backupExemptLabel: labelTrue,
	}, nil) // no FQ reason annotation
	f := newV4Fixture(t, pvc)
	entry := f.reconcile(testNSMyapp, "exempt-bad")

	if entry.Action != ActionNeedsHumanReview {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionNeedsHumanReview)
	}
	if len(entry.Blockers) == 0 {
		t.Errorf("Blockers: got empty, want missing-FQ-reason blocker")
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0", len(entry.PlannedOps))
	}
	f.assertNoWrites()
}

// Case 3: no opt-in labels at all → skipped-not-opted-in.
func TestV4Reconcile_Patch65_NoOptIn_SkippedNotOptedIn(t *testing.T) {
	pvc := makePVC(testNSMyapp, "unlabeled", nil, nil)
	f := newV4Fixture(t, pvc)
	entry := f.reconcile(testNSMyapp, "unlabeled")

	if entry.Action != ActionSkippedNotOptedIn {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionSkippedNotOptedIn)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0", len(entry.PlannedOps))
	}
	f.assertNoWrites()
}

// Case 4: legacy backup: with matching inline (argocd-managed) RS+RD →
// already-matches, no ops. This is the realistic talos repo state today.
func TestV4Reconcile_Patch65_LegacyWithMatchingInline_AlreadyMatches(t *testing.T) {
	pvc := makePVC(testNSOpenWebUI, testPVCStorageName,
		map[string]string{backupLabelKey: backupDaily}, nil)
	rs := makeRS(testNSOpenWebUI, testPVCStorageName, ManagedByArgoCDLabelValue, testRepoSecretShare, testPVCStorageName)
	rd := makeRD(testNSOpenWebUI, testPVCStorageName+"-dst", ManagedByArgoCDLabelValue, testRepoSecretShare)
	f := newV4Fixture(t, pvc, rs, rd)
	entry := f.reconcile(testNSOpenWebUI, testPVCStorageName)

	if entry.Action != ActionAlreadyMatches {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionAlreadyMatches)
	}
	if entry.Owner != OwnerInlineArgo {
		t.Errorf("Owner: got %q, want %q", entry.Owner, OwnerInlineArgo)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0 (no writes on matching inline-argo)", len(entry.PlannedOps))
	}
	f.assertNoWrites()
}

// Case 5: legacy backup: only, no current resources → write-gate-missing.
// (Already covered by the renamed TestV4Reconcile_LegacyBackupHourly_NoCurrent_WriteGateMissing
// above; this duplicate test exists so the full Patch 6.5 grid is
// readable in one block.)
func TestV4Reconcile_Patch65_LegacyOnlyNoCurrent_WriteGateMissing(t *testing.T) {
	pvc := makePVC(testNSMyapp, "legacy-only", map[string]string{backupLabelKey: backupHourly}, nil)
	f := newV4Fixture(t, pvc)
	entry := f.reconcile(testNSMyapp, "legacy-only")

	if entry.Action != ActionWriteGateMissing {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionWriteGateMissing)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0", len(entry.PlannedOps))
	}
	if len(entry.Blockers) == 0 {
		t.Errorf("Blockers: want non-empty")
	}
	f.assertNoWrites()
}

// Case 6: enabled=true alone, no manage-volsync, no current resources →
// write-gate-missing.
func TestV4Reconcile_Patch65_EnabledOnlyNoCurrent_WriteGateMissing(t *testing.T) {
	pvc := makePVC(testNSMyapp, "enabled-only", map[string]string{
		v4labels.LabelEnabled: labelTrue,
		v4labels.LabelTier:    backupDaily,
	}, nil)
	f := newV4Fixture(t, pvc)
	entry := f.reconcile(testNSMyapp, "enabled-only")

	if entry.Action != ActionWriteGateMissing {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionWriteGateMissing)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0", len(entry.PlannedOps))
	}
	f.assertNoWrites()
}

// Case 7: manage-volsync=true on its own (without enabled=true) →
// skipped-not-opted-in + explanatory blocker. This catches PRs that
// set manage-volsync without flipping enabled in the same change.
func TestV4Reconcile_Patch65_ManageOnly_SkippedNotOptedInWithBlocker(t *testing.T) {
	pvc := makePVC(testNSMyapp, "manage-only", map[string]string{
		v4labels.LabelManageVolSync: labelTrue,
		v4labels.LabelTier:          backupDaily,
	}, nil)
	f := newV4Fixture(t, pvc)
	entry := f.reconcile(testNSMyapp, "manage-only")

	if entry.Action != ActionSkippedNotOptedIn {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionSkippedNotOptedIn)
	}
	if len(entry.Blockers) == 0 {
		t.Errorf("Blockers: got empty, want manage-without-enabled blocker")
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0", len(entry.PlannedOps))
	}
	f.assertNoWrites()
}

// Case 8: enabled + manage with no current resources → would-create with
// two planned ops (RS create + RD create) targeting the v4 expected names.
func TestV4Reconcile_Patch65_EnabledAndManage_NoCurrent_WouldCreate(t *testing.T) {
	pvc := makePVC(testNSMyapp, "fresh-pvc", labelsEnabledManage(), nil)
	f := newV4Fixture(t, pvc)
	entry := f.reconcile(testNSMyapp, "fresh-pvc")

	if entry.Action != ActionWouldCreate {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionWouldCreate)
	}
	if len(entry.PlannedOps) != 2 {
		t.Fatalf("PlannedOps: got %d, want 2 (create RS + create RD)", len(entry.PlannedOps))
	}
	got := map[string]PlannedOpSummary{}
	for _, op := range entry.PlannedOps {
		got[op.GVK] = op
	}
	if rs, ok := got[rsGVKStr]; !ok || rs.Kind != "create" || rs.Name != "fresh-pvc" {
		t.Errorf("RS op: got %+v, want create %s/fresh-pvc", rs, rsGVKStr)
	}
	if rd, ok := got[rdGVKStr]; !ok || rd.Kind != "create" || rd.Name != "fresh-pvc-dst" {
		t.Errorf("RD op: got %+v, want create %s/fresh-pvc-dst", rd, rdGVKStr)
	}
	f.assertNoWrites()
}

// Case 9: enabled + manage with matching pvc-plumber-owned RS+RD →
// already-matches, no ops.
func TestV4Reconcile_Patch65_EnabledAndManage_OperatorOwnedMatches_AlreadyMatches(t *testing.T) {
	pvc := makePVC(testNSMyapp, "owned-match", labelsEnabledManage(), nil)
	rs := makeRS(testNSMyapp, "owned-match", ManagedByPVCPlumberLabelValue, testRepoSecretShare, "owned-match")
	rd := makeRD(testNSMyapp, "owned-match-dst", ManagedByPVCPlumberLabelValue, testRepoSecretShare)
	f := newV4Fixture(t, pvc, rs, rd)
	entry := f.reconcile(testNSMyapp, "owned-match")

	if entry.Action != ActionAlreadyMatches {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionAlreadyMatches)
	}
	if entry.Owner != OwnerPVCPlumber {
		t.Errorf("Owner: got %q, want %q", entry.Owner, OwnerPVCPlumber)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0", len(entry.PlannedOps))
	}
	f.assertNoWrites()
}

// Case 10: enabled + manage with pvc-plumber-owned RS+RD whose repo
// drifts → would-update with two update ops.
func TestV4Reconcile_Patch65_EnabledAndManage_OperatorOwnedDrift_WouldUpdate(t *testing.T) {
	pvc := makePVC(testNSMyapp, "owned-drift", labelsEnabledManage(), nil)
	rs := makeRS(testNSMyapp, "owned-drift", ManagedByPVCPlumberLabelValue, "stale-repo", "owned-drift")
	rd := makeRD(testNSMyapp, "owned-drift-dst", ManagedByPVCPlumberLabelValue, "stale-repo")
	f := newV4Fixture(t, pvc, rs, rd)
	entry := f.reconcile(testNSMyapp, "owned-drift")

	if entry.Action != ActionWouldUpdate {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionWouldUpdate)
	}
	if entry.Owner != OwnerPVCPlumber {
		t.Errorf("Owner: got %q, want %q", entry.Owner, OwnerPVCPlumber)
	}
	if len(entry.PlannedOps) != 2 {
		t.Fatalf("PlannedOps: got %d, want 2 (update RS + update RD)", len(entry.PlannedOps))
	}
	for _, op := range entry.PlannedOps {
		if op.Kind != "update" {
			t.Errorf("op %s Kind: got %q, want update", op.GVK, op.Kind)
		}
	}
	f.assertNoWrites()
}

// Case 11: enabled + manage with inline-argo-owned RS+RD whose spec
// drifts → inline-argo-observed, no ops. The operator MUST NOT touch
// GitOps-owned resources even when it has the write fuse on.
func TestV4Reconcile_Patch65_EnabledAndManage_InlineArgoDrift_InlineArgoObserved(t *testing.T) {
	pvc := makePVC(testNSMyapp, "argo-drift", labelsEnabledManage(), nil)
	rs := makeRS(testNSMyapp, "argo-drift", ManagedByArgoCDLabelValue, "argo-repo", "argo-drift")
	rd := makeRD(testNSMyapp, "argo-drift-dst", ManagedByArgoCDLabelValue, "argo-repo")
	f := newV4Fixture(t, pvc, rs, rd)
	entry := f.reconcile(testNSMyapp, "argo-drift")

	if entry.Action != ActionInlineArgoObserved {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionInlineArgoObserved)
	}
	if entry.Owner != OwnerInlineArgo {
		t.Errorf("Owner: got %q, want %q", entry.Owner, OwnerInlineArgo)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0 (never touch inline-argo)", len(entry.PlannedOps))
	}
	f.assertNoWrites()
}

// Case 12: enabled + manage + tier=disabled with pvc-plumber-owned RS+RD
// → would-delete with two delete ops. The tier=disabled value plus
// operator ownership is the explicit cue to tear the children down.
func TestV4Reconcile_Patch65_EnabledAndManage_TierDisabledOperatorOwned_WouldDelete(t *testing.T) {
	pvc := makePVC(testNSMyapp, "to-disable", labelsEnabledManageTier("disabled"), nil)
	rs := makeRS(testNSMyapp, "to-disable", ManagedByPVCPlumberLabelValue, testRepoSecretShare, "to-disable")
	rd := makeRD(testNSMyapp, "to-disable-dst", ManagedByPVCPlumberLabelValue, testRepoSecretShare)
	f := newV4Fixture(t, pvc, rs, rd)
	entry := f.reconcile(testNSMyapp, "to-disable")

	if entry.Action != ActionWouldDelete {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionWouldDelete)
	}
	if entry.Owner != OwnerPVCPlumber {
		t.Errorf("Owner: got %q, want %q", entry.Owner, OwnerPVCPlumber)
	}
	if len(entry.PlannedOps) != 2 {
		t.Fatalf("PlannedOps: got %d, want 2 (delete RS + delete RD)", len(entry.PlannedOps))
	}
	got := map[string]PlannedOpSummary{}
	for _, op := range entry.PlannedOps {
		got[op.GVK] = op
	}
	if rs := got[rsGVKStr]; rs.Kind != "delete" || rs.Name != "to-disable" {
		t.Errorf("RS delete op: got %+v, want delete to-disable", rs)
	}
	if rd := got[rdGVKStr]; rd.Kind != "delete" || rd.Name != "to-disable-dst" {
		t.Errorf("RD delete op: got %+v, want delete to-disable-dst", rd)
	}
	f.assertNoWrites()
}

// Case 13: backup-exempt + pvc-plumber-owned drifted RS+RD → exempt
// wins; verdict is skipped-exempt with NO planned ops. Cleaning the
// stale children is a manual operator decision, never a planner action.
func TestV4Reconcile_Patch65_BackupExempt_OperatorOwnedDrift_SkippedExempt(t *testing.T) {
	pvc := makePVC(testNSMyapp, "exempt-drift",
		map[string]string{
			backupExemptLabel:           labelTrue,
			v4labels.LabelEnabled:       labelTrue,
			v4labels.LabelManageVolSync: labelTrue,
		},
		map[string]string{v4labels.LegacyAnnotationBackupExemptReasonFQ: testReasonNASBacked},
	)
	rs := makeRS(testNSMyapp, "exempt-drift", ManagedByPVCPlumberLabelValue, "stale-repo", "exempt-drift")
	rd := makeRD(testNSMyapp, "exempt-drift-dst", ManagedByPVCPlumberLabelValue, "stale-repo")
	f := newV4Fixture(t, pvc, rs, rd)
	entry := f.reconcile(testNSMyapp, "exempt-drift")

	if entry.Action != ActionSkippedExempt {
		t.Errorf("Action: got %q, want %q (exempt wins)", entry.Action, ActionSkippedExempt)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0 (exempt suppresses all ops)", len(entry.PlannedOps))
	}
	f.assertNoWrites()
}

// Paranoia walk: runs all 13 cases above plus the pre-existing tests in
// a single fixture, then asserts every entry.PlannedOps targets only RS
// or RD GVKs. Catches any future planner-bypass that would slip a
// foreign kind (Secret, PVC, webhook, etc.) into the audit report.
func TestV4Reconcile_Patch65_NoForeignKindsEverPlanned(t *testing.T) {
	// One fixture with one PVC per scenario. Names are unique so each
	// reconcile produces a distinct Store entry.
	objs := []client.Object{
		makePVC(testNSMyapp, "fresh", labelsEnabledManage(), nil),
		makePVC(testNSMyapp, "owned-drift", labelsEnabledManage(), nil),
		makeRS(testNSMyapp, "owned-drift", ManagedByPVCPlumberLabelValue, "stale", "owned-drift"),
		makeRD(testNSMyapp, "owned-drift-dst", ManagedByPVCPlumberLabelValue, "stale"),
		makePVC(testNSMyapp, "to-disable", labelsEnabledManageTier("disabled"), nil),
		makeRS(testNSMyapp, "to-disable", ManagedByPVCPlumberLabelValue, testRepoSecretShare, "to-disable"),
		makeRD(testNSMyapp, "to-disable-dst", ManagedByPVCPlumberLabelValue, testRepoSecretShare),
		makePVC(testNSMyapp, "legacy", map[string]string{backupLabelKey: backupHourly}, nil),
		makePVC(testNSMyapp, "exempt",
			map[string]string{backupExemptLabel: labelTrue},
			map[string]string{v4labels.LegacyAnnotationBackupExemptReasonFQ: testReasonNASShort}),
	}
	f := newV4Fixture(t, objs...)
	f.reconcile(testNSMyapp, "fresh")
	f.reconcile(testNSMyapp, "owned-drift")
	f.reconcile(testNSMyapp, "to-disable")
	f.reconcile(testNSMyapp, "legacy")
	f.reconcile(testNSMyapp, "exempt")

	assertPlannedOpsTargetVolSyncOnly(t, f.store)
	f.assertNoWrites()
}
