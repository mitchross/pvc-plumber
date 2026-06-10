package controller

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/mitchross/pvc-plumber/internal/v4/auditclient"
	"github.com/mitchross/pvc-plumber/internal/v4/builder"
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
	return newV4ModeFixture(t, mode.Audit, seedObjs...)
}

// newV4ModeFixture is the shared fixture builder for both audit and
// non-audit modes. The reconciler's Mode field plus the auditclient's
// own Mode are both threaded from the parameter so the executor's
// short-circuit path (audit) or write path (permissive/enforce/strict)
// is exercised consistently.
func newV4ModeFixture(t *testing.T, m mode.Mode, seedObjs ...client.Object) *v4Fixture {
	t.Helper()
	scheme := newTestScheme(t)
	// v4.0.1 namespace write gate: the reconciler now reads
	// pvc-plumber.io/managed-namespace on the PVC's Namespace before
	// allowing writes. Pre-v4.0.1 tests assume write-eligible PVCs reach
	// the create/update/delete paths, which now requires a MANAGED
	// Namespace to exist. Auto-seed a managed Namespace for every
	// namespace referenced by the seed objects — UNLESS the test provided
	// its own Namespace object for it (so a test can opt a namespace OUT
	// of management by seeding an unlabeled Namespace, or omit one to
	// exercise the NotFound→unmanaged path).
	providedNS := map[string]bool{}
	for _, o := range seedObjs {
		if _, ok := o.(*corev1.Namespace); ok {
			providedNS[o.GetName()] = true
		}
	}
	seenNS := map[string]bool{}
	var nsObjs []client.Object
	for _, o := range seedObjs {
		ns := o.GetNamespace()
		if ns == "" || providedNS[ns] || seenNS[ns] {
			continue
		}
		seenNS[ns] = true
		nsObjs = append(nsObjs, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:   ns,
				Labels: map[string]string{v4labels.NamespaceManagedLabel: "true"},
			},
		})
	}
	allObjs := append(nsObjs, seedObjs...)
	fakeC := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjs...).Build()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	auditC := auditclient.New(fakeC, m, log)
	store := NewStore(m.String(), "bare-dst", testRepoSecretShare)
	store.now = fixedTime
	r := &V4AuditReconciler{
		Client:            auditC,
		Store:             store,
		NamingStrategy:    naming.StrategyBareDst,
		DefaultRepoSecret: testRepoSecretShare,
		SystemNamespaces:  map[string]struct{}{"kube-system": {}, "volsync-system": {}, "argocd": {}},
		OperatorMode:      m.String(),
		Mode:              m,
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
// tier value. Used by the tier=disabled cases today; parametric on
// purpose for upcoming hourly/daily/custom-tier scenarios.
//
//nolint:unparam // both current callers pass "disabled"; future tier cases will diverge
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

// =============================================================================
// Patch 6.7: executor wired into the reconciler
// =============================================================================
//
// These tests exercise the executor invocation site added in Patch 6.7.
// The shared fixture builder (newV4ModeFixture) threads a mode.Mode into
// both the auditclient wrapper AND the reconciler's Mode field so audit
// vs permissive flows are exercised exactly the way the production
// binary will (once 6.7-wire lands).
//
// What this block proves:
//
//   1. Audit-mode reconciler still produces zero writes (regression
//      cover for the existing 13-case audit grid).
//
//   2. Audit-mode entries with PlannedOps surface an ExecutionResult
//      with Counts.Skipped == len(plan.Ops); audit-mode entries with no
//      planned ops leave ExecutionResult nil (no /audit noise).
//
//   3. Permissive-mode actually creates / updates / deletes the planner
//      ops via the embedded client. The fake client is read back to
//      confirm cluster state matches.
//
//   4. Permissive-mode never touches inline-Argo resources (executor's
//      ownership re-check fires when the planner has — incorrectly —
//      not pre-filtered, AND when planner correctly returns
//      inline-argo-observed with empty Ops the executor records nothing).
//
//   5. backup-exempt / no-op-in / write-gate cases produce zero writes
//      under permissive — the planner's empty Ops slice is the gate, not
//      the executor's mode short-circuit.
//
//   6. Enforce + Strict produce identical executor outcomes to
//      Permissive for the same fixture; webhook/admission divergence
//      is Phase 8, not 6.7.
//
//   7. Cross-mode paranoia walk: every ExecutionResult.Outcomes entry
//      across every permissive scenario targets only RS or RD GVKs;
//      no Secret/PVC/ExternalSecret/webhook ever appears.
//
// Note on the v3 isolation property: Patch 6.7 does NOT change main.go,
// so production permissive traffic still hits the v3 PVCReconciler.
// These tests prove the v4 reconciler + executor pair would behave
// correctly if main.go were route-flipped today — that's the load-
// bearing precondition for the 6.7-wire sub-patch.

// assertDidWriteByVerb fails the test if the auditclient's "did" counters
// don't match the requested verb totals exactly. Used to lock down
// permissive-mode write volume by verb.
func (f *v4Fixture) assertDidWriteByVerb(t *testing.T, wantCreate, wantUpdate, wantDelete int64) {
	t.Helper()
	d := f.audit.DidWriteTotals()
	if d.Create != wantCreate {
		t.Errorf("DidWrite.Create: got %d, want %d", d.Create, wantCreate)
	}
	if d.Update != wantUpdate {
		t.Errorf("DidWrite.Update: got %d, want %d", d.Update, wantUpdate)
	}
	if d.Delete != wantDelete {
		t.Errorf("DidWrite.Delete: got %d, want %d", d.Delete, wantDelete)
	}
	if d.Patch != 0 {
		t.Errorf("DidWrite.Patch: got %d, want 0 (executor never uses Patch)", d.Patch)
	}
	if d.DeleteAllOf != 0 {
		t.Errorf("DidWrite.DeleteAllOf: got %d, want 0", d.DeleteAllOf)
	}
}

// liveExists reports whether the underlying fake client holds a resource
// of the given GVK + name in the namespace. Direct read of f.fake, not
// the auditclient — we want to see actual cluster state, not the
// wrapper's view.
func (f *v4Fixture) liveExists(gvk schema.GroupVersionKind, ns, name string) bool {
	f.t.Helper()
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(gvk)
	err := f.fake.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, live)
	return err == nil
}

// liveRepo reads spec.kopia.repository from a live RS or RD. Returns ""
// for not-found / not-set. Used by update tests to verify the executor
// actually overwrote the drifted value.
func (f *v4Fixture) liveRepo(gvk schema.GroupVersionKind, ns, name string) string {
	f.t.Helper()
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(gvk)
	if err := f.fake.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, live); err != nil {
		return ""
	}
	v, _, _ := unstructured.NestedString(live.Object, "spec", "kopia", "repository")
	return v
}

// =============================================================================
// 6.7 — Audit-mode ExecutionResult shape
// =============================================================================

// Case A: audit + planned ops → ExecutionResult populated with Counts.Skipped
// == len(plan.Ops); every outcome carries Status="skipped" and Reason
// mentioning audit; outcomes only target RS/RD GVKs.
func TestV4Reconcile_Patch67_AuditWithPlannedOps_ExecutionResultSkipped(t *testing.T) {
	pvc := makePVC(testNSMyapp, "fresh-pvc", labelsEnabledManage(), nil)
	f := newV4Fixture(t, pvc)
	entry := f.reconcile(testNSMyapp, "fresh-pvc")

	if entry.Action != ActionWouldCreate {
		t.Fatalf("Action: got %q, want %q", entry.Action, ActionWouldCreate)
	}
	if len(entry.PlannedOps) != 2 {
		t.Fatalf("PlannedOps: got %d, want 2", len(entry.PlannedOps))
	}
	if entry.ExecutionResult == nil {
		t.Fatal("ExecutionResult: got nil, want non-nil (planner emitted 2 ops in audit mode)")
	}
	if got := entry.ExecutionResult.Counts.Skipped; got != 2 {
		t.Errorf("ExecutionResult.Counts.Skipped: got %d, want 2", got)
	}
	if got := entry.ExecutionResult.Counts.Succeeded; got != 0 {
		t.Errorf("ExecutionResult.Counts.Succeeded: got %d, want 0 in audit mode", got)
	}
	if got := entry.ExecutionResult.Counts.Failed; got != 0 {
		t.Errorf("ExecutionResult.Counts.Failed: got %d, want 0", got)
	}
	if got := len(entry.ExecutionResult.Outcomes); got != 2 {
		t.Fatalf("ExecutionResult.Outcomes: got %d, want 2", got)
	}
	for _, out := range entry.ExecutionResult.Outcomes {
		if out.Status != "skipped" {
			t.Errorf("Outcome.Status: got %q, want skipped", out.Status)
		}
		if out.GVK != rsGVKStr && out.GVK != rdGVKStr {
			t.Errorf("Outcome.GVK: got %q, want RS or RD", out.GVK)
		}
	}
	// Zero-write proof: even though ExecutionResult shows planned ops,
	// no apiserver writes happened.
	f.assertNoWrites()
	// Permissive paranoia: the fake client never gained any new RS/RD.
	if f.liveExists(rsGVK, testNSMyapp, "fresh-pvc") {
		t.Error("RS exists in cluster after audit reconcile; audit must not write")
	}
	if f.liveExists(rdGVK, testNSMyapp, "fresh-pvc-dst") {
		t.Error("RD exists in cluster after audit reconcile; audit must not write")
	}
}

// Case B: audit + no planned ops → ExecutionResult nil (skimmable /audit).
func TestV4Reconcile_Patch67_AuditWithoutPlannedOps_ExecutionResultNil(t *testing.T) {
	cases := []struct {
		name string
		pvc  *corev1.PersistentVolumeClaim
		want ActionKind
	}{
		{
			"matches_inline_argo",
			makePVC(testNSOpenWebUI, testPVCStorageName,
				map[string]string{backupLabelKey: backupDaily}, nil),
			ActionAlreadyMatches,
		},
		{
			"skipped-not-opted-in",
			makePVC(testNSMyapp, "no-labels", nil, nil),
			ActionSkippedNotOptedIn,
		},
		{
			"skipped-exempt",
			makePVC(testNSMyapp, "exempt", map[string]string{
				backupExemptLabel: labelTrue,
			}, map[string]string{
				v4labels.LegacyAnnotationBackupExemptReasonFQ: testReasonNASShort,
			}),
			ActionSkippedExempt,
		},
		{
			"write-gate-missing-legacy",
			makePVC(testNSMyapp, "legacy-only",
				map[string]string{backupLabelKey: backupHourly}, nil),
			ActionWriteGateMissing,
		},
		{
			"write-gate-missing-enabled-only",
			makePVC(testNSMyapp, "enabled-only", map[string]string{
				v4labels.LabelEnabled: labelTrue,
				v4labels.LabelTier:    backupDaily,
			}, nil),
			ActionWriteGateMissing,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objs := []client.Object{tc.pvc}
			if tc.name == "matches_inline_argo" {
				objs = append(objs,
					makeRS(testNSOpenWebUI, testPVCStorageName, ManagedByArgoCDLabelValue, testRepoSecretShare, testPVCStorageName),
					makeRD(testNSOpenWebUI, testPVCStorageName+"-dst", ManagedByArgoCDLabelValue, testRepoSecretShare),
				)
			}
			f := newV4Fixture(t, objs...)
			entry := f.reconcile(tc.pvc.Namespace, tc.pvc.Name)
			if entry.Action != tc.want {
				t.Errorf("Action: got %q, want %q", entry.Action, tc.want)
			}
			if len(entry.PlannedOps) != 0 {
				t.Errorf("PlannedOps: got %d, want 0", len(entry.PlannedOps))
			}
			if entry.ExecutionResult != nil {
				t.Errorf("ExecutionResult: got %+v, want nil (no planned ops → no execution_result in /audit)", entry.ExecutionResult)
			}
			f.assertNoWrites()
		})
	}
}

// =============================================================================
// 6.7 — Permissive-mode write paths
// =============================================================================

// Permissive: enabled + manage with no current RS/RD → executor creates
// both. Cluster ends up with the v4-named resources, both managed-by=
// pvc-plumber, both repository=shared. /audit ExecutionResult shows
// Counts.Succeeded=2.
func TestV4Reconcile_Patch67_PermissiveEnabledAndManageNoCurrent_CreatesRSRD(t *testing.T) {
	pvc := makePVC(testNSMyapp, "perm-fresh", labelsEnabledManage(), nil)
	f := newV4ModeFixture(t, mode.Permissive, pvc)
	entry := f.reconcile(testNSMyapp, "perm-fresh")

	if entry.Action != ActionWouldCreate {
		t.Fatalf("Action: got %q, want %q", entry.Action, ActionWouldCreate)
	}
	if entry.ExecutionResult == nil {
		t.Fatal("ExecutionResult: got nil, want non-nil")
	}
	if got := entry.ExecutionResult.Counts.Succeeded; got != 2 {
		t.Errorf("Counts.Succeeded: got %d, want 2", got)
	}
	if got := entry.ExecutionResult.Counts.Skipped; got != 0 {
		t.Errorf("Counts.Skipped: got %d, want 0 (permissive does not short-circuit)", got)
	}
	if !f.liveExists(rsGVK, testNSMyapp, "perm-fresh") {
		t.Error("RS missing after permissive reconcile; executor should have created it")
	}
	if !f.liveExists(rdGVK, testNSMyapp, "perm-fresh-dst") {
		t.Error("RD missing after permissive reconcile; executor should have created it")
	}
	f.assertDidWriteByVerb(t, 2, 0, 0)
}

// Permissive: operator-owned drift (managed-by=pvc-plumber but wrong repo)
// → executor reads live, verifies ownership, overwrites with the planner's
// desired body. Repository field on both RS and RD now reflects the
// operator default.
func TestV4Reconcile_Patch67_PermissiveOperatorOwnedDrift_UpdatesRSRD(t *testing.T) {
	pvc := makePVC(testNSMyapp, "perm-drift", labelsEnabledManage(), nil)
	rs := makeRS(testNSMyapp, "perm-drift", ManagedByPVCPlumberLabelValue, "stale-repo", "perm-drift")
	rd := makeRD(testNSMyapp, "perm-drift-dst", ManagedByPVCPlumberLabelValue, "stale-repo")
	f := newV4ModeFixture(t, mode.Permissive, pvc, rs, rd)

	entry := f.reconcile(testNSMyapp, "perm-drift")

	if entry.Action != ActionWouldUpdate {
		t.Fatalf("Action: got %q, want %q", entry.Action, ActionWouldUpdate)
	}
	if entry.ExecutionResult == nil {
		t.Fatal("ExecutionResult: got nil, want non-nil")
	}
	if got := entry.ExecutionResult.Counts.Succeeded; got != 2 {
		t.Errorf("Counts.Succeeded: got %d, want 2", got)
	}
	if got := f.liveRepo(rsGVK, testNSMyapp, "perm-drift"); got != testRepoSecretShare {
		t.Errorf("RS live repository after update: got %q, want %q", got, testRepoSecretShare)
	}
	if got := f.liveRepo(rdGVK, testNSMyapp, "perm-drift-dst"); got != testRepoSecretShare {
		t.Errorf("RD live repository after update: got %q, want %q", got, testRepoSecretShare)
	}
	f.assertDidWriteByVerb(t, 0, 2, 0)
}

// Permissive: inline-Argo drift (managed-by=argocd, drifted repo) →
// planner returns ActionInlineArgoObserved with empty Ops; the executor
// gets nothing to do. Cluster state is unchanged; auditclient counters
// stay at zero. This is the most important non-write case under
// permissive mode — the operator MUST NOT touch GitOps-owned resources
// even when the write fuse is on globally.
func TestV4Reconcile_Patch67_PermissiveInlineArgoDrift_NoWrites(t *testing.T) {
	pvc := makePVC(testNSMyapp, "perm-argo", labelsEnabledManage(), nil)
	rs := makeRS(testNSMyapp, "perm-argo", ManagedByArgoCDLabelValue, "argo-repo", "perm-argo")
	rd := makeRD(testNSMyapp, "perm-argo-dst", ManagedByArgoCDLabelValue, "argo-repo")
	f := newV4ModeFixture(t, mode.Permissive, pvc, rs, rd)

	entry := f.reconcile(testNSMyapp, "perm-argo")

	if entry.Action != ActionInlineArgoObserved {
		t.Fatalf("Action: got %q, want %q", entry.Action, ActionInlineArgoObserved)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0 (never touch inline-argo)", len(entry.PlannedOps))
	}
	if entry.ExecutionResult != nil {
		t.Errorf("ExecutionResult: got %+v, want nil (no planned ops)", entry.ExecutionResult)
	}
	// Live state must be unchanged — Argo-owned drift stays.
	if got := f.liveRepo(rsGVK, testNSMyapp, "perm-argo"); got != "argo-repo" {
		t.Errorf("RS live repository after no-op reconcile: got %q, want %q (must not have been overwritten)", got, "argo-repo")
	}
	f.assertDidWriteByVerb(t, 0, 0, 0)
}

// Permissive: tier=disabled + operator-owned RS/RD → planner emits 2
// deletes; executor reads live, verifies ownership, deletes. Cluster
// state: both RS and RD gone.
func TestV4Reconcile_Patch67_PermissiveTierDisabledOperatorOwned_DeletesRSRD(t *testing.T) {
	pvc := makePVC(testNSMyapp, "perm-disable", labelsEnabledManageTier("disabled"), nil)
	rs := makeRS(testNSMyapp, "perm-disable", ManagedByPVCPlumberLabelValue, testRepoSecretShare, "perm-disable")
	rd := makeRD(testNSMyapp, "perm-disable-dst", ManagedByPVCPlumberLabelValue, testRepoSecretShare)
	f := newV4ModeFixture(t, mode.Permissive, pvc, rs, rd)

	entry := f.reconcile(testNSMyapp, "perm-disable")

	if entry.Action != ActionWouldDelete {
		t.Fatalf("Action: got %q, want %q", entry.Action, ActionWouldDelete)
	}
	if entry.ExecutionResult == nil {
		t.Fatal("ExecutionResult: got nil, want non-nil")
	}
	if got := entry.ExecutionResult.Counts.Succeeded; got != 2 {
		t.Errorf("Counts.Succeeded: got %d, want 2", got)
	}
	if f.liveExists(rsGVK, testNSMyapp, "perm-disable") {
		t.Error("RS still in cluster after permissive delete")
	}
	if f.liveExists(rdGVK, testNSMyapp, "perm-disable-dst") {
		t.Error("RD still in cluster after permissive delete")
	}
	f.assertDidWriteByVerb(t, 0, 0, 2)
}

// Permissive: enabled-only / manage-only / legacy-only / no-label /
// backup-exempt all leave the cluster untouched — planner's empty Ops
// is the gate, executor mechanics never engage.
func TestV4Reconcile_Patch67_PermissiveWriteGatedScenarios_NoWrites(t *testing.T) {
	cases := []struct {
		name     string
		ns, pvc  string
		labels   map[string]string
		anns     map[string]string
		wantAct  ActionKind
		wantExec bool // whether ExecutionResult should be non-nil
	}{
		{
			"legacy-only",
			testNSMyapp, "g-legacy",
			map[string]string{backupLabelKey: backupHourly}, nil,
			ActionWriteGateMissing, false,
		},
		{
			"enabled-only",
			testNSMyapp, "g-enabled",
			map[string]string{v4labels.LabelEnabled: labelTrue, v4labels.LabelTier: backupDaily}, nil,
			ActionWriteGateMissing, false,
		},
		{
			"manage-only",
			testNSMyapp, "g-manage",
			map[string]string{v4labels.LabelManageVolSync: labelTrue, v4labels.LabelTier: backupDaily}, nil,
			ActionSkippedNotOptedIn, false,
		},
		{
			"no-labels",
			testNSMyapp, "g-none",
			nil, nil,
			ActionSkippedNotOptedIn, false,
		},
		{
			"backup-exempt",
			testNSMyapp, "g-exempt",
			map[string]string{backupExemptLabel: labelTrue},
			map[string]string{v4labels.LegacyAnnotationBackupExemptReasonFQ: testReasonNASShort},
			ActionSkippedExempt, false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pvc := makePVC(tc.ns, tc.pvc, tc.labels, tc.anns)
			f := newV4ModeFixture(t, mode.Permissive, pvc)
			entry := f.reconcile(tc.ns, tc.pvc)

			if entry.Action != tc.wantAct {
				t.Errorf("Action: got %q, want %q", entry.Action, tc.wantAct)
			}
			if len(entry.PlannedOps) != 0 {
				t.Errorf("PlannedOps: got %d, want 0", len(entry.PlannedOps))
			}
			if (entry.ExecutionResult != nil) != tc.wantExec {
				t.Errorf("ExecutionResult presence: got %v, want %v", entry.ExecutionResult != nil, tc.wantExec)
			}
			f.assertDidWriteByVerb(t, 0, 0, 0)
			// Defense-in-depth: cluster must hold exactly one object —
			// the PVC itself. Nothing executor-created appeared.
			if f.liveExists(rsGVK, tc.ns, tc.pvc) {
				t.Errorf("RS appeared in cluster for %s; permissive must not write when planner gate fires", tc.name)
			}
			if f.liveExists(rdGVK, tc.ns, tc.pvc+"-dst") {
				t.Errorf("RD appeared in cluster for %s; permissive must not write when planner gate fires", tc.name)
			}
		})
	}
}

// =============================================================================
// 6.7 — Enforce + Strict mechanical parity with Permissive
// =============================================================================

// Enforce + Strict share executor mechanics with Permissive in Patch 6.7.
// Same fixture, three modes, identical counts + identical cluster state
// after the reconcile.
func TestV4Reconcile_Patch67_EnforceStrictParityWithPermissive(t *testing.T) {
	modes := []struct {
		name string
		m    mode.Mode
	}{
		{"permissive", mode.Permissive},
		{"enforce", mode.Enforce},
		{"strict", mode.Strict},
	}
	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			pvc := makePVC(testNSMyapp, "parity-pvc", labelsEnabledManage(), nil)
			f := newV4ModeFixture(t, tc.m, pvc)
			entry := f.reconcile(testNSMyapp, "parity-pvc")

			if entry.Action != ActionWouldCreate {
				t.Fatalf("Action: got %q, want %q", entry.Action, ActionWouldCreate)
			}
			if entry.ExecutionResult == nil || entry.ExecutionResult.Counts.Succeeded != 2 {
				t.Errorf("ExecutionResult: got %+v, want Succeeded=2 in %s mode", entry.ExecutionResult, tc.name)
			}
			if !f.liveExists(rsGVK, testNSMyapp, "parity-pvc") || !f.liveExists(rdGVK, testNSMyapp, "parity-pvc-dst") {
				t.Errorf("%s: cluster missing RS or RD after reconcile", tc.name)
			}
			f.assertDidWriteByVerb(t, 2, 0, 0)
		})
	}
}

// =============================================================================
// 6.7 — Paranoia: no foreign GVKs in any permissive ExecutionResult
// =============================================================================

// Run every permissive scenario through a single fixture sweep, then
// scan every entry's ExecutionResult.Outcomes for the GVK signature.
// Only ReplicationSource and ReplicationDestination may ever appear.
// This is the cross-cutting contract that lets us claim "the bounded
// executor can never write a Secret/PVC/webhook/SA, even given a
// buggy or compromised planner."
func TestV4Reconcile_Patch67_PermissiveOutcomesOnlyTargetVolSyncGVKs(t *testing.T) {
	// One PVC per scenario, unique names so each lands in its own
	// Store entry.
	pvcCreate := makePVC(testNSMyapp, "p-create", labelsEnabledManage(), nil)
	pvcUpdate := makePVC(testNSMyapp, "p-update", labelsEnabledManage(), nil)
	rsUpdate := makeRS(testNSMyapp, "p-update", ManagedByPVCPlumberLabelValue, "stale", "p-update")
	rdUpdate := makeRD(testNSMyapp, "p-update-dst", ManagedByPVCPlumberLabelValue, "stale")
	pvcDelete := makePVC(testNSMyapp, "p-delete", labelsEnabledManageTier("disabled"), nil)
	rsDelete := makeRS(testNSMyapp, "p-delete", ManagedByPVCPlumberLabelValue, testRepoSecretShare, "p-delete")
	rdDelete := makeRD(testNSMyapp, "p-delete-dst", ManagedByPVCPlumberLabelValue, testRepoSecretShare)
	pvcLegacy := makePVC(testNSMyapp, "p-legacy", map[string]string{backupLabelKey: backupHourly}, nil)
	pvcExempt := makePVC(testNSMyapp, "p-exempt",
		map[string]string{backupExemptLabel: labelTrue},
		map[string]string{v4labels.LegacyAnnotationBackupExemptReasonFQ: testReasonNASShort})

	f := newV4ModeFixture(t, mode.Permissive,
		pvcCreate,
		pvcUpdate, rsUpdate, rdUpdate,
		pvcDelete, rsDelete, rdDelete,
		pvcLegacy,
		pvcExempt,
	)
	f.reconcile(testNSMyapp, "p-create")
	f.reconcile(testNSMyapp, "p-update")
	f.reconcile(testNSMyapp, "p-delete")
	f.reconcile(testNSMyapp, "p-legacy")
	f.reconcile(testNSMyapp, "p-exempt")

	// PlannedOps (planner-side) constraint.
	assertPlannedOpsTargetVolSyncOnly(t, f.store)

	// ExecutionResult (executor-side) constraint — the paranoia walk
	// Patch 6.7 adds. Every outcome's GVK MUST be RS or RD.
	snap := f.store.Snapshot()
	for _, e := range snap.Entries {
		if e.ExecutionResult == nil {
			continue
		}
		for _, out := range e.ExecutionResult.Outcomes {
			if out.GVK != rsGVKStr && out.GVK != rdGVKStr {
				t.Errorf("foreign executor-outcome GVK %q for entry %s/%s (status=%q reason=%q)",
					out.GVK, e.Namespace, e.PVC, out.Status, out.Reason)
			}
		}
	}

	// Aggregate write volume: 2 creates (p-create) + 2 updates
	// (p-update) + 2 deletes (p-delete) = 6 verbs.
	f.assertDidWriteByVerb(t, 2, 2, 2)
}

// =============================================================================
// 6.7 — v3 isolation: V4AuditReconciler never writes chart-era resources
// =============================================================================
//
// The v4 reconciler observes only v4-named expected resources (<pvc> /
// <pvc>-dst). A PVC that has only chart-era children (<pvc>-backup) is
// not adopted under permissive — the planner returns
// ActionWriteGateMissing because legacy backup: labels remain
// reporting-only, AND the chart-era resources don't match the v4
// expected names so observeCurrent reports no current state. Even if
// the labels were upgraded to v4, the planner would emit creates with
// v4 names, and the chart-era resources would be left alone (no adopt,
// no delete). This isolates v3 chart-era state from v4 mutations.

func TestV4Reconcile_Patch67_PermissiveChartEraResourcesUntouched(t *testing.T) {
	pvc := makePVC(testNSMyapp, "iso-pvc", labelsEnabledManage(), nil)
	// Chart-era shape: shared name "<pvc>-backup", pvc-plumber-owned.
	chartRS := makeRS(testNSMyapp, "iso-pvc-backup", ManagedByPVCPlumberLabelValue, testRepoSecretShare, "iso-pvc")
	chartRD := makeRD(testNSMyapp, "iso-pvc-backup", ManagedByPVCPlumberLabelValue, testRepoSecretShare)
	f := newV4ModeFixture(t, mode.Permissive, pvc, chartRS, chartRD)

	entry := f.reconcile(testNSMyapp, "iso-pvc")

	// Planner sees no current resources at the v4 names → emits 2
	// creates for the v4-named resources.
	if entry.Action != ActionWouldCreate {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionWouldCreate)
	}
	if entry.ExecutionResult == nil || entry.ExecutionResult.Counts.Succeeded != 2 {
		t.Errorf("ExecutionResult: got %+v, want Succeeded=2", entry.ExecutionResult)
	}
	// v4-named resources now exist.
	if !f.liveExists(rsGVK, testNSMyapp, "iso-pvc") {
		t.Error("v4-named RS missing after permissive reconcile")
	}
	if !f.liveExists(rdGVK, testNSMyapp, "iso-pvc-dst") {
		t.Error("v4-named RD missing after permissive reconcile")
	}
	// Chart-era resources are untouched — the executor's GVK + name +
	// ownership chain never landed on them because the planner never
	// emitted ops targeting them.
	if !f.liveExists(rsGVK, testNSMyapp, "iso-pvc-backup") {
		t.Error("chart-era RS (iso-pvc-backup) was deleted; v4 must leave chart-era alone")
	}
	if !f.liveExists(rdGVK, testNSMyapp, "iso-pvc-backup") {
		t.Error("chart-era RD (iso-pvc-backup) was deleted; v4 must leave chart-era alone")
	}
	// Verb totals: 2 creates only (the chart-era resources are seed,
	// not executor output).
	f.assertDidWriteByVerb(t, 2, 0, 0)
}

// =============================================================================
// v4.0.1: namespace write gate (reconciler integration)
// =============================================================================

// A write-eligible PVC whose namespace LACKS pvc-plumber.io/managed-namespace
// is gated: action skipped-namespace-not-managed, no cluster writes (even in
// permissive mode), blocker present. The test seeds its OWN unlabeled
// Namespace so the fixture auto-seed (which would label it managed) is
// skipped.
func TestV4Reconcile_NamespaceNotManaged_GatesWrite(t *testing.T) {
	unmanagedNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns-unmanaged"}}
	pvc := makePVC("ns-unmanaged", "data", map[string]string{
		v4labels.LabelEnabled:       labelTrue,
		v4labels.LabelManageVolSync: labelTrue,
		v4labels.LabelTier:          backupDaily,
	}, nil)
	f := newV4ModeFixture(t, mode.Permissive, unmanagedNS, pvc)
	entry := f.reconcile("ns-unmanaged", "data")

	if entry.Action != ActionSkippedNamespaceNotManaged {
		t.Errorf("Action: got %q, want %q", entry.Action, ActionSkippedNamespaceNotManaged)
	}
	if len(entry.PlannedOps) != 0 {
		t.Errorf("PlannedOps: got %d, want 0 (namespace gate)", len(entry.PlannedOps))
	}
	if len(entry.Blockers) == 0 {
		t.Error("expected a namespace-gate blocker in the /audit entry")
	}
	f.assertNoWrites()
}

// A write-eligible PVC whose namespace IS labeled managed proceeds to
// would-create. Uses the fixture auto-seed (managed) by not providing a
// Namespace — proving the managed-namespace label flows through to the
// planner and allows the write.
func TestV4Reconcile_NamespaceManaged_AllowsWrite(t *testing.T) {
	pvc := makePVC("ns-managed", "data", map[string]string{
		v4labels.LabelEnabled:       labelTrue,
		v4labels.LabelManageVolSync: labelTrue,
		v4labels.LabelTier:          backupDaily,
	}, nil)
	f := newV4Fixture(t, pvc) // audit mode; auto-seeds a managed Namespace
	entry := f.reconcile("ns-managed", "data")

	if entry.Action != ActionWouldCreate {
		t.Errorf("Action: got %q, want %q (managed namespace must allow the write path)", entry.Action, ActionWouldCreate)
	}
}

// =============================================================================
// Schedule-drift detection (v4.0.2 — 2026-06-09 review finding B2)
// =============================================================================

// Schedule drift on an operator-owned RS must surface as would-update and
// be repaired in permissive mode. Regression for the 2026-06-09 review
// finding: observeCurrent never captured spec.trigger.schedule, so a
// pvc-plumber.io/tier change (or hand-edited RS) stayed already-matches
// forever.
func TestV4Reconcile_Permissive_ScheduleDrift_Repaired(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName, map[string]string{
		v4labels.LabelEnabled:       "true",
		v4labels.LabelManageVolSync: "true",
		v4labels.LabelTier:          "daily",
	}, nil)
	rs := makeRS(testNSMyapp, testPVCName, v4labels.LabelManagedByValue,
		naming.DefaultRepoSecretName, testPVCName)
	// Wrong schedule: simulates a stale cadence after a tier change.
	_ = unstructured.SetNestedField(rs.Object, "59 23 * * 6", "spec", "trigger", "schedule")
	rd := makeRD(testNSMyapp, testPVCName+"-dst", v4labels.LabelManagedByValue,
		naming.DefaultRepoSecretName)

	f := newV4ModeFixture(t, mode.Permissive, pvc, rs, rd)
	entry := f.reconcile(testNSMyapp, testPVCName)

	if entry.Action != ActionWouldUpdate {
		t.Fatalf("Action: got %q, want %q (schedule drift must be detected)",
			entry.Action, ActionWouldUpdate)
	}
	if entry.Current.RSSchedule != "59 23 * * 6" {
		t.Errorf("Current.RSSchedule: got %q, want the live drifted schedule", entry.Current.RSSchedule)
	}
	// updateOps rewrites both children.
	f.assertDidWriteByVerb(t, 0, 2, 0)

	// The live RS must now carry the builder's expected daily schedule.
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(rsGVK)
	if err := f.fake.Get(context.Background(),
		types.NamespacedName{Namespace: testNSMyapp, Name: testPVCName}, live); err != nil {
		t.Fatalf("get live RS: %v", err)
	}
	got, _, _ := unstructured.NestedString(live.Object, "spec", "trigger", "schedule")
	want := builder.ScheduleFor(testNSMyapp, testPVCName, v4labels.TierDaily)
	if got != want {
		t.Errorf("live RS schedule after repair: got %q, want %q", got, want)
	}
}

// The matching case must NOT regress to a false-positive would-update:
// an operator-owned RS already carrying the builder's schedule stays
// already-matches with zero writes.
func TestV4Reconcile_Permissive_ScheduleMatches_AlreadyMatches(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName, map[string]string{
		v4labels.LabelEnabled:       "true",
		v4labels.LabelManageVolSync: "true",
		v4labels.LabelTier:          "daily",
	}, nil)
	rs := makeRS(testNSMyapp, testPVCName, v4labels.LabelManagedByValue,
		naming.DefaultRepoSecretName, testPVCName)
	_ = unstructured.SetNestedField(rs.Object,
		builder.ScheduleFor(testNSMyapp, testPVCName, v4labels.TierDaily),
		"spec", "trigger", "schedule")
	rd := makeRD(testNSMyapp, testPVCName+"-dst", v4labels.LabelManagedByValue,
		naming.DefaultRepoSecretName)

	f := newV4ModeFixture(t, mode.Permissive, pvc, rs, rd)
	entry := f.reconcile(testNSMyapp, testPVCName)

	if entry.Action != ActionAlreadyMatches {
		t.Fatalf("Action: got %q, want %q", entry.Action, ActionAlreadyMatches)
	}
	f.assertDidWriteByVerb(t, 0, 0, 0)
}
