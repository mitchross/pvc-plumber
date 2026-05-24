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

func TestV4Reconcile_LegacyBackupHourly_NoCurrent_WouldCreate(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName, map[string]string{backupLabelKey: backupHourly}, nil)
	f := newV4Fixture(t, pvc)

	entry := f.reconcile(testNSMyapp, testPVCName)
	if entry.Action != ActionWouldCreate {
		t.Errorf("legacy backup: hourly + no current: got %q, want %q", entry.Action, ActionWouldCreate)
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

func TestV4Reconcile_V4Label_NoCurrentResources_WouldCreate(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName,
		map[string]string{
			v4labels.LabelEnabled: labelTrue,
			v4labels.LabelTier:    backupHourly,
		},
		nil)
	f := newV4Fixture(t, pvc)

	entry := f.reconcile(testNSMyapp, testPVCName)
	if entry.Action != ActionWouldCreate {
		t.Errorf("v4 + no current: got %q, want %q", entry.Action, ActionWouldCreate)
	}
	if entry.Owner != OwnerNone {
		t.Errorf("Owner: got %q, want %q", entry.Owner, OwnerNone)
	}
	if entry.Current.RSPresent || entry.Current.RDPresent {
		t.Errorf("Current should be empty: %+v", entry.Current)
	}
	f.assertNoWrites()
}

// =============================================================================
// Phase 5 contract: chart-era <pvc>-backup names are NOT the v4 expected.
// =============================================================================

// PVC has backup: daily label. Only chart-era resources exist
// (RS named "data-backup", RD named "data-backup"). The v4 expected
// is RS=data, RD=data-dst — chart-era names don't satisfy that
// expectation. Verdict: would-create (since OwnerNone — neither chart-era
// resource matches the bare-dst names we query).
func TestV4Reconcile_OnlyChartEraNamesPresent_WouldCreate(t *testing.T) {
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
	if entry.Action != ActionWouldCreate {
		t.Errorf("chart-era only: got %q, want %q", entry.Action, ActionWouldCreate)
	}
	if entry.Owner != OwnerNone {
		t.Errorf("Owner: got %q, want %q (the chart-era resources don't have v4 names)", entry.Owner, OwnerNone)
	}
	if entry.Expected.RDName == testPVCName+"-backup" {
		t.Errorf("Expected.RDName must not be <pvc>-backup; got %q", entry.Expected.RDName)
	}
	f.assertNoWrites()
}

// =============================================================================
// Phase 5 contract item #3: backup-exempt → skipped-exempt
// =============================================================================

func TestV4Reconcile_BackupExempt_SkippedExempt(t *testing.T) {
	pvc := makePVC("comfyui", "comfyui-storage",
		map[string]string{backupExemptLabel: labelTrue},
		map[string]string{v4labels.LegacyAnnotationBackupExemptReasonFQ: "NAS-backed, non-snapshottable"},
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
		map[string]string{v4labels.LegacyAnnotationBackupExemptReasonFQ: "NAS"},
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

// PVC is opted in. A managed-by=pvc-plumber RS exists for the EXPECTED
// name (`data`) but the spec drifts (different repository). DecideAction
// classifies this as would-update (operator-owned drift), NOT would-delete.
// This is the typical "v3 reconciler created resources, then schedule
// formula changed" scenario.
func TestV4Reconcile_ManagedByPVCPlumber_ShapeDrifts_WouldUpdate(t *testing.T) {
	pvc := makePVC(testNSMyapp, testPVCName,
		map[string]string{v4labels.LabelEnabled: labelTrue},
		nil)
	rs := makeRS(testNSMyapp, testPVCName, ManagedByPVCPlumberLabelValue, "wrong-repo", testPVCName)
	rd := makeRD(testNSMyapp, testPVCName+"-dst", ManagedByPVCPlumberLabelValue, "wrong-repo")

	f := newV4Fixture(t, pvc, rs, rd)
	entry := f.reconcile(testNSMyapp, testPVCName)

	if entry.Action != ActionWouldUpdate {
		t.Errorf("pvc-plumber drift: got %q, want %q", entry.Action, ActionWouldUpdate)
	}
	if entry.Owner != OwnerPVCPlumber {
		t.Errorf("Owner: got %q, want %q", entry.Owner, OwnerPVCPlumber)
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
		{"a", "p1", ActionWouldCreate, LabelSourceLegacy},
		{"b", "p2", ActionWouldCreate, LabelSourceV4},
		{"c", "p3", ActionSkippedNotOptedIn, LabelSourceNone},
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
			map[string]string{v4labels.LegacyAnnotationBackupExemptReasonFQ: "NAS"}),
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
