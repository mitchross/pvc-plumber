package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// Test fixture constants. Centralized so the table-driven tests, the
// per-helper assertions, and the schedule-formula sanity checks all line up
// on the same canonical values. Keeping them as consts also satisfies the
// goconst linter, which counts repeated literals across the whole package.
const (
	// testNamespace is the canonical namespace every reconcile happy-path
	// test runs in. mustExist used to take a namespace parameter that
	// always received this value (unparam); the parameter was dropped.
	testNamespace = "app"

	// testPVCName is the canonical backup-labeled PVC name used across
	// the reconcile and cleanup tests.
	testPVCName = "data"

	// testRSDestName is the conventional <pvc>-backup name pvc-plumber
	// gives the companion ReplicationSource AND ReplicationDestination
	// objects. Computed once instead of pasting "data-backup" everywhere.
	testRSDestName = testPVCName + "-backup"

	// testESName is the conventional volsync-<pvc> name pvc-plumber
	// gives the companion ExternalSecret.
	testESName = "volsync-" + testPVCName
)

// newTestScheme builds a scheme that knows both core/v1 and the unstructured
// child kinds. The fake client refuses to operate on a GVK its scheme has
// never heard of, so we register the singular + List variants for each
// child here.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	sch := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(sch); err != nil {
		t.Fatalf("add corev1 to scheme: %v", err)
	}
	for _, gvk := range childGVKs {
		sch.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		listGVK := schema.GroupVersionKind{
			Group:   gvk.Group,
			Version: gvk.Version,
			Kind:    gvk.Kind + "List",
		}
		sch.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	}
	return sch
}

// childObject is a helper to build a labeled unstructured child for cleanup
// tests. Always sets the pvcLabel so cleanup's selector matches.
func childObject(gvk schema.GroupVersionKind, namespace, name, pvcName string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(namespace)
	u.SetName(name)
	u.SetLabels(map[string]string{
		managedByLabel: managedByValue,
		pvcLabel:       pvcName,
	})
	return u
}

func TestBackupSchedule_HourlyDaily(t *testing.T) {
	// Expected values are the SHA256-derived minutes:
	//   minute = uint32(big-endian first 4 bytes of sha256(ns + "/" + pvc)) % 60
	// Pre-computed locally; if the formula or separator ever drifts these
	// will fail loud. v3 spec § "SHA256 schedule formula" is the source of
	// truth — see /home/vanillax/programming/talos-argocd-proxmox/docs/plans/pvc-plumber-v3-roadmap.md.
	cases := []struct {
		ns       string
		pvc      string
		wantHour string
		wantDay  string
	}{
		// SHA256-based schedule (v2.1+). Each expected minute is the first 4
		// big-endian bytes of sha256(ns + "/" + pvcName) mod 60. See v3 roadmap
		// §"Resolved questions". testPVCName == "data" (lint-extracted constant).
		// sha256("ns/pvc") → 26
		{ns: "ns", pvc: "pvc", wantHour: "26 * * * *", wantDay: "26 2 * * *"},
		// sha256("default/data") → 3
		{ns: "default", pvc: testPVCName, wantHour: "3 * * * *", wantDay: "3 2 * * *"},
		// sha256("karakeep/data-pvc") → 10 — picked from the v3 spec's
		// own example PVC name to anchor the formula here.
		{ns: "karakeep", pvc: "data-pvc", wantHour: "10 * * * *", wantDay: "10 2 * * *"},
		// Distinct from the equivalent length-mod result (60-char joined
		// string used to land on minute 0 under the deprecated formula).
		// SHA256 returns 33 here — also serves as a regression pin against
		// accidental reversion to length-mod.
		{
			ns:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // 30
			pvc:      "bbbbbbbbbbbbbbbbbbbbbbbbbbbbb",  // 29
			wantHour: "33 * * * *",
			wantDay:  "33 2 * * *",
		},
	}
	for _, c := range cases {
		gotHourly := backupSchedule(c.ns, c.pvc, "hourly")
		if gotHourly != c.wantHour {
			t.Errorf("hourly schedule for %s/%s: got %q, want %q", c.ns, c.pvc, gotHourly, c.wantHour)
		}
		gotDaily := backupSchedule(c.ns, c.pvc, "daily")
		if gotDaily != c.wantDay {
			t.Errorf("daily schedule for %s/%s: got %q, want %q", c.ns, c.pvc, gotDaily, c.wantDay)
		}
	}

	// Pin against accidental reversion to the previous length-mod scheme.
	// Old formula: len("a-b") % 60 = 3 → "3 * * * *".
	// New formula: sha256("a/b") first 4 BE bytes % 60 = 52 → "52 * * * *".
	// If anyone "simplifies" backupSchedule back to length-mod this fires.
	if got := backupSchedule("a", "b", "hourly"); got != "52 * * * *" {
		t.Errorf("backupSchedule must use SHA256(ns + \"/\" + pvc); got %q (regression: length-mod scheme)", got)
	}
}

// TestBackupSchedule_DistributionIsUniform asserts the SHA256 derivation
// distributes minute values close to uniformly across [0,60). 1000 synthetic
// PVC keys are hashed; if the function ever silently regresses to length-mod
// (or another distribution-clustering scheme) variance spikes far past the
// thresholds below.
//
// Math: 1000 keys / 60 buckets ≈ 16.67 expected per bucket. For a uniform
// distribution the per-bucket count follows Binomial(n=1000, p=1/60), which
// has mean 16.67 and stddev ≈ 4.05. We pin "no bucket is empty" (anything
// short of catastrophically clustered passes) and "no bucket is overfull"
// (>40 — that's >5 stddev above the mean, statistically implausible for any
// reasonable hash but trivially achievable by length-mod when names share
// length).
func TestBackupSchedule_DistributionIsUniform(t *testing.T) {
	const total = 1000
	buckets := make(map[int]int, 60)
	for i := 0; i < total; i++ {
		ns := fmt.Sprintf("ns-%d", i)
		pvc := fmt.Sprintf("pvc-%d", i)
		s := backupSchedule(ns, pvc, "hourly")
		var minute int
		// Schedules look like "<minute> * * * *"; parse the first int.
		if _, err := fmt.Sscanf(s, "%d", &minute); err != nil {
			t.Fatalf("parse minute from %q: %v", s, err)
		}
		if minute < 0 || minute >= 60 {
			t.Fatalf("minute %d out of [0,60) for %s/%s", minute, ns, pvc)
		}
		buckets[minute]++
	}
	if len(buckets) < 60 {
		// Length-mod over names of varying length still hits all 60
		// buckets if the names span enough lengths, but our generator
		// produces names whose length varies in a tightly bounded range.
		// Under length-mod many of the 60 buckets would be empty here.
		t.Errorf("only %d/60 minute buckets populated — distribution looks clustered", len(buckets))
	}
	// Per-bucket bounds. Tightening these much further would risk flake
	// even though the underlying RNG (SHA256) is deterministic — they're
	// pinned generously to leave headroom for any future input change.
	const minPerBucket = 3
	const maxPerBucket = 40
	for m := 0; m < 60; m++ {
		count := buckets[m]
		if count < minPerBucket {
			t.Errorf("minute %d had %d hits (< %d); distribution likely regressed to length-mod or worse", m, count, minPerBucket)
		}
		if count > maxPerBucket {
			t.Errorf("minute %d had %d hits (> %d); distribution likely regressed to length-mod or worse", m, count, maxPerBucket)
		}
	}
}

func TestCleanup_DeletesAllByLabel(t *testing.T) {
	scheme := newTestScheme(t)
	ns := testNamespace
	pvcName := testPVCName

	es := childObject(esGVK, ns, testESName, pvcName)
	rs := childObject(rsGVK, ns, testRSDestName, pvcName)
	rd := childObject(rdGVK, ns, testRSDestName, pvcName)
	// Decoy in another namespace and another pvcName — must NOT be deleted.
	decoyNS := childObject(esGVK, "other", testESName, pvcName)
	decoyName := childObject(esGVK, ns, "volsync-other", "other")

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(es, rs, rd, decoyNS, decoyName).
		Build()
	r := &PVCReconciler{Client: cli}

	if err := r.cleanup(context.Background(), ns, pvcName); err != nil {
		t.Fatalf("cleanup returned error: %v", err)
	}

	// The three labeled siblings should be gone.
	for _, want := range []struct {
		gvk  schema.GroupVersionKind
		name string
	}{
		{esGVK, testESName},
		{rsGVK, testRSDestName},
		{rdGVK, testRSDestName},
	} {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(want.gvk)
		err := cli.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: want.name}, got)
		if !apierrors.IsNotFound(err) {
			t.Errorf("expected %s %s/%s to be deleted, got err=%v", want.gvk.Kind, ns, want.name, err)
		}
	}

	// The decoys must survive.
	gotDecoyNS := &unstructured.Unstructured{}
	gotDecoyNS.SetGroupVersionKind(esGVK)
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: "other", Name: testESName}, gotDecoyNS); err != nil {
		t.Errorf("cross-namespace decoy was deleted: %v", err)
	}
	gotDecoyName := &unstructured.Unstructured{}
	gotDecoyName.SetGroupVersionKind(esGVK)
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "volsync-other"}, gotDecoyName); err != nil {
		t.Errorf("cross-pvc decoy was deleted: %v", err)
	}
}

func TestCleanup_IgnoresNotFound(t *testing.T) {
	scheme := newTestScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &PVCReconciler{Client: cli}

	// Empty cluster, nothing to delete — must be a clean no-op.
	if err := r.cleanup(context.Background(), "any", "missing"); err != nil {
		t.Fatalf("cleanup on empty cluster errored: %v", err)
	}
}

// noMatchListClient wraps a fake client and forces every unstructured List
// to return *meta.NoKindMatchError — the error class controller-runtime's
// REST mapper produces when a GVK has no CRD installed in the cluster.
// Building a real fake client with a deliberately-incomplete scheme would
// either panic or surface a different error (the fake client's behavior
// for unregistered GVKs is internal-implementation territory we don't want
// to lean on); injecting the exact production error keeps the test
// pinned to the contract `cleanup` actually has to honor.
type noMatchListClient struct {
	client.Client
}

func (c *noMatchListClient) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	u, ok := list.(*unstructured.UnstructuredList)
	if !ok {
		return nil
	}
	gvk := u.GroupVersionKind()
	kind := strings.TrimSuffix(gvk.Kind, "List")
	return &meta.NoKindMatchError{
		GroupKind:        schema.GroupKind{Group: gvk.Group, Kind: kind},
		SearchedVersions: []string{gvk.Version},
	}
}

// TestCleanup_IgnoresMissingCRD verifies the orphan reaper does not
// infinite-requeue a backup-labeled PVC during bootstrap or in dev/test
// clusters that lack the VolSync / external-secrets CRDs. With no CRD
// registered, the REST mapper returns *meta.NoKindMatchError on List —
// `cleanup` must swallow that and return nil.
func TestCleanup_IgnoresMissingCRD(t *testing.T) {
	scheme := newTestScheme(t)
	inner := fake.NewClientBuilder().WithScheme(scheme).Build()
	wrapper := &noMatchListClient{Client: inner}

	// Sanity-check: confirm meta.IsNoMatchError actually classifies the
	// wrapper's error — if k8s.io/apimachinery ever changed the type
	// surface this test fails loud rather than silent.
	probeErr := wrapper.List(context.Background(),
		func() *unstructured.UnstructuredList {
			l := &unstructured.UnstructuredList{}
			l.SetGroupVersionKind(rsGVK)
			return l
		}(),
	)
	if !meta.IsNoMatchError(probeErr) {
		t.Fatalf("test setup is wrong: wrapper error %T %v is not classified as NoMatchError", probeErr, probeErr)
	}

	r := &PVCReconciler{Client: wrapper}
	if err := r.cleanup(context.Background(), testNamespace, testPVCName); err != nil {
		t.Fatalf("cleanup on cluster with missing CRDs errored: %v (want nil — bootstrap path)", err)
	}
}

// labeledPVC builds a backup-labeled PVC with optional bind state and age.
// The namespace parameter was dropped after every call site converged on
// testNamespace (unparam lint).
func labeledPVC(label string, phase corev1.PersistentVolumeClaimPhase, age time.Duration) *corev1.PersistentVolumeClaim {
	created := metav1.NewTime(time.Now().Add(-age))
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         testNamespace,
			Name:              testPVCName,
			Labels:            map[string]string{backupLabelKey: label},
			CreationTimestamp: created,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: phase},
	}
	return pvc
}

func TestReconcile_NotBound_Requeues30s(t *testing.T) {
	scheme := newTestScheme(t)
	pvc := labeledPVC("hourly", corev1.ClaimPending, 0)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc).
		Build()
	r := &PVCReconciler{Client: cli}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testPVCName}})
	if err != nil {
		t.Fatalf("reconcile errored: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Errorf("RequeueAfter = %v, want 30s", res.RequeueAfter)
	}

	// ES + RD should have been created even though the PVC isn't bound;
	// only RS is gated on Bound+age.
	mustExist(t, cli, esGVK, testESName)
	mustExist(t, cli, rdGVK, testRSDestName)
	mustNotExist(t, cli, rsGVK, testRSDestName)
}

func TestReconcile_BoundYoung_RequeuesUntilOld(t *testing.T) {
	scheme := newTestScheme(t)
	// Bound but only 30 minutes old → must wait another ~90m before backup.
	pvc := labeledPVC("daily", corev1.ClaimBound, 30*time.Minute)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc).
		Build()
	r := &PVCReconciler{Client: cli}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testPVCName}})
	if err != nil {
		t.Fatalf("reconcile errored: %v", err)
	}
	// Expected RequeueAfter ≈ 1h30m. Allow a few seconds of slop for test
	// runtime between the PVC's CreationTimestamp and time.Now() inside
	// Reconcile.
	expected := 90 * time.Minute
	if delta := absDuration(res.RequeueAfter - expected); delta > 5*time.Second {
		t.Errorf("RequeueAfter = %v, want ≈ %v (delta %v)", res.RequeueAfter, expected, delta)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("RequeueAfter must be positive, got %v", res.RequeueAfter)
	}

	// ES + RD must exist; RS must not (still in age gate).
	mustExist(t, cli, esGVK, testESName)
	mustExist(t, cli, rdGVK, testRSDestName)
	mustNotExist(t, cli, rsGVK, testRSDestName)
}

func TestReconcile_BoundOld_CreatesAllThree(t *testing.T) {
	scheme := newTestScheme(t)
	pvc := labeledPVC("hourly", corev1.ClaimBound, 3*time.Hour)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc).
		Build()
	r := &PVCReconciler{Client: cli}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testPVCName}})
	if err != nil {
		t.Fatalf("reconcile errored: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0 (steady state)", res.RequeueAfter)
	}

	mustExist(t, cli, esGVK, testESName)
	mustExist(t, cli, rdGVK, testRSDestName)
	mustExist(t, cli, rsGVK, testRSDestName)

	// Spot-check labels and a representative spec field on the RS, since
	// that's the resource whose schedule is the trickiest port.
	rs := &unstructured.Unstructured{}
	rs.SetGroupVersionKind(rsGVK)
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testRSDestName}, rs); err != nil {
		t.Fatalf("get RS: %v", err)
	}
	if v := rs.GetLabels()[managedByLabel]; v != managedByValue {
		t.Errorf("RS missing managed-by label, got %q", v)
	}
	if v := rs.GetLabels()[pvcLabel]; v != testPVCName {
		t.Errorf("RS missing pvc label, got %q", v)
	}
	schedule, _, err := unstructured.NestedString(rs.Object, "spec", "trigger", "schedule")
	if err != nil {
		t.Fatalf("read schedule: %v", err)
	}
	// sha256("app/data") first 4 BE bytes mod 60 = 58, hourly.
	if schedule != "58 * * * *" {
		t.Errorf("RS schedule = %q, want %q", schedule, "58 * * * *")
	}
}

// mustExist fails the test if a child of the given GVK is absent in
// testNamespace. The namespace parameter was dropped after every call site
// converged on testNamespace ("app") — unparam linter.
func mustExist(t *testing.T, cli client.Client, gvk schema.GroupVersionKind, name string) {
	t.Helper()
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvk)
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: name}, got); err != nil {
		t.Errorf("expected %s %s/%s to exist, got err=%v", gvk.Kind, testNamespace, name, err)
	}
}

// mustNotExist fails if the object is present in testNamespace.
func mustNotExist(t *testing.T, cli client.Client, gvk schema.GroupVersionKind, name string) {
	t.Helper()
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvk)
	err := cli.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: name}, got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected %s %s/%s to be absent, got err=%v", gvk.Kind, testNamespace, name, err)
	}
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// TestReconcile_BackupExempt_TriggersCleanup covers the "user added
// backup-exempt=true to a previously-backed-up PVC" transition. The
// reconciler must reap any managed-by children even though the PVC still
// carries the original `backup: hourly|daily` label — exempt is the
// override.
func TestReconcile_BackupExempt_TriggersCleanup(t *testing.T) {
	scheme := newTestScheme(t)
	// PVC is backup-labeled (would normally produce children) AND
	// backup-exempt=true (override → cleanup). Bound and old enough that
	// the non-exempt path would have created all three children.
	pvc := labeledPVC("hourly", corev1.ClaimBound, 3*time.Hour)
	pvc.Labels[backupExemptLabel] = "true"

	// Pre-existing children labeled by pvc-plumber from before exempt was
	// added. Reconcile must reap them.
	es := childObject(esGVK, "app", "volsync-data", "data")
	rs := childObject(rsGVK, "app", "data-backup", "data")
	rd := childObject(rdGVK, "app", "data-backup", "data")

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, es, rs, rd).
		Build()
	r := &PVCReconciler{Client: cli}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app", Name: "data"}})
	if err != nil {
		t.Fatalf("reconcile errored: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0 (exempt → cleanup → done)", res.RequeueAfter)
	}

	// All three children must have been reaped.
	mustNotExist(t, cli, esGVK, "volsync-data")
	mustNotExist(t, cli, rsGVK, "data-backup")
	mustNotExist(t, cli, rdGVK, "data-backup")
}
