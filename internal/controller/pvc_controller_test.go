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

	// testStoreName / testVaultKey / testKopiaPwProp pin the ES rendering
	// fixture values so goconst doesn't complain when they appear in both
	// the ExternalSecretConfig builder and the assertion expressions.
	testStoreName    = "1password"
	testVaultKey     = "rustfs"
	testKopiaPwProp  = "kopia_password"
	testS3AccessProp = "k8s-admin-access-key"
	testS3SecretProp = "k8s-admin-secret-key"
	testS3Endpoint   = "http://192.168.10.133:30293"
	testS3Bucket     = "volsync-kopia"
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

// testExternalSecretConfig returns a representative ExternalSecretConfig for
// tests. Mirrors what cmd/operator/main.go produces from default env vars on
// the reference cluster. Centralizing the literals here keeps the new ES
// shape assertions in TestEnsureExternalSecret_RendersS3Shape readable
// without inlining the same six strings every test.
func testExternalSecretConfig() ExternalSecretConfig {
	return ExternalSecretConfig{
		SecretStoreName:       testStoreName,
		VaultKey:              testVaultKey,
		KopiaPasswordProperty: testKopiaPwProp,
		S3AccessKeyProperty:   testS3AccessProp,
		S3SecretKeyProperty:   testS3SecretProp,
		S3Endpoint:            testS3Endpoint,
		S3Bucket:              testS3Bucket,
		S3DisableTLS:          true,
	}
}

// newTestReconciler wraps the fake-client + ExternalSecretConfig boilerplate
// so the call sites in this file all get the same default rendering knobs.
// Tests that need a different config can construct a PVCReconciler by hand.
func newTestReconciler(cli client.Client) *PVCReconciler {
	return &PVCReconciler{Client: cli, ExternalSecret: testExternalSecretConfig()}
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
	r := newTestReconciler(cli)

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
	r := newTestReconciler(cli)

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

	r := newTestReconciler(wrapper)
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
	r := newTestReconciler(cli)

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
	r := newTestReconciler(cli)

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
	r := newTestReconciler(cli)

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
	pvc.Labels[backupExemptLabel] = labelTrue

	// Pre-existing children labeled by pvc-plumber from before exempt was
	// added. Reconcile must reap them.
	es := childObject(esGVK, testNamespace, testESName, testPVCName)
	rs := childObject(rsGVK, testNamespace, testRSDestName, testPVCName)
	rd := childObject(rdGVK, testNamespace, testRSDestName, testPVCName)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, es, rs, rd).
		Build()
	r := newTestReconciler(cli)

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testPVCName}})
	if err != nil {
		t.Fatalf("reconcile errored: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0 (exempt → cleanup → done)", res.RequeueAfter)
	}

	// All three children must have been reaped.
	mustNotExist(t, cli, esGVK, testESName)
	mustNotExist(t, cli, rsGVK, testRSDestName)
	mustNotExist(t, cli, rdGVK, testRSDestName)
}

// TestEnsureExternalSecret_RendersS3Shape pins the v3.0.0 ES template:
//   - secretStoreRef is configurable (was hardcoded `1password`).
//   - target.template.data carries the four S3 env vars (KOPIA_REPOSITORY=
//     s3://<bucket>, KOPIA_S3_ENDPOINT, KOPIA_S3_BUCKET, KOPIA_S3_DISABLE_TLS).
//   - data[] carries three remoteRefs (KOPIA_PASSWORD, AWS_ACCESS_KEY_ID,
//     AWS_SECRET_ACCESS_KEY), each pointing at the configurable vault key
//     and configurable per-property name.
//
// Regression catch: the v2.x shape (filesystem:///repository, single
// KOPIA_PASSWORD entry) MUST NOT appear anywhere in the rendered object.
func TestEnsureExternalSecret_RendersS3Shape(t *testing.T) {
	scheme := newTestScheme(t)
	pvc := labeledPVC("hourly", corev1.ClaimBound, 3*time.Hour)
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()
	r := newTestReconciler(cli)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testPVCName}}); err != nil {
		t.Fatalf("reconcile errored: %v", err)
	}

	es := &unstructured.Unstructured{}
	es.SetGroupVersionKind(esGVK)
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testESName}, es); err != nil {
		t.Fatalf("get ES: %v", err)
	}

	// secretStoreRef.name picks up the configurable store name.
	storeName, _, err := unstructured.NestedString(es.Object, "spec", "secretStoreRef", "name")
	if err != nil {
		t.Fatalf("read secretStoreRef.name: %v", err)
	}
	if storeName != "1password" {
		t.Errorf("secretStoreRef.name = %q, want %q", storeName, "1password")
	}

	// deletionPolicy must be Retain so a deleted ES doesn't drag the rendered
	// Secret with it (mover Jobs may still need it).
	dp, _, err := unstructured.NestedString(es.Object, "spec", "target", "deletionPolicy")
	if err != nil {
		t.Fatalf("read deletionPolicy: %v", err)
	}
	if dp != "Retain" {
		t.Errorf("deletionPolicy = %q, want %q", dp, "Retain")
	}

	tmplData, _, err := unstructured.NestedStringMap(es.Object, "spec", "target", "template", "data")
	if err != nil {
		t.Fatalf("read template.data: %v", err)
	}
	wantTmpl := map[string]string{
		kopiaEnvRepository:   "s3://" + testS3Bucket,
		kopiaEnvS3Endpoint:   testS3Endpoint,
		kopiaEnvS3Bucket:     testS3Bucket,
		kopiaEnvS3DisableTLS: labelTrue,
	}
	for k, want := range wantTmpl {
		if got := tmplData[k]; got != want {
			t.Errorf("template.data[%q] = %q, want %q", k, got, want)
		}
	}
	// Regression: the legacy filesystem keys must be gone.
	for _, banned := range []string{"KOPIA_FS_PATH"} {
		if _, ok := tmplData[banned]; ok {
			t.Errorf("template.data must not contain legacy key %q (v2.x → v3 migration)", banned)
		}
	}
	if strings.Contains(tmplData[kopiaEnvRepository], "filesystem://") {
		t.Errorf("KOPIA_REPOSITORY = %q, must not start with filesystem://", tmplData[kopiaEnvRepository])
	}

	// data[] should carry exactly three remoteRefs in stable order.
	dataList, _, err := unstructured.NestedSlice(es.Object, "spec", dataField)
	if err != nil {
		t.Fatalf("read data: %v", err)
	}
	if len(dataList) != 3 {
		t.Fatalf("spec.data length = %d, want 3", len(dataList))
	}

	wantRefs := []struct {
		secretKey string
		property  string
	}{
		{kopiaEnvPassword, testKopiaPwProp},
		{awsEnvAccessKeyID, testS3AccessProp},
		{awsEnvSecretAccessKey, testS3SecretProp},
	}
	for i, want := range wantRefs {
		entry, ok := dataList[i].(map[string]interface{})
		if !ok {
			t.Fatalf("data[%d] is not a map: %T", i, dataList[i])
		}
		if got, _ := entry[esFieldSecretKey].(string); got != want.secretKey {
			t.Errorf("data[%d].secretKey = %q, want %q", i, got, want.secretKey)
		}
		ref, ok := entry[esFieldRemoteRef].(map[string]interface{})
		if !ok {
			t.Fatalf("data[%d].remoteRef is not a map", i)
		}
		if got, _ := ref[esFieldKey].(string); got != testVaultKey {
			t.Errorf("data[%d].remoteRef.key = %q, want %q", i, got, testVaultKey)
		}
		if got, _ := ref[esFieldProperty].(string); got != want.property {
			t.Errorf("data[%d].remoteRef.property = %q, want %q", i, got, want.property)
		}
	}
}

// TestEnsureExternalSecret_RecyclesLegacyFilesystemShape pins the one-time
// v2 → v3 migration: a pre-existing ES with KOPIA_REPOSITORY=filesystem:///…
// must be deleted and recreated in the new S3 shape on the next reconcile.
// This is the ONLY drift correction the reconciler performs.
func TestEnsureExternalSecret_RecyclesLegacyFilesystemShape(t *testing.T) {
	scheme := newTestScheme(t)
	pvc := labeledPVC("hourly", corev1.ClaimBound, 3*time.Hour)

	// Pre-existing legacy ES, mimicking the v2.x rendered shape.
	legacy := childObject(esGVK, testNamespace, testESName, testPVCName)
	legacy.Object["spec"] = map[string]interface{}{
		esFieldRefreshInterval: "1h",
		"secretStoreRef": map[string]interface{}{
			"kind":      "ClusterSecretStore",
			esFieldName: testStoreName,
		},
		esFieldTarget: map[string]interface{}{
			esFieldName:           testESName,
			esFieldCreationPolicy: creationPolicyOwner,
			esFieldTemplate: map[string]interface{}{
				"engineVersion": "v2",
				"mergePolicy":   "Merge",
				dataField: map[string]interface{}{
					kopiaEnvRepository: "filesystem:///repository",
					"KOPIA_FS_PATH":    "/repository",
				},
			},
		},
		dataField: []interface{}{
			esRemoteRef(kopiaEnvPassword, testVaultKey, testKopiaPwProp),
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc, legacy).Build()
	r := newTestReconciler(cli)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testPVCName}}); err != nil {
		t.Fatalf("reconcile errored: %v", err)
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(esGVK)
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testESName}, got); err != nil {
		t.Fatalf("get ES: %v", err)
	}
	repo, _, err := unstructured.NestedString(got.Object, "spec", esFieldTarget, esFieldTemplate, dataField, kopiaEnvRepository)
	if err != nil {
		t.Fatalf("read KOPIA_REPOSITORY: %v", err)
	}
	if !strings.HasPrefix(repo, "s3://") {
		t.Errorf("after recycle: KOPIA_REPOSITORY = %q, want prefix %q (legacy ES not recycled)", repo, "s3://")
	}

	// data[] now has the three S3-shape entries.
	dataList, _, err := unstructured.NestedSlice(got.Object, "spec", dataField)
	if err != nil {
		t.Fatalf("read data: %v", err)
	}
	if len(dataList) != 3 {
		t.Errorf("after recycle: spec.data len = %d, want 3", len(dataList))
	}
}

// promStackNS is the system namespace used in the long-name short-circuit
// test. Constant so goconst doesn't flag the four occurrences inside
// TestReconcile_SystemNamespace_ShortCircuits.
const promStackNS = "prometheus-stack"

// TestReconcile_SystemNamespace_ShortCircuits pins the v3.1.0 invariant:
// a PVC in any system namespace must be skipped at the very top of
// Reconcile, BEFORE any cleanup/Get/List call. This is the primary fix
// for the 2026-05-08 prometheus-stack incident — the long-named monitoring
// PVCs (>63 bytes) are in `prometheus-stack`, which IS in the operator's
// SystemNamespaces set, but the pre-v3.1.0 reconciler reached cleanup()
// before the system-namespace check and crashed building the label
// selector. With the early short-circuit, we never even attempt to read
// the PVC.
func TestReconcile_SystemNamespace_ShortCircuits(t *testing.T) {
	scheme := newTestScheme(t)
	// Long-named PVC in a system namespace — the exact failure shape from
	// 2026-05-08. 104 chars, well over the 63-byte label-value limit.
	longName := "prometheus-kube-prometheus-stack-prometheus-db-prometheus-kube-prometheus-stack-prometheus-0-extra"
	if len(longName) <= k8sLabelValueMaxLen {
		t.Fatalf("test fixture mistake: long name length %d not > %d", len(longName), k8sLabelValueMaxLen)
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: promStackNS,
			Name:      longName,
			Labels:    map[string]string{backupLabelKey: backupHourly},
		},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()

	r := &PVCReconciler{
		Client:           cli,
		ExternalSecret:   testExternalSecretConfig(),
		SystemNamespaces: map[string]struct{}{promStackNS: {}},
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: promStackNS, Name: longName}})
	if err != nil {
		t.Fatalf("reconcile must succeed for system-namespace PVC, got error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0 (system-namespace short-circuit)", res.RequeueAfter)
	}

	// No children should have been created — the reconciler short-
	// circuited before the ensure* path.
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(esGVK)
	err = cli.Get(context.Background(), types.NamespacedName{Namespace: promStackNS, Name: "volsync-" + longName}, got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("ES must not be created in system namespace; got err=%v", err)
	}
}

// TestLabelSafePVCRef_ShortNameUnchanged pins the no-op path: a PVC name
// that fits the 63-byte limit returns verbatim, so steady-state callers
// (the bulk of the cluster) see no behavior change.
func TestLabelSafePVCRef_ShortNameUnchanged(t *testing.T) {
	cases := []string{
		"data",
		"prometheus-data-0", // realistic short prom name
		strings.Repeat("a", k8sLabelValueMaxLen),
	}
	for _, name := range cases {
		if got := labelSafePVCRef(name); got != name {
			t.Errorf("labelSafePVCRef(%q) = %q, want unchanged", name, got)
		}
	}
}

// TestLabelSafePVCRef_LongNameHashed pins the hash-truncate path: a PVC
// name over 63 bytes maps to a deterministic 28-char `pvc-<hex>` value
// that fits the label-value limit and survives selector validation.
func TestLabelSafePVCRef_LongNameHashed(t *testing.T) {
	long1 := strings.Repeat("a", k8sLabelValueMaxLen+1)
	long2 := "prometheus-kube-prometheus-stack-prometheus-db-prometheus-kube-prometheus-stack-prometheus-0"

	r1 := labelSafePVCRef(long1)
	r2 := labelSafePVCRef(long2)

	for _, r := range []string{r1, r2} {
		if len(r) > k8sLabelValueMaxLen {
			t.Errorf("hashed ref %q exceeds %d bytes (len=%d)", r, k8sLabelValueMaxLen, len(r))
		}
		if !strings.HasPrefix(r, "pvc-") {
			t.Errorf("hashed ref %q must start with `pvc-` to mark it as hashed", r)
		}
		if len(r) != 28 {
			t.Errorf("hashed ref %q has length %d, want exactly 28 (4 prefix + 24 hex)", r, len(r))
		}
	}

	// Determinism: same input maps to the same output every time. cleanup()
	// relies on this so the selector matches what newUnstructured emitted at
	// create time.
	if got := labelSafePVCRef(long1); got != r1 {
		t.Errorf("labelSafePVCRef is non-deterministic: %q vs %q", got, r1)
	}
	// Distinctness: two different long names hash to different values
	// (otherwise cleanup would reap the wrong children).
	if r1 == r2 {
		t.Errorf("labelSafePVCRef collided for distinct inputs: %q", r1)
	}
}

// TestReconcile_LongPVCName_AppNamespace_UsesHashedLabel pins the
// defense-in-depth path: a long-named PVC in an APP namespace (not in
// SystemNamespaces) reconciles cleanly and the children carry the
// hashed `volsync.backup/pvc=pvc-<hex>` label rather than the raw name.
// Without labelSafePVCRef the unstructured.SetLabels call would emit a
// 100+-char label that the apiserver would reject on the next List.
func TestReconcile_LongPVCName_AppNamespace_UsesHashedLabel(t *testing.T) {
	scheme := newTestScheme(t)
	longName := strings.Repeat("a", 80)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         testNamespace,
			Name:              longName,
			Labels:            map[string]string{backupLabelKey: backupHourly},
			CreationTimestamp: metav1.NewTime(time.Now().Add(-3 * time.Hour)),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()
	r := newTestReconciler(cli)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: longName}}); err != nil {
		t.Fatalf("reconcile errored on long-name PVC: %v", err)
	}

	// Pull the rendered ES and check its labels.
	es := &unstructured.Unstructured{}
	es.SetGroupVersionKind(esGVK)
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "volsync-" + longName}, es); err != nil {
		t.Fatalf("get ES for long-name pvc: %v", err)
	}
	got := es.GetLabels()[pvcLabel]
	want := labelSafePVCRef(longName)
	if got != want {
		t.Errorf("ES pvcLabel = %q, want %q", got, want)
	}
	if len(got) > k8sLabelValueMaxLen {
		t.Errorf("ES pvcLabel %q exceeds 63 bytes (len=%d)", got, len(got))
	}

	// And cleanup must find it via the same hashed selector. Trigger
	// cleanup by removing the label and re-reconciling.
	pvc.Labels = nil
	if err := cli.Update(context.Background(), pvc); err != nil {
		t.Fatalf("update pvc to remove backup label: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: longName}}); err != nil {
		t.Fatalf("cleanup reconcile errored on long-name PVC: %v", err)
	}
	gotAfter := &unstructured.Unstructured{}
	gotAfter.SetGroupVersionKind(esGVK)
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "volsync-" + longName}, gotAfter); !apierrors.IsNotFound(err) {
		t.Errorf("cleanup must reap children of long-name PVC; got err=%v", err)
	}
}

// TestEnsureExternalSecret_NoRecycleOnSteadyState pins the inverse of the
// migration test: a pre-existing v3-shaped ES must NOT be recycled (the
// reconciler is Get-or-Create in steady state). This guards against an
// over-eager migration helper that drift-corrects every reconcile.
func TestEnsureExternalSecret_NoRecycleOnSteadyState(t *testing.T) {
	scheme := newTestScheme(t)
	pvc := labeledPVC("hourly", corev1.ClaimBound, 3*time.Hour)

	preExisting := childObject(esGVK, testNamespace, testESName, testPVCName)
	preExisting.Object["spec"] = map[string]interface{}{
		esFieldRefreshInterval: "1h",
		esFieldTarget: map[string]interface{}{
			esFieldName:           testESName,
			esFieldCreationPolicy: creationPolicyOwner,
			esFieldTemplate: map[string]interface{}{
				dataField: map[string]interface{}{
					kopiaEnvRepository: "s3://" + testS3Bucket,
				},
			},
		},
		dataField: []interface{}{
			esRemoteRef(kopiaEnvPassword, testVaultKey, testKopiaPwProp),
		},
	}
	// Annotate with a deliberate hand-edit marker; if the reconciler recycles
	// the ES the annotation will be lost.
	preExisting.SetAnnotations(map[string]string{"operator.note": "do-not-recycle"})

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc, preExisting).Build()
	r := newTestReconciler(cli)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testPVCName}}); err != nil {
		t.Fatalf("reconcile errored: %v", err)
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(esGVK)
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testESName}, got); err != nil {
		t.Fatalf("get ES: %v", err)
	}
	if got.GetAnnotations()["operator.note"] != "do-not-recycle" {
		t.Errorf("pre-existing v3-shape ES was recycled (lost hand-edit annotation); reconciler must be Get-or-Create in steady state")
	}
}
