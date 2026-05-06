package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	cases := []struct {
		ns       string
		pvc      string
		label    string
		wantHour string
		wantDay  string
	}{
		// "ns-pvc" → 6 chars → minute 6
		{ns: "ns", pvc: "pvc", wantHour: "6 * * * *", wantDay: "6 2 * * *"},
		// "default-data" → 12 chars → minute 12
		{ns: "default", pvc: "data", wantHour: "12 * * * *", wantDay: "12 2 * * *"},
		// 60-char joined string → minute 0
		{
			ns:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // 30
			pvc:      "bbbbbbbbbbbbbbbbbbbbbbbbbbbbb",  // 29 + dash = 60
			wantHour: "0 * * * *",
			wantDay:  "0 2 * * *",
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

	// Sanity: the design doc's WRONG formula (len(ns)+len(pvc)) gives the
	// same minute in every test case above except when there's a dash
	// boundary. Pin the difference here so a future refactor that "fixes"
	// the formula back to len(ns)+len(pvc) breaks loudly: "a"+"b" yields
	// minute=3 (len("a-b")=3), not 2.
	if got := backupSchedule("a", "b", "hourly"); got != "3 * * * *" {
		t.Errorf("backupSchedule must use len(ns+\"-\"+pvc), got %q (regression: design-doc formula)", got)
	}
}

func TestCleanup_DeletesAllByLabel(t *testing.T) {
	scheme := newTestScheme(t)
	ns := "app"
	pvcName := "data"

	es := childObject(esGVK, ns, "volsync-data", pvcName)
	rs := childObject(rsGVK, ns, "data-backup", pvcName)
	rd := childObject(rdGVK, ns, "data-backup", pvcName)
	// Decoy in another namespace and another pvcName — must NOT be deleted.
	decoyNS := childObject(esGVK, "other", "volsync-data", pvcName)
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
		{esGVK, "volsync-data"},
		{rsGVK, "data-backup"},
		{rdGVK, "data-backup"},
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
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: "other", Name: "volsync-data"}, gotDecoyNS); err != nil {
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

// labeledPVC builds a backup-labeled PVC with optional bind state and age.
func labeledPVC(ns, name, label string, phase corev1.PersistentVolumeClaimPhase, age time.Duration) *corev1.PersistentVolumeClaim {
	created := metav1.NewTime(time.Now().Add(-age))
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         ns,
			Name:              name,
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
	pvc := labeledPVC("app", "data", "hourly", corev1.ClaimPending, 0)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc).
		Build()
	r := &PVCReconciler{Client: cli}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app", Name: "data"}})
	if err != nil {
		t.Fatalf("reconcile errored: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Errorf("RequeueAfter = %v, want 30s", res.RequeueAfter)
	}

	// ES + RD should have been created even though the PVC isn't bound;
	// only RS is gated on Bound+age.
	mustExist(t, cli, esGVK, "app", "volsync-data")
	mustExist(t, cli, rdGVK, "app", "data-backup")
	mustNotExist(t, cli, rsGVK, "app", "data-backup")
}

func TestReconcile_BoundYoung_RequeuesUntilOld(t *testing.T) {
	scheme := newTestScheme(t)
	// Bound but only 30 minutes old → must wait another ~90m before backup.
	pvc := labeledPVC("app", "data", "daily", corev1.ClaimBound, 30*time.Minute)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc).
		Build()
	r := &PVCReconciler{Client: cli}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app", Name: "data"}})
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
	mustExist(t, cli, esGVK, "app", "volsync-data")
	mustExist(t, cli, rdGVK, "app", "data-backup")
	mustNotExist(t, cli, rsGVK, "app", "data-backup")
}

func TestReconcile_BoundOld_CreatesAllThree(t *testing.T) {
	scheme := newTestScheme(t)
	pvc := labeledPVC("app", "data", "hourly", corev1.ClaimBound, 3*time.Hour)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc).
		Build()
	r := &PVCReconciler{Client: cli}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app", Name: "data"}})
	if err != nil {
		t.Fatalf("reconcile errored: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0 (steady state)", res.RequeueAfter)
	}

	mustExist(t, cli, esGVK, "app", "volsync-data")
	mustExist(t, cli, rdGVK, "app", "data-backup")
	mustExist(t, cli, rsGVK, "app", "data-backup")

	// Spot-check labels and a representative spec field on the RS, since
	// that's the resource whose schedule is the trickiest port.
	rs := &unstructured.Unstructured{}
	rs.SetGroupVersionKind(rsGVK)
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: "app", Name: "data-backup"}, rs); err != nil {
		t.Fatalf("get RS: %v", err)
	}
	if v := rs.GetLabels()[managedByLabel]; v != managedByValue {
		t.Errorf("RS missing managed-by label, got %q", v)
	}
	if v := rs.GetLabels()[pvcLabel]; v != "data" {
		t.Errorf("RS missing pvc label, got %q", v)
	}
	schedule, _, err := unstructured.NestedString(rs.Object, "spec", "trigger", "schedule")
	if err != nil {
		t.Fatalf("read schedule: %v", err)
	}
	// "app-data" = 8 chars → minute 8, hourly.
	if schedule != "8 * * * *" {
		t.Errorf("RS schedule = %q, want %q", schedule, "8 * * * *")
	}
}

// mustExist fails the test if a child of the given GVK is absent.
func mustExist(t *testing.T, cli client.Client, gvk schema.GroupVersionKind, namespace, name string) {
	t.Helper()
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvk)
	if err := cli.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, got); err != nil {
		t.Errorf("expected %s %s/%s to exist, got err=%v", gvk.Kind, namespace, name, err)
	}
}

// mustNotExist fails if the object is present.
func mustNotExist(t *testing.T, cli client.Client, gvk schema.GroupVersionKind, namespace, name string) {
	t.Helper()
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvk)
	err := cli.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected %s %s/%s to be absent, got err=%v", gvk.Kind, namespace, name, err)
	}
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
