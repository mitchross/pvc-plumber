package controller

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/mitchross/pvc-plumber/internal/v4/auditclient"
	"github.com/mitchross/pvc-plumber/internal/v4/mode"
)

// TestAuditMode_PVCReconciler_NoWrites is the Phase 2.5 safety guarantee.
//
// The same fixture that, in TestReconcile_BoundOld_CreatesAllThree, produces
// three child resources (ExternalSecret, ReplicationSource, Replication
// Destination) MUST produce zero writes when the reconciler is given an
// audit-mode-wrapped client.
//
// The assertion strategy is positive: query the fake client for each
// expected child and assert IsNotFound. The audit wrapper logs every
// "would" write to the captured slog buffer for visibility.
func TestAuditMode_PVCReconciler_NoWrites(t *testing.T) {
	scheme := newTestScheme(t)
	pvc := labeledPVC("hourly", corev1.ClaimBound, 3*time.Hour)

	fakeCli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc).
		Build()

	var logBuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logBuf, nil))
	auditCli := auditclient.New(fakeCli, mode.Audit, log)

	r := &PVCReconciler{
		Client:         auditCli,
		ExternalSecret: testExternalSecretConfig(),
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testPVCName},
	})
	if err != nil {
		t.Fatalf("reconcile in audit mode should not error: %v", err)
	}
	_ = res // RequeueAfter behavior is not the contract under test here

	// Symmetric assertions vs TestReconcile_BoundOld_CreatesAllThree: none
	// of the three children that the live reconciler would have created
	// may be visible to the underlying fake client.
	for _, want := range []struct {
		gvk  schema.GroupVersionKind
		name string
	}{
		{esGVK, testESName},
		{rsGVK, testRSDestName},
		{rdGVK, testRSDestName},
	} {
		t.Run("absent/"+want.gvk.Kind, func(t *testing.T) {
			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(want.gvk)
			err := fakeCli.Get(context.Background(),
				types.NamespacedName{Namespace: testNamespace, Name: want.name},
				obj,
			)
			if !apierrors.IsNotFound(err) {
				t.Errorf("audit mode leaked through: fake client has %s %s/%s (err=%v)",
					want.gvk.Kind, testNamespace, want.name, err)
			}
		})
	}

	// The audit wrapper must have observed the "would" writes.
	would := auditCli.WouldWriteTotals()
	if would.Create < 3 {
		t.Errorf("WouldWrite.Create: got %d, want ≥3 (one per child ES/RS/RD); full snapshot %+v",
			would.Create, would)
	}

	// And the "did" counters must be zero in audit mode.
	if did := auditCli.DidWriteTotals(); did.Total() != 0 {
		t.Errorf("DidWrite.Total in audit mode: got %d, want 0; full snapshot %+v",
			did.Total(), did)
	}

	// Per-kind histogram should mention each child kind.
	byKind := auditCli.WouldWriteByKind()
	gotKinds := map[string]bool{}
	for _, k := range byKind {
		gotKinds[k.VerbKind] = true
	}
	for _, want := range []string{
		"create/ExternalSecret.external-secrets.io",
		"create/ReplicationSource.volsync.backube",
		"create/ReplicationDestination.volsync.backube",
	} {
		if !gotKinds[want] {
			t.Errorf("WouldWriteByKind missing %s; got: %+v", want, gotKinds)
		}
	}

	// The audit log buffer must mention all three create attempts.
	logged := logBuf.String()
	for _, marker := range []string{
		"audit-mode would-write",
		"verb=create",
		"kind=ExternalSecret.external-secrets.io",
		"kind=ReplicationSource.volsync.backube",
		"kind=ReplicationDestination.volsync.backube",
	} {
		if !contains(logged, marker) {
			t.Errorf("log missing marker %q\nfull log:\n%s", marker, logged)
		}
	}
}

// TestAuditMode_PVCReconciler_ReapPathBlocked covers the cleanup branch:
// when a previously-backed-up PVC gains backup-exempt=true, the reconciler
// would normally Delete the three children. In audit mode, the Delete must
// not propagate to the fake client.
func TestAuditMode_PVCReconciler_ReapPathBlocked(t *testing.T) {
	scheme := newTestScheme(t)

	// PVC is backup-exempt; pretend the chart-era operator already created
	// children (we seed them into the fake) and the reconciler should reap.
	pvc := labeledPVC("hourly", corev1.ClaimBound, 3*time.Hour)
	pvc.Labels[backupExemptLabel] = labelTrue
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	// Hard-coded literal because the FQ-reason constant lives in the
	// webhook package; importing webhook from a controller test would
	// pull a webhook→controller cycle in some build configurations.
	pvc.Annotations["storage.vanillax.dev/backup-exempt-reason"] = "test fixture: pre-existing children must NOT be deleted in audit mode"

	// Seed children with the operator's managed-by + pvc labels so the
	// cleanup selector finds them.
	es := &unstructured.Unstructured{}
	es.SetGroupVersionKind(esGVK)
	es.SetNamespace(testNamespace)
	es.SetName(testESName)
	es.SetLabels(map[string]string{managedByLabel: managedByValue, pvcLabel: testPVCName})

	rs := &unstructured.Unstructured{}
	rs.SetGroupVersionKind(rsGVK)
	rs.SetNamespace(testNamespace)
	rs.SetName(testRSDestName)
	rs.SetLabels(map[string]string{managedByLabel: managedByValue, pvcLabel: testPVCName})

	rd := &unstructured.Unstructured{}
	rd.SetGroupVersionKind(rdGVK)
	rd.SetNamespace(testNamespace)
	rd.SetName(testRSDestName)
	rd.SetLabels(map[string]string{managedByLabel: managedByValue, pvcLabel: testPVCName})

	fakeCli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, es, rs, rd).
		Build()

	auditCli := auditclient.New(fakeCli, mode.Audit, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	r := &PVCReconciler{Client: auditCli, ExternalSecret: testExternalSecretConfig()}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testPVCName},
	}); err != nil {
		t.Fatalf("reconcile in audit mode (reap path) should not error: %v", err)
	}

	// All three children must still exist in the fake client.
	for _, target := range []struct {
		name string
		gvk  schema.GroupVersionKind
	}{
		{testESName, esGVK},
		{testRSDestName, rsGVK},
		{testRSDestName, rdGVK},
	} {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(target.gvk)
		err := fakeCli.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: target.name}, obj)
		if err != nil {
			t.Errorf("audit mode reap leaked: %s/%s is missing (err=%v) — Delete should have been a no-op",
				testNamespace, target.name, err)
		}
	}

	if did := auditCli.DidWriteTotals(); did.Total() != 0 {
		t.Errorf("DidWrite.Total in audit-reap mode: got %d, want 0 (%+v)", did.Total(), did)
	}
	// Should have recorded ≥3 "would" deletes.
	if would := auditCli.WouldWriteTotals(); would.Delete < 3 {
		t.Errorf("WouldWrite.Delete in audit-reap mode: got %d, want ≥3 (%+v)", would.Delete, would)
	}
}

// TestAuditMode_NonAuditAllowsWrites confirms the wrapper does NOT block
// writes when the mode is anything other than audit. This is the
// symmetric counter-test: the same fixture and reconciler, with mode set
// to enforce, must produce the three child resources.
func TestAuditMode_NonAuditAllowsWrites(t *testing.T) {
	scheme := newTestScheme(t)
	pvc := labeledPVC("hourly", corev1.ClaimBound, 3*time.Hour)
	fakeCli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()
	auditCli := auditclient.New(fakeCli, mode.Enforce, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))

	r := &PVCReconciler{Client: auditCli, ExternalSecret: testExternalSecretConfig()}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testPVCName},
	}); err != nil {
		t.Fatalf("reconcile in enforce mode errored: %v", err)
	}

	mustExist(t, fakeCli, esGVK, testESName)
	mustExist(t, fakeCli, rsGVK, testRSDestName)
	mustExist(t, fakeCli, rdGVK, testRSDestName)

	if did := auditCli.DidWriteTotals(); did.Create < 3 {
		t.Errorf("enforce DidWrite.Create: got %d, want ≥3 (%+v)", did.Create, did)
	}
	if would := auditCli.WouldWriteTotals(); would.Total() != 0 {
		t.Errorf("enforce WouldWrite.Total: got %d, want 0 (%+v)", would.Total(), would)
	}
}

// contains is a small dependency-free substring helper so this test file
// doesn't import "strings" just to call Contains.
func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
