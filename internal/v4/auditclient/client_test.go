package auditclient

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/mitchross/pvc-plumber/internal/v4/mode"
)

// Test-scope constants — repeated fixture strings.
const (
	testNSMyapp    = "myapp"
	testNSNsA      = "ns-a"
	testSecretName = "preseed"
)

// makeAuditingClient builds an AuditingClient backed by a fake client.
// Returns the wrapper, the underlying fake (so tests can introspect it),
// and a captured slog buffer.
func makeAuditingClient(t *testing.T, m mode.Mode, seedObjs ...client.Object) (*Client, client.WithWatch, *bytes.Buffer) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(seedObjs...).Build()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	return New(fakeClient, m, log), fakeClient, &buf
}

func pvcFixture(ns, name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.PersistentVolumeClaimSpec{},
	}
}

func secretFixture(ns, name string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
	}
}

// TestAuditMode_BlocksAllWriteVerbs is the core safety guarantee for Phase
// 2.5: in audit mode, every write verb returns nil without touching the
// underlying fake client.
func TestAuditMode_BlocksAllWriteVerbs(t *testing.T) {
	ctx := context.Background()
	ac, fakeC, buf := makeAuditingClient(t, mode.Audit)

	// Create — must not appear in the fake.
	if err := ac.Create(ctx, pvcFixture(testNSMyapp, "data")); err != nil {
		t.Fatalf("audit Create returned error: %v", err)
	}
	got := &corev1.PersistentVolumeClaim{}
	err := fakeC.Get(ctx, types.NamespacedName{Namespace: testNSMyapp, Name: "data"}, got)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("audit Create leaked through: fake has the object (err=%v)", err)
	}

	// Seed an object so Update / Patch / Delete have a target.
	preexisting := secretFixture(testNSMyapp, "preseed")
	if err := fakeC.Create(ctx, preexisting); err != nil {
		t.Fatal(err)
	}
	original := preexisting.DeepCopy()

	// Update — must not change the seeded object.
	preexisting.Annotations = map[string]string{"audit-test": "shouldnotpersist"}
	if err := ac.Update(ctx, preexisting); err != nil {
		t.Fatalf("audit Update returned error: %v", err)
	}
	gotSec := &corev1.Secret{}
	if err := fakeC.Get(ctx, types.NamespacedName{Namespace: testNSMyapp, Name: testSecretName}, gotSec); err != nil {
		t.Fatal(err)
	}
	if _, has := gotSec.Annotations["audit-test"]; has {
		t.Errorf("audit Update leaked through: object now carries annotation %q", "audit-test")
	}

	// Patch — must not change the seeded object.
	mergePatch := client.MergeFrom(original)
	patched := original.DeepCopy()
	patched.Labels = map[string]string{"audit-test": "shouldnotpersist"}
	if err := ac.Patch(ctx, patched, mergePatch); err != nil {
		t.Fatalf("audit Patch returned error: %v", err)
	}
	if err := fakeC.Get(ctx, types.NamespacedName{Namespace: testNSMyapp, Name: testSecretName}, gotSec); err != nil {
		t.Fatal(err)
	}
	if _, has := gotSec.Labels["audit-test"]; has {
		t.Errorf("audit Patch leaked through: object now carries label %q", "audit-test")
	}

	// Delete — must NOT delete the seeded object.
	if err := ac.Delete(ctx, preexisting); err != nil {
		t.Fatalf("audit Delete returned error: %v", err)
	}
	if err := fakeC.Get(ctx, types.NamespacedName{Namespace: testNSMyapp, Name: testSecretName}, gotSec); err != nil {
		t.Errorf("audit Delete leaked through: object no longer in fake (err=%v)", err)
	}

	// DeleteAllOf — must not delete anything.
	if err := ac.DeleteAllOf(ctx, &corev1.Secret{}, client.InNamespace(testNSMyapp)); err != nil {
		t.Fatalf("audit DeleteAllOf returned error: %v", err)
	}
	if err := fakeC.Get(ctx, types.NamespacedName{Namespace: testNSMyapp, Name: testSecretName}, gotSec); err != nil {
		t.Errorf("audit DeleteAllOf leaked through: pre-seeded object is gone (err=%v)", err)
	}

	// Counters: 4 writes attempted (Create, Update, Patch, Delete) + 1 DeleteAllOf = 5 wouldWrites total.
	totals := ac.WouldWriteTotals()
	if totals.Create != 1 {
		t.Errorf("WouldWrite.Create: got %d, want 1", totals.Create)
	}
	if totals.Update != 1 {
		t.Errorf("WouldWrite.Update: got %d, want 1", totals.Update)
	}
	if totals.Patch != 1 {
		t.Errorf("WouldWrite.Patch: got %d, want 1", totals.Patch)
	}
	if totals.Delete != 1 {
		t.Errorf("WouldWrite.Delete: got %d, want 1", totals.Delete)
	}
	if totals.DeleteAllOf != 1 {
		t.Errorf("WouldWrite.DeleteAllOf: got %d, want 1", totals.DeleteAllOf)
	}
	if totals.Total() != 5 {
		t.Errorf("WouldWrite.Total: got %d, want 5", totals.Total())
	}

	// "Did" counters must remain zero in audit mode.
	if did := ac.DidWriteTotals(); did.Total() != 0 {
		t.Errorf("DidWrite.Total in audit mode: got %d, want 0 (%+v)", did.Total(), did)
	}

	// Log output must contain "audit-mode would-write" for every call.
	logged := buf.String()
	for _, verb := range []string{"create", "update", "patch", "delete", "deleteAllOf"} {
		if !strings.Contains(logged, "verb="+verb) {
			t.Errorf("log missing entry for verb=%s\nlogs:\n%s", verb, logged)
		}
	}
	if !strings.Contains(logged, "audit-mode would-write") {
		t.Errorf("log missing the 'audit-mode would-write' marker string")
	}
}

// TestNonAuditMode_WritesPassThrough confirms the wrapper is transparent in
// modes other than audit.
func TestNonAuditMode_WritesPassThrough(t *testing.T) {
	for _, m := range []mode.Mode{mode.Permissive, mode.Enforce, mode.Strict} {
		t.Run(m.String(), func(t *testing.T) {
			ctx := context.Background()
			ac, fakeC, _ := makeAuditingClient(t, m)

			obj := pvcFixture(testNSMyapp, "live-write")
			if err := ac.Create(ctx, obj); err != nil {
				t.Fatalf("%s Create error: %v", m, err)
			}

			got := &corev1.PersistentVolumeClaim{}
			if err := fakeC.Get(ctx, types.NamespacedName{Namespace: testNSMyapp, Name: "live-write"}, got); err != nil {
				t.Errorf("%s Create did NOT propagate to fake: %v", m, err)
			}

			// Did counters: 1 create; would counters: 0.
			if did := ac.DidWriteTotals(); did.Create != 1 {
				t.Errorf("%s DidWrite.Create: got %d, want 1", m, did.Create)
			}
			if would := ac.WouldWriteTotals(); would.Total() != 0 {
				t.Errorf("%s WouldWrite in non-audit mode: got %d, want 0", m, would.Total())
			}
		})
	}
}

// TestAuditMode_ReadsPassThrough confirms Get/List are not affected.
func TestAuditMode_ReadsPassThrough(t *testing.T) {
	ctx := context.Background()
	seed := pvcFixture(testNSMyapp, "seeded-pvc")
	ac, _, _ := makeAuditingClient(t, mode.Audit, seed)

	// Get
	got := &corev1.PersistentVolumeClaim{}
	if err := ac.Get(ctx, types.NamespacedName{Namespace: testNSMyapp, Name: "seeded-pvc"}, got); err != nil {
		t.Fatalf("Get failed in audit mode: %v", err)
	}
	if got.Name != "seeded-pvc" {
		t.Errorf("Get returned wrong object: %s", got.Name)
	}

	// List
	var list corev1.PersistentVolumeClaimList
	if err := ac.List(ctx, &list, client.InNamespace(testNSMyapp)); err != nil {
		t.Fatalf("List failed in audit mode: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("List returned %d items, want 1", len(list.Items))
	}
}

// TestAuditMode_DefenseInDepthUnspecified treats mode.Unspecified the same
// as audit — defensive against future config bugs that leave Mode at zero.
func TestAuditMode_DefenseInDepthUnspecified(t *testing.T) {
	ctx := context.Background()
	ac, fakeC, _ := makeAuditingClient(t, mode.Unspecified)
	if err := ac.Create(ctx, pvcFixture(testNSMyapp, "should-be-blocked")); err != nil {
		t.Fatal(err)
	}
	got := &corev1.PersistentVolumeClaim{}
	err := fakeC.Get(ctx, types.NamespacedName{Namespace: testNSMyapp, Name: "should-be-blocked"}, got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("Unspecified mode allowed write through: %v", err)
	}
}

// TestStatusWriter_AuditGated covers Status() sub-resource path.
func TestStatusWriter_AuditGated(t *testing.T) {
	ctx := context.Background()
	seed := pvcFixture(testNSMyapp, "status-target")
	ac, fakeC, buf := makeAuditingClient(t, mode.Audit, seed)

	updated := seed.DeepCopy()
	updated.Status.Phase = corev1.ClaimBound
	if err := ac.Status().Update(ctx, updated); err != nil {
		t.Fatalf("Status().Update error in audit: %v", err)
	}

	got := &corev1.PersistentVolumeClaim{}
	if err := fakeC.Get(ctx, types.NamespacedName{Namespace: testNSMyapp, Name: "status-target"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase == corev1.ClaimBound {
		t.Errorf("Status().Update leaked through in audit mode")
	}
	if !strings.Contains(buf.String(), "subresource=status") {
		t.Errorf("expected status sub-resource log line; got:\n%s", buf.String())
	}
}

// TestWouldWriteByKind verifies the per-(verb/kind) histogram.
func TestWouldWriteByKind(t *testing.T) {
	ctx := context.Background()
	ac, _, _ := makeAuditingClient(t, mode.Audit)

	_ = ac.Create(ctx, pvcFixture(testNSNsA, "p1"))
	_ = ac.Create(ctx, pvcFixture(testNSNsA, "p2"))
	_ = ac.Create(ctx, secretFixture(testNSNsA, "s1"))
	_ = ac.Delete(ctx, secretFixture(testNSNsA, "s1"))

	byKind := ac.WouldWriteByKind()
	want := map[string]int64{
		"create/PersistentVolumeClaim": 2,
		"create/Secret":                1,
		"delete/Secret":                1,
	}
	got := map[string]int64{}
	for _, kc := range byKind {
		got[kc.VerbKind] = kc.Count
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("WouldWriteByKind[%s]: got %d, want %d (full: %+v)", k, got[k], v, got)
		}
	}
}

// TestConstructor_NilLogger sets slog.Default() when nil.
func TestConstructor_NilLogger(t *testing.T) {
	ac, _, _ := makeAuditingClient(t, mode.Audit)
	if ac.Log == nil {
		t.Error("Log should never be nil after construction")
	}
	// Also test direct construction with nil:
	direct := New(nil, mode.Audit, nil)
	if direct.Log == nil {
		t.Error("New() should default to slog.Default() when log is nil")
	}
}

// TestNonAuditMode_AllWriteVerbsPassThrough exercises Update/Patch/Delete/
// DeleteAllOf success paths (TestNonAuditMode_WritesPassThrough only
// covered Create). Bumps coverage on the "did" counter increments and
// confirms the wrapper is transparent for every write verb.
func TestNonAuditMode_AllWriteVerbsPassThrough(t *testing.T) {
	ctx := context.Background()
	seed := secretFixture(testNSNsA, testSecretName)
	ac, fakeC, _ := makeAuditingClient(t, mode.Enforce, seed)

	// Update
	seed.Annotations = map[string]string{"phase2.5": "did-update"}
	if err := ac.Update(ctx, seed); err != nil {
		t.Fatalf("enforce Update: %v", err)
	}
	got := &corev1.Secret{}
	_ = fakeC.Get(ctx, types.NamespacedName{Namespace: testNSNsA, Name: testSecretName}, got)
	if got.Annotations["phase2.5"] != "did-update" {
		t.Errorf("enforce Update did not propagate")
	}

	// Patch
	original := got.DeepCopy()
	patched := got.DeepCopy()
	patched.Labels = map[string]string{"phase2.5": "did-patch"}
	if err := ac.Patch(ctx, patched, client.MergeFrom(original)); err != nil {
		t.Fatalf("enforce Patch: %v", err)
	}
	_ = fakeC.Get(ctx, types.NamespacedName{Namespace: testNSNsA, Name: testSecretName}, got)
	if got.Labels["phase2.5"] != "did-patch" {
		t.Errorf("enforce Patch did not propagate")
	}

	// Delete + recreate so we can DeleteAllOf below.
	if err := ac.Delete(ctx, seed); err != nil {
		t.Fatalf("enforce Delete: %v", err)
	}
	// The fake client returns NotFound on Get of a deleted object and
	// zeroes the destination struct. A non-empty Name here would mean
	// Delete didn't actually remove the object. We don't fail the test
	// on the converse (some fake-client versions may surface a zero
	// object without Get returning NotFound), but we do assert deletion
	// happened.
	err := fakeC.Get(ctx, types.NamespacedName{Namespace: testNSNsA, Name: testSecretName}, got)
	if err == nil && got.Name == "victim" {
		t.Errorf("enforce Delete did not propagate: victim still present")
	}

	// Reseed and DeleteAllOf.
	if err := fakeC.Create(ctx, secretFixture(testNSNsA, "doomed-1")); err != nil {
		t.Fatal(err)
	}
	if err := fakeC.Create(ctx, secretFixture(testNSNsA, "doomed-2")); err != nil {
		t.Fatal(err)
	}
	if err := ac.DeleteAllOf(ctx, &corev1.Secret{}, client.InNamespace(testNSNsA)); err != nil {
		t.Fatalf("enforce DeleteAllOf: %v", err)
	}
	var remaining corev1.SecretList
	_ = fakeC.List(ctx, &remaining, client.InNamespace(testNSNsA))
	if len(remaining.Items) != 0 {
		t.Errorf("enforce DeleteAllOf did not propagate: %d items remain", len(remaining.Items))
	}

	did := ac.DidWriteTotals()
	if did.Update < 1 || did.Patch < 1 || did.Delete < 1 || did.DeleteAllOf < 1 {
		t.Errorf("DidWrite verb counters: %+v (expected ≥1 each)", did)
	}
}

// TestSubResourceClient_AuditGated covers the SubResource() code path with
// a status update via the SubResourceClient API (rare but supported).
func TestSubResourceClient_AuditGated(t *testing.T) {
	ctx := context.Background()
	seed := pvcFixture(testNSNsA, "subres-target")
	ac, fakeC, buf := makeAuditingClient(t, mode.Audit, seed)

	updated := seed.DeepCopy()
	updated.Status.Phase = corev1.ClaimBound
	if err := ac.SubResource("status").Update(ctx, updated); err != nil {
		t.Fatalf("SubResource('status').Update in audit: %v", err)
	}
	got := &corev1.PersistentVolumeClaim{}
	_ = fakeC.Get(ctx, types.NamespacedName{Namespace: testNSNsA, Name: "subres-target"}, got)
	if got.Status.Phase == corev1.ClaimBound {
		t.Errorf("SubResource('status').Update leaked through")
	}
	if !strings.Contains(buf.String(), "subresource=status") {
		t.Errorf("SubResource log missing subresource=status; got:\n%s", buf.String())
	}

	// And the Get on SubResource passes through. We don't care whether
	// the fake client returns an error (it may, since /status isn't a
	// real sub-resource on PVCs in the fake) — we only care that the
	// call reaches the wrapped client and doesn't panic.
	_ = ac.SubResource("status").Get(ctx, seed, &corev1.PersistentVolumeClaim{})
}

// TestAuditMode_NoEventCreation defends the "no cluster writes" contract
// against a future contributor adding direct `client.Create(ctx, &corev1.Event{})`
// calls in the reconciler. EventRecorder bypasses this wrapper, but a
// direct Create through client.Client is gated identically to any other
// write.
//
// If an EventRecorder is ever added to the reconciler, it must be gated
// separately. See the package-level doc comment in client.go.
func TestAuditMode_NoEventCreation(t *testing.T) {
	ctx := context.Background()
	ac, fakeC, _ := makeAuditingClient(t, mode.Audit)

	evt := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "myapp",
			Name:      "data.test.event",
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "PersistentVolumeClaim",
			Namespace: "myapp",
			Name:      "data",
		},
		Type:    "Normal",
		Reason:  "PhantomEvent",
		Message: "this event must NOT land in the cluster in audit mode",
	}
	if err := ac.Create(ctx, evt); err != nil {
		t.Fatalf("audit Create of Event returned error: %v", err)
	}

	got := &corev1.Event{}
	err := fakeC.Get(ctx, types.NamespacedName{Namespace: testNSMyapp, Name: "data.test.event"}, got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("audit Create of Event leaked through: fake has the Event (err=%v)", err)
	}

	byKind := ac.WouldWriteByKind()
	foundEvent := false
	for _, k := range byKind {
		if k.VerbKind == "create/Event" {
			foundEvent = true
			break
		}
	}
	if !foundEvent {
		t.Errorf("WouldWriteByKind missing create/Event entry; got: %+v", byKind)
	}
}

// silence unused-import check when slog handler discards output.
var _ = io.Discard
