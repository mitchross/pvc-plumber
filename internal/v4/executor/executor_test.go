package executor_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/mitchross/pvc-plumber/internal/v4/executor"
	v4labels "github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/mode"
	"github.com/mitchross/pvc-plumber/internal/v4/planner"
)

// =============================================================================
// Test constants + canonical names
// =============================================================================

const (
	tns        = "myapp"
	tpvcName   = "data"
	tdstName   = "data-dst"
	tgoodRepo  = "volsync-kopia-repository"
	tdriftRepo = "stale-repo"

	// Canonical "group/version/Kind" strings, mirrored from
	// safety.go's allow-list so test assertions don't have to import
	// schema directly.
	rsGVKStr = "volsync.backube/v1alpha1/ReplicationSource"
	rdGVKStr = "volsync.backube/v1alpha1/ReplicationDestination"

	// Refusal reasons we assert on.
	reasonForbiddenKind = "forbidden-kind"
	reasonExists        = "exists"
	reasonNotOwned      = "not-owned"
	reasonAbsent        = "absent"
	reasonModeAudit     = "mode=audit"

	managedByPVCPlumber = "pvc-plumber"
	managedByArgoCD     = "argocd"

	// Group/version/kind string fragments. Hoisted to constants both
	// for readability and because goconst flags the duplicates.
	groupVolSync   = "volsync.backube"
	versionV1Alpha = "v1alpha1"
	kindSecret     = "Secret"
)

var (
	rsGVK = schema.GroupVersionKind{Group: groupVolSync, Version: versionV1Alpha, Kind: "ReplicationSource"}
	rdGVK = schema.GroupVersionKind{Group: groupVolSync, Version: versionV1Alpha, Kind: "ReplicationDestination"}
)

// =============================================================================
// Fixture helpers
// =============================================================================

// recordedAction captures every write that passed through the
// recordingClient. The paranoia walk uses this slice to prove only
// RS/RD GVKs ever reached the apiserver.
type recordedAction struct {
	Verb      string // "create" | "update" | "delete"
	GVK       string
	Namespace string
	Name      string
}

// recordingClient wraps a controller-runtime client.Client and records
// every Create/Update/Delete call. Reads pass through to the embedded
// client unchanged so the executor's Get calls during execUpdate /
// execDelete don't contaminate the action log.
//
// Patch / DeleteAllOf are not used by the executor but are recorded
// defensively — if a future bug ever calls them, the paranoia walk
// will see GVKs that don't match the allow-list.
type recordingClient struct {
	client.Client
	actions []recordedAction
}

func (rc *recordingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	rc.actions = append(rc.actions, recordedAction{
		Verb: "create", GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.Create(ctx, obj, opts...)
}

func (rc *recordingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	rc.actions = append(rc.actions, recordedAction{
		Verb: "update", GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.Update(ctx, obj, opts...)
}

func (rc *recordingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	rc.actions = append(rc.actions, recordedAction{
		Verb: "delete", GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.Delete(ctx, obj, opts...)
}

func (rc *recordingClient) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	rc.actions = append(rc.actions, recordedAction{
		Verb: "patch", GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.Patch(ctx, obj, p, opts...)
}

func (rc *recordingClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	rc.actions = append(rc.actions, recordedAction{
		Verb: "deleteAllOf", GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.DeleteAllOf(ctx, obj, opts...)
}

// gvkOf returns the canonical "group/version/Kind" string for any
// client.Object. Works for both Unstructured (uses the explicit GVK)
// and typed objects (uses TypeMeta).
func gvkOf(obj client.Object) string {
	gvk := obj.GetObjectKind().GroupVersionKind()
	return gvk.GroupVersion().String() + "/" + gvk.Kind
}

// newRecordingClient builds a fake controller-runtime client seeded
// with the given objects and wraps it in a recordingClient. Returns
// both so tests can assert on actions (via rc.actions) and read live
// state (via fc.Get).
func newRecordingClient(t *testing.T, objs ...client.Object) (*recordingClient, client.Client) {
	t.Helper()
	fc := fake.NewClientBuilder().WithObjects(objs...).Build()
	return &recordingClient{Client: fc}, fc
}

// =============================================================================
// Resource builders for desired state and live state
// =============================================================================

// rsDesired returns a fully-formed RS that the planner would emit for
// a create or update op. The minimum required to exercise the
// executor's flow — spec.kopia.repository so update-and-readback can
// be asserted.
func rsDesired(name, repo string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(rsGVK)
	u.SetNamespace(tns)
	u.SetName(name)
	u.SetLabels(map[string]string{
		v4labels.LabelManagedByKey: managedByPVCPlumber,
	})
	_ = unstructured.SetNestedField(u.Object, repo, "spec", "kopia", "repository")
	_ = unstructured.SetNestedField(u.Object, name, "spec", "sourcePVC")
	return u
}

// rdDesired mirrors rsDesired for ReplicationDestination.
func rdDesired(name, repo string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(rdGVK)
	u.SetNamespace(tns)
	u.SetName(name)
	u.SetLabels(map[string]string{
		v4labels.LabelManagedByKey: managedByPVCPlumber,
	})
	_ = unstructured.SetNestedField(u.Object, repo, "spec", "kopia", "repository")
	return u
}

// rsLive returns an existing-in-cluster RS for fixture seeding. The
// managedBy parameter controls the ownership label: pass empty string
// for the unmanaged case, "argocd" for inline-Argo, "pvc-plumber" for
// operator-owned.
func rsLive(name, managedBy, repo string) *unstructured.Unstructured {
	u := rsDesired(name, repo)
	if managedBy == "" {
		u.SetLabels(nil)
	} else {
		u.SetLabels(map[string]string{v4labels.LabelManagedByKey: managedBy})
	}
	return u
}

func rdLive(name, managedBy, repo string) *unstructured.Unstructured {
	u := rdDesired(name, repo)
	if managedBy == "" {
		u.SetLabels(nil)
	} else {
		u.SetLabels(map[string]string{v4labels.LabelManagedByKey: managedBy})
	}
	return u
}

// =============================================================================
// Plan helpers
// =============================================================================

func planCreate(resources ...*unstructured.Unstructured) planner.Plan {
	return planWith(planner.OpCreate, resources...)
}

func planUpdate(resources ...*unstructured.Unstructured) planner.Plan {
	return planWith(planner.OpUpdate, resources...)
}

func planDelete(resources ...*unstructured.Unstructured) planner.Plan {
	return planWith(planner.OpDelete, resources...)
}

func planWith(kind planner.OpKind, resources ...*unstructured.Unstructured) planner.Plan {
	ops := make([]planner.PlannedOp, 0, len(resources))
	for _, r := range resources {
		ops = append(ops, planner.PlannedOp{Kind: kind, Resource: r})
	}
	return planner.Plan{Ops: ops}
}

// =============================================================================
// Cross-cutting assertion helpers
// =============================================================================

// assertAllActionsAreRSOrRD walks the recorded write actions and fails
// the test if any one targets a GVK other than RS or RD. This is the
// last-line paranoia check that proves the executor never touched a
// Secret, ExternalSecret, PVC, webhook, or any other forbidden kind.
func assertAllActionsAreRSOrRD(t *testing.T, actions []recordedAction) {
	t.Helper()
	for _, a := range actions {
		if a.GVK != rsGVKStr && a.GVK != rdGVKStr {
			t.Errorf("forbidden write: verb=%s gvk=%s name=%s/%s (allow-list breach)", a.Verb, a.GVK, a.Namespace, a.Name)
		}
	}
}

// assertCounts asserts the four-bucket totals exactly. Fails verbosely
// so a mismatch immediately shows which bucket is off. wantFailed is
// kept in the signature (always 0 in current cases) so future tests
// that inject apiserver errors can use the same helper without a
// breaking rename.
//
//nolint:unparam // wantFailed is intentionally always 0 in Patch 6.6
func assertCounts(t *testing.T, got executor.Counts, wantSkipped, wantSucceeded, wantRefused, wantFailed int) {
	t.Helper()
	if got.Skipped != wantSkipped || got.Succeeded != wantSucceeded || got.Refused != wantRefused || got.Failed != wantFailed {
		t.Errorf("Counts: got %+v, want {Skipped:%d Succeeded:%d Refused:%d Failed:%d}",
			got, wantSkipped, wantSucceeded, wantRefused, wantFailed)
	}
}

// assertOutcomeStatus asserts a specific OpOutcome's Status and Reason.
func assertOutcomeStatus(t *testing.T, out executor.OpOutcome, wantStatus executor.OpStatus, wantReason string) {
	t.Helper()
	if out.Status != wantStatus {
		t.Errorf("OpOutcome %s/%s/%s: Status got %q, want %q (reason=%q err=%v)",
			out.GVK, out.Namespace, out.Name, out.Status, wantStatus, out.Reason, out.Err)
	}
	if wantReason != "" && out.Reason != wantReason {
		t.Errorf("OpOutcome %s/%s/%s: Reason got %q, want %q",
			out.GVK, out.Namespace, out.Name, out.Reason, wantReason)
	}
}

// =============================================================================
// Tests
// =============================================================================

// ---- Audit mode (Case 1, Case 2) -------------------------------------------

// Case 1: mode=Audit + non-empty plan (2 create ops) → both Skipped,
// fake client recorded zero Create calls.
func TestExecute_Audit_NonEmptyPlan_AllSkipped_NoClientCalls(t *testing.T) {
	rc, _ := newRecordingClient(t)
	plan := planCreate(rsDesired(tpvcName, tgoodRepo), rdDesired(tdstName, tgoodRepo))

	res := executor.Execute(context.Background(), rc, mode.Audit, plan)

	assertCounts(t, res.Counts, 2, 0, 0, 0)
	if len(res.Attempted) != 2 {
		t.Fatalf("Attempted: got %d, want 2", len(res.Attempted))
	}
	for _, out := range res.Attempted {
		assertOutcomeStatus(t, out, executor.OpSkipped, reasonModeAudit)
	}
	if len(rc.actions) != 0 {
		t.Errorf("recorded actions: got %d, want 0 (audit must be silent at the client layer)", len(rc.actions))
	}
}

// Case 2: mode=Audit + plan with a foreign-kind op → still Skipped, not
// Refused. Audit short-circuit wins over the GVK allow-list at the top
// of Execute.
func TestExecute_Audit_ForeignKindOp_StillSkipped(t *testing.T) {
	rc, _ := newRecordingClient(t)

	foreign := &unstructured.Unstructured{}
	foreign.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: kindSecret})
	foreign.SetNamespace(tns)
	foreign.SetName("kopia-creds")
	plan := planner.Plan{Ops: []planner.PlannedOp{{Kind: planner.OpUpdate, Resource: foreign}}}

	res := executor.Execute(context.Background(), rc, mode.Audit, plan)

	assertCounts(t, res.Counts, 1, 0, 0, 0)
	assertOutcomeStatus(t, res.Attempted[0], executor.OpSkipped, reasonModeAudit)
	if len(rc.actions) != 0 {
		t.Errorf("recorded actions: got %d, want 0", len(rc.actions))
	}
}

// ---- Permissive: create paths (Cases 3, 4) ---------------------------------

// Case 3: permissive + create RS+RD when both absent → both Succeeded.
// Resources are present on the fake client with managed-by=pvc-plumber.
func TestExecute_Permissive_CreateBothMissing_BothSucceed(t *testing.T) {
	rc, fc := newRecordingClient(t)
	plan := planCreate(rsDesired(tpvcName, tgoodRepo), rdDesired(tdstName, tgoodRepo))

	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)

	assertCounts(t, res.Counts, 0, 2, 0, 0)
	if len(rc.actions) != 2 {
		t.Fatalf("recorded actions: got %d, want 2", len(rc.actions))
	}
	for _, a := range rc.actions {
		if a.Verb != "create" {
			t.Errorf("action verb: got %q, want create", a.Verb)
		}
	}
	assertAllActionsAreRSOrRD(t, rc.actions)

	// Verify both resources are now present on the cluster.
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(rsGVK)
	if err := fc.Get(context.Background(), client.ObjectKey{Namespace: tns, Name: tpvcName}, got); err != nil {
		t.Errorf("RS not present after Create: %v", err)
	}
	if got.GetLabels()[v4labels.LabelManagedByKey] != managedByPVCPlumber {
		t.Errorf("RS managed-by: got %q, want %q", got.GetLabels()[v4labels.LabelManagedByKey], managedByPVCPlumber)
	}
}

// Case 4: permissive + create when RS already exists (any owner) →
// Refused("exists"). Fake client unchanged. Critically: the EXISTING
// resource's labels are not modified — no adoption.
func TestExecute_Permissive_CreateWhenExists_RefusedNoAdoption(t *testing.T) {
	// Seed: pre-existing inline-Argo RS at the canonical name.
	existing := rsLive(tpvcName, managedByArgoCD, tgoodRepo)
	rc, fc := newRecordingClient(t, existing)

	plan := planCreate(rsDesired(tpvcName, tgoodRepo))
	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)

	assertCounts(t, res.Counts, 0, 0, 1, 0)
	assertOutcomeStatus(t, res.Attempted[0], executor.OpRefused, reasonExists)
	// Critically — verify the existing resource was NOT relabeled.
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(rsGVK)
	if err := fc.Get(context.Background(), client.ObjectKey{Namespace: tns, Name: tpvcName}, got); err != nil {
		t.Fatalf("pre-existing RS lost: %v", err)
	}
	if got.GetLabels()[v4labels.LabelManagedByKey] != managedByArgoCD {
		t.Errorf("pre-existing RS managed-by got %q, want %q (NO ADOPTION on AlreadyExists)",
			got.GetLabels()[v4labels.LabelManagedByKey], managedByArgoCD)
	}
}

// ---- Permissive: update paths (Cases 5, 6, 7, 8) ---------------------------

// Case 5: permissive + update when live has managed-by=pvc-plumber and
// drifts → Succeeded, live spec replaced with planner's desired body.
func TestExecute_Permissive_UpdateOperatorOwnedDrift_Succeeds(t *testing.T) {
	live := rsLive(tpvcName, managedByPVCPlumber, tdriftRepo)
	liveRD := rdLive(tdstName, managedByPVCPlumber, tdriftRepo)
	rc, fc := newRecordingClient(t, live, liveRD)

	plan := planUpdate(rsDesired(tpvcName, tgoodRepo), rdDesired(tdstName, tgoodRepo))
	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)

	assertCounts(t, res.Counts, 0, 2, 0, 0)
	assertAllActionsAreRSOrRD(t, rc.actions)

	// Verify the RS's spec.kopia.repository was overwritten.
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(rsGVK)
	if err := fc.Get(context.Background(), client.ObjectKey{Namespace: tns, Name: tpvcName}, got); err != nil {
		t.Fatalf("RS gone after update: %v", err)
	}
	gotRepo, _, _ := unstructured.NestedString(got.Object, "spec", "kopia", "repository")
	if gotRepo != tgoodRepo {
		t.Errorf("RS repo after update: got %q, want %q", gotRepo, tgoodRepo)
	}
}

// Case 6: permissive + update when live has managed-by=argocd →
// Refused("not-owned"). Fake client unchanged.
func TestExecute_Permissive_UpdateInlineArgoOwned_RefusedNotOwned(t *testing.T) {
	live := rsLive(tpvcName, managedByArgoCD, tdriftRepo)
	rc, fc := newRecordingClient(t, live)

	plan := planUpdate(rsDesired(tpvcName, tgoodRepo))
	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)

	assertCounts(t, res.Counts, 0, 0, 1, 0)
	assertOutcomeStatus(t, res.Attempted[0], executor.OpRefused, reasonNotOwned)
	for _, a := range rc.actions {
		if a.Verb == "update" {
			t.Errorf("forbidden update on inline-argo resource: %+v", a)
		}
	}
	// Re-read to confirm repository was not changed.
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(rsGVK)
	_ = fc.Get(context.Background(), client.ObjectKey{Namespace: tns, Name: tpvcName}, got)
	gotRepo, _, _ := unstructured.NestedString(got.Object, "spec", "kopia", "repository")
	if gotRepo != tdriftRepo {
		t.Errorf("inline-argo RS was modified: repo got %q, want %q (unchanged)", gotRepo, tdriftRepo)
	}
}

// Case 7: permissive + update when live has NO managed-by label →
// Refused("not-owned").
func TestExecute_Permissive_UpdateUnmanaged_RefusedNotOwned(t *testing.T) {
	live := rsLive(tpvcName, "", tdriftRepo)
	rc, _ := newRecordingClient(t, live)

	plan := planUpdate(rsDesired(tpvcName, tgoodRepo))
	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)

	assertCounts(t, res.Counts, 0, 0, 1, 0)
	assertOutcomeStatus(t, res.Attempted[0], executor.OpRefused, reasonNotOwned)
}

// Case 8: permissive + update when live is missing → Refused("absent").
// (Planner shouldn't emit Update on absent state, but the executor is
// defensive.)
func TestExecute_Permissive_UpdateAbsent_RefusedAbsent(t *testing.T) {
	rc, _ := newRecordingClient(t)

	plan := planUpdate(rsDesired(tpvcName, tgoodRepo))
	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)

	assertCounts(t, res.Counts, 0, 0, 1, 0)
	assertOutcomeStatus(t, res.Attempted[0], executor.OpRefused, reasonAbsent)
}

// ---- Permissive: delete paths (Cases 9, 10, 11) ----------------------------

// Case 9: permissive + delete when live is managed-by=pvc-plumber →
// Succeeded, both gone from fake client.
func TestExecute_Permissive_DeleteOperatorOwned_Succeeds(t *testing.T) {
	live := rsLive(tpvcName, managedByPVCPlumber, tgoodRepo)
	liveRD := rdLive(tdstName, managedByPVCPlumber, tgoodRepo)
	rc, fc := newRecordingClient(t, live, liveRD)

	plan := planDelete(rsDesired(tpvcName, tgoodRepo), rdDesired(tdstName, tgoodRepo))
	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)

	assertCounts(t, res.Counts, 0, 2, 0, 0)
	assertAllActionsAreRSOrRD(t, rc.actions)

	// Both should be gone.
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(rsGVK)
	if err := fc.Get(context.Background(), client.ObjectKey{Namespace: tns, Name: tpvcName}, got); err == nil {
		t.Errorf("RS still present after delete")
	}
}

// Case 10: permissive + delete on inline-argo-owned resource →
// Refused("not-owned"), resource untouched.
func TestExecute_Permissive_DeleteInlineArgoOwned_RefusedNotOwned(t *testing.T) {
	live := rsLive(tpvcName, managedByArgoCD, tgoodRepo)
	rc, fc := newRecordingClient(t, live)

	plan := planDelete(rsDesired(tpvcName, tgoodRepo))
	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)

	assertCounts(t, res.Counts, 0, 0, 1, 0)
	assertOutcomeStatus(t, res.Attempted[0], executor.OpRefused, reasonNotOwned)
	for _, a := range rc.actions {
		if a.Verb == "delete" {
			t.Errorf("forbidden delete on inline-argo: %+v", a)
		}
	}
	// Verify still there.
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(rsGVK)
	if err := fc.Get(context.Background(), client.ObjectKey{Namespace: tns, Name: tpvcName}, got); err != nil {
		t.Errorf("inline-argo RS was deleted: %v", err)
	}
}

// Case 11: permissive + delete when live is missing → Succeeded
// (NotFound treated as idempotent success).
func TestExecute_Permissive_DeleteAbsent_SucceedsIdempotent(t *testing.T) {
	rc, _ := newRecordingClient(t)

	plan := planDelete(rsDesired(tpvcName, tgoodRepo))
	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)

	assertCounts(t, res.Counts, 0, 1, 0, 0)
	assertOutcomeStatus(t, res.Attempted[0], executor.OpSucceeded, "already-gone")
}

// ---- GVK allow-list paranoia (Cases 12, 13, 14) ----------------------------

// Case 12: permissive + hand-crafted Op with a Secret GVK → Refused
// ("forbidden-kind"), Secret in cluster unchanged.
func TestExecute_Permissive_ForeignKindSecret_RefusedForbidden(t *testing.T) {
	existingSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: tns, Name: "kopia-creds"},
		Data:       map[string][]byte{"password": []byte("supersecret")},
	}
	rc, fc := newRecordingClient(t, existingSecret)

	foreign := &unstructured.Unstructured{}
	foreign.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: kindSecret})
	foreign.SetNamespace(tns)
	foreign.SetName("kopia-creds")
	_ = unstructured.SetNestedField(foreign.Object, "evil", "data", "password")
	plan := planner.Plan{Ops: []planner.PlannedOp{{Kind: planner.OpUpdate, Resource: foreign}}}

	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)
	assertCounts(t, res.Counts, 0, 0, 1, 0)
	assertOutcomeStatus(t, res.Attempted[0], executor.OpRefused, reasonForbiddenKind)
	for _, a := range rc.actions {
		if a.GVK != rsGVKStr && a.GVK != rdGVKStr {
			t.Errorf("forbidden GVK reached client: %+v", a)
		}
	}
	// Secret value untouched.
	got := &corev1.Secret{}
	if err := fc.Get(context.Background(), client.ObjectKey{Namespace: tns, Name: "kopia-creds"}, got); err != nil {
		t.Fatalf("Secret gone: %v", err)
	}
	if string(got.Data["password"]) != "supersecret" {
		t.Errorf("Secret data mutated! got %q, want %q (forbidden-kind safety failed)", got.Data["password"], "supersecret")
	}
}

// Case 13: permissive + hand-crafted Op with PVC GVK → Refused.
func TestExecute_Permissive_ForeignKindPVC_RefusedForbidden(t *testing.T) {
	rc, _ := newRecordingClient(t)

	foreign := &unstructured.Unstructured{}
	foreign.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "PersistentVolumeClaim"})
	foreign.SetNamespace(tns)
	foreign.SetName("rogue-pvc")
	plan := planner.Plan{Ops: []planner.PlannedOp{{Kind: planner.OpDelete, Resource: foreign}}}

	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)
	assertCounts(t, res.Counts, 0, 0, 1, 0)
	assertOutcomeStatus(t, res.Attempted[0], executor.OpRefused, reasonForbiddenKind)
	if len(rc.actions) != 0 {
		t.Errorf("client touched on forbidden-kind PVC op: %+v", rc.actions)
	}
}

// Case 14: permissive + hand-crafted Op with MutatingWebhookConfiguration
// GVK → Refused.
func TestExecute_Permissive_ForeignKindWebhook_RefusedForbidden(t *testing.T) {
	rc, _ := newRecordingClient(t)

	foreign := &unstructured.Unstructured{}
	foreign.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "admissionregistration.k8s.io", Version: "v1", Kind: "MutatingWebhookConfiguration",
	})
	foreign.SetName("rogue-webhook")
	plan := planner.Plan{Ops: []planner.PlannedOp{{Kind: planner.OpCreate, Resource: foreign}}}

	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)
	assertCounts(t, res.Counts, 0, 0, 1, 0)
	assertOutcomeStatus(t, res.Attempted[0], executor.OpRefused, reasonForbiddenKind)
	if len(rc.actions) != 0 {
		t.Errorf("client touched on forbidden-kind webhook op: %+v", rc.actions)
	}
}

// ---- Idempotency (Case 15) -------------------------------------------------

// Case 15: run Execute with 2 create ops against empty cluster → both
// succeed. Re-run an Execute with the (now empty) plan you'd get from
// re-planning against the post-create state → no additional client
// calls. Total create count across both runs is exactly 2.
func TestExecute_Permissive_Idempotent_ReRunWithEmptyPlanIsNoOp(t *testing.T) {
	rc, _ := newRecordingClient(t)

	plan1 := planCreate(rsDesired(tpvcName, tgoodRepo), rdDesired(tdstName, tgoodRepo))
	res1 := executor.Execute(context.Background(), rc, mode.Permissive, plan1)
	if res1.Counts.Succeeded != 2 {
		t.Fatalf("first run: Succeeded got %d, want 2", res1.Counts.Succeeded)
	}
	firstRunActions := len(rc.actions)

	// Second pass: the planner would emit an empty Ops slice once
	// resources match. Simulate that here.
	res2 := executor.Execute(context.Background(), rc, mode.Permissive, planner.Plan{})
	if len(res2.Attempted) != 0 {
		t.Errorf("re-run with empty plan: Attempted got %d, want 0", len(res2.Attempted))
	}
	if len(rc.actions) != firstRunActions {
		t.Errorf("re-run produced extra actions: got %d, want %d (idempotency violation)",
			len(rc.actions)-firstRunActions, 0)
	}
}

// Case 15b: same idempotency check, but the second run carries the same
// create plan. The first creates succeed; the second is Refused("exists")
// for both ops, NOT a duplicate create. Each run reports its own
// terminal verdict via Counts.
//
// The recording client logs every CALL the executor makes — including
// calls that the apiserver subsequently rejects with AlreadyExists.
// That's intentional: the action log captures attempts, which is what
// the paranoia walk needs to prove no foreign GVKs were ever reached.
// Idempotency at the cluster level is asserted via Counts (Succeeded
// across both runs = 2; Refused on the second run = 2) and via a final
// re-read confirming each resource exists exactly once.
func TestExecute_Permissive_Idempotent_DuplicateCreatePlanRefusedExists(t *testing.T) {
	rc, fc := newRecordingClient(t)

	plan := planCreate(rsDesired(tpvcName, tgoodRepo), rdDesired(tdstName, tgoodRepo))
	res1 := executor.Execute(context.Background(), rc, mode.Permissive, plan)
	assertCounts(t, res1.Counts, 0, 2, 0, 0)

	plan2 := planCreate(rsDesired(tpvcName, tgoodRepo), rdDesired(tdstName, tgoodRepo))
	res2 := executor.Execute(context.Background(), rc, mode.Permissive, plan2)

	assertCounts(t, res2.Counts, 0, 0, 2, 0)
	for _, out := range res2.Attempted {
		assertOutcomeStatus(t, out, executor.OpRefused, reasonExists)
	}

	// Cluster-level idempotency: each resource exists exactly once
	// after both runs. Verified by Get + presence check.
	gotRS := &unstructured.Unstructured{}
	gotRS.SetGroupVersionKind(rsGVK)
	if err := fc.Get(context.Background(), client.ObjectKey{Namespace: tns, Name: tpvcName}, gotRS); err != nil {
		t.Errorf("RS missing after idempotent re-run: %v", err)
	}
	gotRD := &unstructured.Unstructured{}
	gotRD.SetGroupVersionKind(rdGVK)
	if err := fc.Get(context.Background(), client.ObjectKey{Namespace: tns, Name: tdstName}, gotRD); err != nil {
		t.Errorf("RD missing after idempotent re-run: %v", err)
	}
}

// ---- Paranoia walk (Case 16, mode parity tests) ----------------------------

// Case 16: cross-cutting paranoia. Run multiple Execute calls covering
// every code path (create / update / delete / refuse / forbidden-kind /
// idempotent re-run) against a shared recordingClient, then assert
// EVERY recorded write targets RS or RD.
func TestExecute_Permissive_ParanoiaWalk_OnlyRSOrRDGVKsEverTouched(t *testing.T) {
	// Seed: operator-owned RS to update, operator-owned RD to delete,
	// inline-argo RS to refuse, plus a Secret we'll try to write to.
	live := []client.Object{
		rsLive("op-owned-rs", managedByPVCPlumber, tdriftRepo),
		rdLive("op-owned-rd", managedByPVCPlumber, tdriftRepo),
		rsLive("argo-rs", managedByArgoCD, tgoodRepo),
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: tns, Name: "creds"}},
	}
	rc, _ := newRecordingClient(t, live...)

	// 1. Update op-owned-rs (succeed)
	executor.Execute(context.Background(), rc, mode.Permissive, planUpdate(rsDesired("op-owned-rs", tgoodRepo)))
	// 2. Delete op-owned-rd (succeed)
	executor.Execute(context.Background(), rc, mode.Permissive, planDelete(rdDesired("op-owned-rd", tgoodRepo)))
	// 3. Refused: update inline-argo
	executor.Execute(context.Background(), rc, mode.Permissive, planUpdate(rsDesired("argo-rs", tgoodRepo)))
	// 4. Forbidden-kind: Secret update
	foreign := &unstructured.Unstructured{}
	foreign.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: kindSecret})
	foreign.SetNamespace(tns)
	foreign.SetName("creds")
	executor.Execute(context.Background(), rc, mode.Permissive, planner.Plan{Ops: []planner.PlannedOp{{Kind: planner.OpUpdate, Resource: foreign}}})
	// 5. Create on absent (succeed)
	executor.Execute(context.Background(), rc, mode.Permissive, planCreate(rsDesired("new-rs", tgoodRepo)))

	assertAllActionsAreRSOrRD(t, rc.actions)
}

// Mode parity: Enforce and Strict behave identically to Permissive for
// the executor's mechanics in Patch 6.6. Differences in admission/deny
// semantics for those modes are deferred to Phase 8 webhook work.
func TestExecute_EnforceModeBehavesLikePermissive(t *testing.T) {
	rc, _ := newRecordingClient(t)
	plan := planCreate(rsDesired(tpvcName, tgoodRepo), rdDesired(tdstName, tgoodRepo))

	res := executor.Execute(context.Background(), rc, mode.Enforce, plan)
	assertCounts(t, res.Counts, 0, 2, 0, 0)
	assertAllActionsAreRSOrRD(t, rc.actions)
}

func TestExecute_StrictModeBehavesLikePermissive(t *testing.T) {
	rc, _ := newRecordingClient(t)
	plan := planCreate(rsDesired(tpvcName, tgoodRepo), rdDesired(tdstName, tgoodRepo))

	res := executor.Execute(context.Background(), rc, mode.Strict, plan)
	assertCounts(t, res.Counts, 0, 2, 0, 0)
	assertAllActionsAreRSOrRD(t, rc.actions)
}

// Empty plan: zero ops in, zero outcomes out, zero client calls.
func TestExecute_EmptyPlan_NoActions(t *testing.T) {
	rc, _ := newRecordingClient(t)
	res := executor.Execute(context.Background(), rc, mode.Permissive, planner.Plan{})

	assertCounts(t, res.Counts, 0, 0, 0, 0)
	if len(res.Attempted) != 0 {
		t.Errorf("Attempted: got %d, want 0", len(res.Attempted))
	}
	if len(rc.actions) != 0 {
		t.Errorf("recorded actions: got %d, want 0", len(rc.actions))
	}
}

// Nil Resource defensiveness: the executor refuses without crashing.
func TestExecute_Permissive_NilResource_Refused(t *testing.T) {
	rc, _ := newRecordingClient(t)
	plan := planner.Plan{Ops: []planner.PlannedOp{{Kind: planner.OpCreate, Resource: nil}}}

	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)
	assertCounts(t, res.Counts, 0, 0, 1, 0)
	assertOutcomeStatus(t, res.Attempted[0], executor.OpRefused, "nil-resource")
	if len(rc.actions) != 0 {
		t.Errorf("recorded actions on nil-resource: got %d, want 0", len(rc.actions))
	}
}

// Unknown OpKind defensiveness: refuses without touching the client.
func TestExecute_Permissive_UnknownOpKind_Refused(t *testing.T) {
	rc, _ := newRecordingClient(t)
	plan := planner.Plan{Ops: []planner.PlannedOp{{
		Kind:     planner.OpKind("invalid-kind"),
		Resource: rsDesired(tpvcName, tgoodRepo),
	}}}

	res := executor.Execute(context.Background(), rc, mode.Permissive, plan)
	assertCounts(t, res.Counts, 0, 0, 1, 0)
	if res.Attempted[0].Status != executor.OpRefused {
		t.Errorf("unknown kind Status: got %q, want refused", res.Attempted[0].Status)
	}
	if len(rc.actions) != 0 {
		t.Errorf("recorded actions on unknown-kind: got %d, want 0", len(rc.actions))
	}
}

// =============================================================================
// safety.go unit tests
// =============================================================================

func TestIsAllowedGVK(t *testing.T) {
	cases := []struct {
		gvk  schema.GroupVersionKind
		want bool
	}{
		{rsGVK, true},
		{rdGVK, true},
		{schema.GroupVersionKind{Group: "", Version: "v1", Kind: kindSecret}, false},
		{schema.GroupVersionKind{Group: "", Version: "v1", Kind: "PersistentVolumeClaim"}, false},
		{schema.GroupVersionKind{Group: "external-secrets.io", Version: "v1beta1", Kind: "ExternalSecret"}, false},
		{schema.GroupVersionKind{Group: "admissionregistration.k8s.io", Version: "v1", Kind: "MutatingWebhookConfiguration"}, false},
		{schema.GroupVersionKind{Group: groupVolSync, Version: versionV1Alpha, Kind: "Snapshot"}, false},     // wrong kind, right group
		{schema.GroupVersionKind{Group: "volsync.backube", Version: "v2", Kind: "ReplicationSource"}, false}, // wrong version
	}
	for _, c := range cases {
		if got := executor.IsAllowedGVK(c.gvk); got != c.want {
			t.Errorf("IsAllowedGVK(%v): got %v, want %v", c.gvk, got, c.want)
		}
	}
}

func TestIsOperatorOwned(t *testing.T) {
	cases := []struct {
		name string
		obj  *unstructured.Unstructured
		want bool
	}{
		{"nil", nil, false},
		{"no-labels", &unstructured.Unstructured{}, false},
		{"managed-by-pvc-plumber", rsLive(tpvcName, managedByPVCPlumber, tgoodRepo), true},
		{"managed-by-argocd", rsLive(tpvcName, managedByArgoCD, tgoodRepo), false},
		{"unlabeled", rsLive(tpvcName, "", tgoodRepo), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := executor.IsOperatorOwned(c.obj); got != c.want {
				t.Errorf("IsOperatorOwned: got %v, want %v", got, c.want)
			}
		})
	}
}
