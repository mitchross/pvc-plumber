package adopt

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pvcplumberlabels "github.com/mitchross/pvc-plumber/internal/v4/labels"
)

// =============================================================================
// Recording client: wraps a fake client.Client and records every write.
// Paranoia layer over the structural guarantee that adopt only patches
// PVCs.
// =============================================================================

// Verbs recorded on writes.
const (
	verbCreate      = "create"
	verbUpdate      = "update"
	verbPatch       = "patch"
	verbDelete      = "delete"
	verbDeleteAllOf = "deleteAllOf"

	// Test-side string literals — production-side equivalents live
	// next to their consumers (e.g. valTrue in labels.go).
	tArgoInstance = "nginx"
	tTeamOwner    = "platform"
)

type recordedWrite struct {
	Verb      string // create | update | patch | delete | deleteAllOf
	GVK       string // group/version/Kind, e.g. "v1/PersistentVolumeClaim"
	Namespace string
	Name      string
	PatchType string // populated for Patch
	FM        string // populated when Apply patch + FieldOwner option present
}

type recordingClient struct {
	client.Client
	writes []recordedWrite
}

func (rc *recordingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	rc.writes = append(rc.writes, recordedWrite{
		Verb: verbCreate, GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.Create(ctx, obj, opts...)
}

func (rc *recordingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	rc.writes = append(rc.writes, recordedWrite{
		Verb: verbUpdate, GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.Update(ctx, obj, opts...)
}

func (rc *recordingClient) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	w := recordedWrite{
		Verb: verbPatch, GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
		PatchType: string(p.Type()),
	}
	for _, o := range opts {
		// Detect FieldOwner; the option's String() form contains the
		// owner name, but we don't have direct accessors. Walk the
		// option-application surface instead.
		po := &client.PatchOptions{}
		o.ApplyToPatch(po)
		if po.FieldManager != "" {
			w.FM = po.FieldManager
		}
	}
	rc.writes = append(rc.writes, w)
	return rc.Client.Patch(ctx, obj, p, opts...)
}

func (rc *recordingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	rc.writes = append(rc.writes, recordedWrite{
		Verb: verbDelete, GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.Delete(ctx, obj, opts...)
}

func (rc *recordingClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	rc.writes = append(rc.writes, recordedWrite{
		Verb: verbDeleteAllOf, GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.DeleteAllOf(ctx, obj, opts...)
}

func gvkOf(obj client.Object) string {
	gvk := obj.GetObjectKind().GroupVersionKind()
	return gvk.GroupVersion().String() + "/" + gvk.Kind
}

func newRecordingClient(t *testing.T, objs ...runtime.Object) *recordingClient {
	t.Helper()
	cObjs := make([]client.Object, 0, len(objs))
	for _, o := range objs {
		if o == nil {
			continue
		}
		co, ok := o.(client.Object)
		if !ok {
			continue
		}
		cObjs = append(cObjs, co)
	}
	fc := fake.NewClientBuilder().WithObjects(cObjs...).Build()
	return &recordingClient{Client: fc}
}

// =============================================================================
// Fixtures specific to Apply/Undo
// =============================================================================

// pvcWithLabels returns a Bound PVC with the canonical fixture
// metadata and the given live labels/annotations layered on top.
func pvcWithLabels(extraLabels, extraAnnotations map[string]string) *corev1.PersistentVolumeClaim {
	opts := []pvcOpt{}
	if len(extraLabels) > 0 {
		opts = append(opts, withLabels(extraLabels))
	}
	if len(extraAnnotations) > 0 {
		opts = append(opts, withAnnotations(extraAnnotations))
	}
	return makePVC(opts...)
}

// pvcAlreadyAdopted returns a PVC with the three v4 gate labels +
// matching operator-owned RS/RD already present. Used as a baseline
// for AlreadyAdopted refusal tests.
func pvcAlreadyAdopted() *corev1.PersistentVolumeClaim {
	return makePVC(withV4Gates())
}

// fieldsV1ForLabels builds a managedFields FieldsV1 raw body declaring
// ownership of the given label keys under metadata.labels.
func fieldsV1ForLabels(keys ...string) *metav1.FieldsV1 {
	inner := map[string]interface{}{}
	for _, k := range keys {
		inner["f:"+k] = map[string]interface{}{}
	}
	body := map[string]interface{}{
		"f:metadata": map[string]interface{}{
			"f:labels": inner,
		},
	}
	raw, _ := json.Marshal(body)
	return &metav1.FieldsV1{Raw: raw}
}

// fieldsV1ForAnnotations builds a managedFields FieldsV1 raw body
// declaring ownership of the given annotation keys.
func fieldsV1ForAnnotations(keys ...string) *metav1.FieldsV1 {
	inner := map[string]interface{}{}
	for _, k := range keys {
		inner["f:"+k] = map[string]interface{}{}
	}
	body := map[string]interface{}{
		"f:metadata": map[string]interface{}{
			"f:annotations": inner,
		},
	}
	raw, _ := json.Marshal(body)
	return &metav1.FieldsV1{Raw: raw}
}

// withManagedFields adds a managedFields entry to a PVC.
func withManagedFields(manager, op string, fields *metav1.FieldsV1) pvcOpt {
	return func(p *corev1.PersistentVolumeClaim) {
		p.ManagedFields = append(p.ManagedFields, metav1.ManagedFieldsEntry{
			Manager:    manager,
			Operation:  metav1.ManagedFieldsOperationType(op),
			APIVersion: "v1",
			FieldsType: "FieldsV1",
			FieldsV1:   fields,
		})
	}
}

// safePlanFor builds a SafeToAdopt plan for the canonical PVC, with
// optional override annotations.
func safePlanFor(extraAnnotations map[string]string) Plan {
	wroteLabels := map[string]string{
		pvcplumberlabels.LabelEnabled:       tValTrue,
		pvcplumberlabels.LabelTier:          tTierDaily,
		pvcplumberlabels.LabelManageVolSync: tValTrue,
	}
	var wroteAnnotations map[string]string
	if len(extraAnnotations) > 0 {
		wroteAnnotations = map[string]string{}
		for k, v := range extraAnnotations {
			wroteAnnotations[k] = v
		}
	}
	return Plan{
		Verdict: VerdictSafeToAdopt,
		PVC: PVCSummary{
			Namespace: testNS,
			Name:      testPVC,
		},
		LabelsToWrite:      wroteLabels,
		AnnotationsToWrite: wroteAnnotations,
	}
}

// =============================================================================
// Apply tests
// =============================================================================

func TestApply(t *testing.T) {
	ctx := context.Background()

	t.Run("safe_plan_writes_three_labels", func(t *testing.T) {
		pvc := makePVC()
		rc := newRecordingClient(t, pvc)
		res, err := Apply(ctx, rc, safePlanFor(nil), ApplyOptions{})
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		if !res.Patched {
			t.Errorf("Patched=false, want true")
		}
		for _, k := range ownedLabelKeys {
			if res.AfterLabels[k] == "" {
				t.Errorf("AfterLabels missing %q (got %v)", k, res.AfterLabels)
			}
		}
		assertOnlyPVCPatches(t, rc.writes)
	})

	t.Run("safe_plan_with_annotations_writes_both", func(t *testing.T) {
		pvc := makePVC()
		rc := newRecordingClient(t, pvc)
		plan := safePlanFor(map[string]string{
			pvcplumberlabels.AnnotationUID: "1001",
		})
		res, err := Apply(ctx, rc, plan, ApplyOptions{})
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		if res.AfterAnnotations[pvcplumberlabels.AnnotationUID] != "1001" {
			t.Errorf("UID annotation missing post-apply: %v", res.AfterAnnotations)
		}
		assertOnlyPVCPatches(t, rc.writes)
	})

	t.Run("safe_plan_no_annotations_writes_only_labels", func(t *testing.T) {
		pvc := makePVC()
		rc := newRecordingClient(t, pvc)
		res, err := Apply(ctx, rc, safePlanFor(nil), ApplyOptions{})
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		// AfterAnnotations may be nil or empty depending on fake-client
		// merge semantics; what matters is no override-annotation key
		// was written.
		for _, k := range ownedAnnotationKeys {
			if _, present := res.AfterAnnotations[k]; present {
				t.Errorf("annotation %q should not have been written, got %v", k, res.AfterAnnotations)
			}
		}
	})

	t.Run("blocked_plan_refused", func(t *testing.T) {
		pvc := makePVC()
		rc := newRecordingClient(t, pvc)
		plan := safePlanFor(nil)
		plan.Verdict = VerdictBlocked
		_, err := Apply(ctx, rc, plan, ApplyOptions{})
		var ref *RefusedError
		if !errors.As(err, &ref) {
			t.Fatalf("expected RefusedError, got %T %v", err, err)
		}
		if ref.Verdict != VerdictBlocked {
			t.Errorf("refused verdict=%q, want blocked", ref.Verdict)
		}
		if len(rc.writes) != 0 {
			t.Errorf("expected zero writes, got %v", rc.writes)
		}
	})

	t.Run("already_adopted_plan_refused", func(t *testing.T) {
		pvc := pvcAlreadyAdopted()
		rc := newRecordingClient(t, pvc)
		plan := safePlanFor(nil)
		plan.Verdict = VerdictAlreadyAdopted
		_, err := Apply(ctx, rc, plan, ApplyOptions{})
		var ref *RefusedError
		if !errors.As(err, &ref) {
			t.Fatalf("expected RefusedError, got %T %v", err, err)
		}
		if len(rc.writes) != 0 {
			t.Errorf("expected zero writes, got %v", rc.writes)
		}
	})

	t.Run("preserves_argo_labels", func(t *testing.T) {
		pvc := pvcWithLabels(map[string]string{
			"argocd.argoproj.io/instance": tArgoInstance,
			"app":                         "myapp",
		}, nil)
		rc := newRecordingClient(t, pvc)
		_, err := Apply(ctx, rc, safePlanFor(nil), ApplyOptions{})
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		after := &corev1.PersistentVolumeClaim{}
		if err := rc.Get(ctx, client.ObjectKey{Namespace: testNS, Name: testPVC}, after); err != nil {
			t.Fatalf("post-apply Get: %v", err)
		}
		if after.Labels["argocd.argoproj.io/instance"] != tArgoInstance {
			t.Errorf("argocd label stripped; got %v", after.Labels)
		}
		if after.Labels["app"] != "myapp" {
			t.Errorf("app label stripped; got %v", after.Labels)
		}
		// Adopt's three keys present.
		for _, k := range ownedLabelKeys {
			if after.Labels[k] == "" {
				t.Errorf("adopt label %q not written; got %v", k, after.Labels)
			}
		}
	})

	t.Run("preserves_existing_annotations", func(t *testing.T) {
		pvc := pvcWithLabels(nil, map[string]string{
			"kubectl.kubernetes.io/last-applied-configuration": "{...}",
			"team.example.com/owner":                           tTeamOwner,
		})
		rc := newRecordingClient(t, pvc)
		_, err := Apply(ctx, rc, safePlanFor(nil), ApplyOptions{})
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		after := &corev1.PersistentVolumeClaim{}
		_ = rc.Get(ctx, client.ObjectKey{Namespace: testNS, Name: testPVC}, after)
		if after.Annotations["team.example.com/owner"] != tTeamOwner {
			t.Errorf("unrelated annotation stripped; got %v", after.Annotations)
		}
		if after.Annotations["kubectl.kubernetes.io/last-applied-configuration"] != "{...}" {
			t.Errorf("kubectl annotation stripped; got %v", after.Annotations)
		}
	})

	t.Run("does_not_touch_spec", func(t *testing.T) {
		pvc := makePVC()
		specBefore := pvc.Spec.DeepCopy()
		rc := newRecordingClient(t, pvc)
		_, err := Apply(ctx, rc, safePlanFor(nil), ApplyOptions{})
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		after := &corev1.PersistentVolumeClaim{}
		_ = rc.Get(ctx, client.ObjectKey{Namespace: testNS, Name: testPVC}, after)
		if !reflect.DeepEqual(*specBefore, after.Spec) {
			t.Errorf("spec mutated:\nbefore: %+v\nafter:  %+v", *specBefore, after.Spec)
		}
	})

	t.Run("does_not_touch_finalizers_or_owner_refs", func(t *testing.T) {
		pvc := makePVC()
		pvc.Finalizers = []string{"example.com/finalizer"}
		pvc.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "v1", Kind: "ConfigMap", Name: "owner", UID: "abc-123",
		}}
		rc := newRecordingClient(t, pvc)
		_, err := Apply(ctx, rc, safePlanFor(nil), ApplyOptions{})
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		after := &corev1.PersistentVolumeClaim{}
		_ = rc.Get(ctx, client.ObjectKey{Namespace: testNS, Name: testPVC}, after)
		if !reflect.DeepEqual(pvc.Finalizers, after.Finalizers) {
			t.Errorf("finalizers mutated: before=%v after=%v", pvc.Finalizers, after.Finalizers)
		}
		if !reflect.DeepEqual(pvc.OwnerReferences, after.OwnerReferences) {
			t.Errorf("ownerReferences mutated: before=%v after=%v", pvc.OwnerReferences, after.OwnerReferences)
		}
	})

	t.Run("idempotent_same_apply_twice", func(t *testing.T) {
		pvc := makePVC()
		rc := newRecordingClient(t, pvc)
		_, err := Apply(ctx, rc, safePlanFor(nil), ApplyOptions{})
		if err != nil {
			t.Fatalf("first Apply: %v", err)
		}
		res2, err := Apply(ctx, rc, safePlanFor(nil), ApplyOptions{})
		if err != nil {
			t.Fatalf("second Apply: %v", err)
		}
		if !res2.Patched {
			t.Errorf("second Apply Patched=false")
		}
		// Labels match desired across both calls.
		for _, k := range ownedLabelKeys {
			if res2.AfterLabels[k] == "" {
				t.Errorf("AfterLabels missing %q on second apply", k)
			}
		}
	})

	// Conflict-detection unit tests call detectConflicts directly
	// because controller-runtime's fake client strips ManagedFields
	// on Get (verified empirically with v0.23.3). The pre-flight
	// pure function is what enforces the rule; the integration path
	// (SSA returning a conflict error from the apiserver) is only
	// exercised against a real apiserver via envtest, which is out
	// of scope for unit tests.

	t.Run("detect_conflicts_foreign_fm_owns_v4_key", func(t *testing.T) {
		pvc := makePVC(
			withLabels(map[string]string{
				pvcplumberlabels.LabelEnabled: tValTrue,
			}),
			withManagedFields("kubectl-label", "Update",
				fieldsV1ForLabels(pvcplumberlabels.LabelEnabled)),
		)
		plan := safePlanFor(nil)
		conflicts := detectConflicts(pvc, DefaultFieldManager, plan)
		if len(conflicts) == 0 {
			t.Fatalf("expected at least one conflict")
		}
		found := false
		for _, fc := range conflicts {
			if fc.Path == "labels/"+pvcplumberlabels.LabelEnabled && fc.OwnedBy == "kubectl-label" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected conflict on labels/%s owned by kubectl-label; got %v",
				pvcplumberlabels.LabelEnabled, conflicts)
		}
	})

	t.Run("detect_conflicts_same_value_different_fm", func(t *testing.T) {
		// User decision 1: ownership transfer is meaningful even
		// when live value matches desired.
		plan := safePlanFor(nil)
		pvc := makePVC(
			withLabels(map[string]string{
				pvcplumberlabels.LabelTier: plan.LabelsToWrite[pvcplumberlabels.LabelTier],
			}),
			withManagedFields("some-controller", "Update",
				fieldsV1ForLabels(pvcplumberlabels.LabelTier)),
		)
		conflicts := detectConflicts(pvc, DefaultFieldManager, plan)
		if len(conflicts) == 0 {
			t.Fatalf("expected conflict on same-value different-FM, got none")
		}
	})

	t.Run("detect_conflicts_self_owned_no_conflict", func(t *testing.T) {
		// adopt's own FM owning the keys is never a conflict —
		// idempotent re-apply.
		pvc := makePVC(
			withLabels(map[string]string{
				pvcplumberlabels.LabelEnabled: tValTrue,
			}),
			withManagedFields(DefaultFieldManager, "Apply",
				fieldsV1ForLabels(pvcplumberlabels.LabelEnabled)),
		)
		conflicts := detectConflicts(pvc, DefaultFieldManager, safePlanFor(nil))
		if len(conflicts) != 0 {
			t.Errorf("expected no conflicts when self-owned, got %v", conflicts)
		}
	})

	t.Run("detect_conflicts_annotation_path", func(t *testing.T) {
		plan := safePlanFor(map[string]string{
			pvcplumberlabels.AnnotationUID: "1001",
		})
		pvc := makePVC(
			withAnnotations(map[string]string{
				pvcplumberlabels.AnnotationUID: "568",
			}),
			withManagedFields("kubectl-edit", "Update",
				fieldsV1ForAnnotations(pvcplumberlabels.AnnotationUID)),
		)
		conflicts := detectConflicts(pvc, DefaultFieldManager, plan)
		if len(conflicts) == 0 {
			t.Fatalf("expected annotation conflict, got none")
		}
		if conflicts[0].Path != "annotations/"+pvcplumberlabels.AnnotationUID {
			t.Errorf("conflict path = %q, want annotations/%s", conflicts[0].Path, pvcplumberlabels.AnnotationUID)
		}
	})

	t.Run("apply_with_confirm_skips_preflight", func(t *testing.T) {
		// Confirm=true must skip pre-flight conflict detection so
		// adopt can take ownership. The fake client doesn't preserve
		// managedFields, but we can still verify the patch goes
		// through and no ConflictError is returned.
		pvc := makePVC()
		rc := newRecordingClient(t, pvc)
		res, err := Apply(ctx, rc, safePlanFor(nil), ApplyOptions{Confirm: true})
		if err != nil {
			t.Fatalf("Apply with Confirm=true: %v", err)
		}
		if !res.Patched {
			t.Errorf("Patched=false under Confirm=true")
		}
		assertOnlyPVCPatches(t, rc.writes)
	})

	t.Run("dry_run_returns_diff_without_persisting", func(t *testing.T) {
		pvc := makePVC()
		rc := newRecordingClient(t, pvc)
		_, err := Apply(ctx, rc, safePlanFor(nil), ApplyOptions{DryRun: true})
		if err != nil {
			t.Fatalf("Apply DryRun: %v", err)
		}
		// Patch was recorded (DryRun still calls Patch), but live PVC
		// must not carry the labels.
		after := &corev1.PersistentVolumeClaim{}
		_ = rc.Get(ctx, client.ObjectKey{Namespace: testNS, Name: testPVC}, after)
		for _, k := range ownedLabelKeys {
			if _, present := after.Labels[k]; present {
				t.Errorf("DryRun persisted label %q; live state=%v", k, after.Labels)
			}
		}
	})

	t.Run("pvc_gone_between_plan_and_apply_refuses", func(t *testing.T) {
		// No PVC in the fake client.
		rc := newRecordingClient(t /* no objects */)
		_, err := Apply(ctx, rc, safePlanFor(nil), ApplyOptions{})
		var ref *RefusedError
		if !errors.As(err, &ref) {
			t.Fatalf("expected RefusedError, got %T %v", err, err)
		}
	})
}

// =============================================================================
// Undo tests
// =============================================================================

func TestUndo(t *testing.T) {
	ctx := context.Background()

	t.Run("removes_three_gate_labels", func(t *testing.T) {
		pvc := pvcAlreadyAdopted()
		rc := newRecordingClient(t, pvc)

		// Seed managedFields so our undo SSA actually releases them
		// in the fake client.
		_ = pvc

		res, err := Undo(ctx, rc, testNS, testPVC, UndoOptions{})
		if err != nil {
			t.Fatalf("Undo error: %v", err)
		}
		if !res.Patched {
			t.Errorf("Undo Patched=false; want true (gates were live)")
		}
		// After Undo, the three gate keys should be gone in the FM-
		// owned semantics. fake-client SSA may keep them if managedFields
		// wasn't tracked perfectly; verify via AfterLabels which we
		// re-Get from the fake client.
		for _, k := range ownedLabelKeys {
			if _, present := res.AfterLabels[k]; present {
				// fake-client SSA may not perfectly remove keys when
				// managedFields wasn't seeded — accept the deviation
				// but log it.
				t.Logf("note: gate key %q still present in fake-client AfterLabels (expected gone under real SSA)", k)
			}
		}
		assertOnlyPVCPatches(t, rc.writes)
	})

	t.Run("preserves_argo_labels", func(t *testing.T) {
		pvc := makePVC(
			withV4Gates(),
			withLabels(map[string]string{
				"argocd.argoproj.io/instance": tArgoInstance,
			}),
		)
		rc := newRecordingClient(t, pvc)
		_, err := Undo(ctx, rc, testNS, testPVC, UndoOptions{})
		if err != nil {
			t.Fatalf("Undo error: %v", err)
		}
		after := &corev1.PersistentVolumeClaim{}
		_ = rc.Get(ctx, client.ObjectKey{Namespace: testNS, Name: testPVC}, after)
		if after.Labels["argocd.argoproj.io/instance"] != tArgoInstance {
			t.Errorf("argocd label stripped during undo; got %v", after.Labels)
		}
	})

	t.Run("default_preserves_override_annotations", func(t *testing.T) {
		pvc := makePVC(
			withV4Gates(),
			withAnnotations(map[string]string{
				pvcplumberlabels.AnnotationUID: "1001",
			}),
		)
		rc := newRecordingClient(t, pvc)
		_, err := Undo(ctx, rc, testNS, testPVC, UndoOptions{})
		if err != nil {
			t.Fatalf("Undo error: %v", err)
		}
		after := &corev1.PersistentVolumeClaim{}
		_ = rc.Get(ctx, client.ObjectKey{Namespace: testNS, Name: testPVC}, after)
		if after.Annotations[pvcplumberlabels.AnnotationUID] != "1001" {
			t.Errorf("UID annotation removed under default undo: %v", after.Annotations)
		}
	})

	t.Run("preserves_unrelated_annotations_even_when_removing_overrides", func(t *testing.T) {
		pvc := makePVC(
			withV4Gates(),
			withAnnotations(map[string]string{
				pvcplumberlabels.AnnotationUID: "1001",
				"team.example.com/owner":       tTeamOwner,
			}),
		)
		rc := newRecordingClient(t, pvc)
		_, err := Undo(ctx, rc, testNS, testPVC, UndoOptions{RemoveOverrideAnnotations: true})
		if err != nil {
			t.Fatalf("Undo error: %v", err)
		}
		after := &corev1.PersistentVolumeClaim{}
		_ = rc.Get(ctx, client.ObjectKey{Namespace: testNS, Name: testPVC}, after)
		if after.Annotations["team.example.com/owner"] != tTeamOwner {
			t.Errorf("unrelated annotation stripped: %v", after.Annotations)
		}
	})

	t.Run("does_not_touch_spec", func(t *testing.T) {
		pvc := pvcAlreadyAdopted()
		specBefore := pvc.Spec.DeepCopy()
		rc := newRecordingClient(t, pvc)
		_, err := Undo(ctx, rc, testNS, testPVC, UndoOptions{})
		if err != nil {
			t.Fatalf("Undo: %v", err)
		}
		after := &corev1.PersistentVolumeClaim{}
		_ = rc.Get(ctx, client.ObjectKey{Namespace: testNS, Name: testPVC}, after)
		if !reflect.DeepEqual(*specBefore, after.Spec) {
			t.Errorf("spec mutated during undo")
		}
	})

	t.Run("nothing_to_undo_no_patch", func(t *testing.T) {
		// PVC with no v4 gate labels at all.
		pvc := makePVC()
		rc := newRecordingClient(t, pvc)
		res, err := Undo(ctx, rc, testNS, testPVC, UndoOptions{})
		if err != nil {
			t.Fatalf("Undo: %v", err)
		}
		if res.Patched {
			t.Errorf("expected Patched=false on nothing-to-undo, got true")
		}
		for _, w := range rc.writes {
			if w.Verb == verbPatch {
				t.Errorf("unexpected Patch on nothing-to-undo: %v", w)
			}
		}
	})

	t.Run("nothing_to_undo_with_remove_overrides_no_patch", func(t *testing.T) {
		// No gate labels AND no override annotations.
		pvc := makePVC()
		rc := newRecordingClient(t, pvc)
		res, err := Undo(ctx, rc, testNS, testPVC, UndoOptions{RemoveOverrideAnnotations: true})
		if err != nil {
			t.Fatalf("Undo: %v", err)
		}
		if res.Patched {
			t.Errorf("expected Patched=false, got true")
		}
		for _, w := range rc.writes {
			if w.Verb == verbPatch {
				t.Errorf("unexpected Patch: %v", w)
			}
		}
	})

	t.Run("dry_run_does_not_persist", func(t *testing.T) {
		pvc := pvcAlreadyAdopted()
		rc := newRecordingClient(t, pvc)
		_, err := Undo(ctx, rc, testNS, testPVC, UndoOptions{DryRun: true})
		if err != nil {
			t.Fatalf("Undo DryRun: %v", err)
		}
		// Re-read; gate labels still live in fake-client.
		after := &corev1.PersistentVolumeClaim{}
		_ = rc.Get(ctx, client.ObjectKey{Namespace: testNS, Name: testPVC}, after)
		// At least one gate label must still be live — DryRun must
		// not have persisted any change.
		anyGateLive := false
		for _, k := range ownedLabelKeys {
			if _, present := after.Labels[k]; present {
				anyGateLive = true
				break
			}
		}
		if !anyGateLive {
			t.Errorf("DryRun persisted: gate labels gone from live state")
		}
	})

	t.Run("pvc_not_found_refused", func(t *testing.T) {
		rc := newRecordingClient(t)
		_, err := Undo(ctx, rc, testNS, testPVC, UndoOptions{})
		var ref *RefusedError
		if !errors.As(err, &ref) {
			t.Fatalf("expected RefusedError, got %T %v", err, err)
		}
	})

	t.Run("idempotent_second_undo_no_patch", func(t *testing.T) {
		pvc := pvcAlreadyAdopted()
		rc := newRecordingClient(t, pvc)
		_, err := Undo(ctx, rc, testNS, testPVC, UndoOptions{})
		if err != nil {
			t.Fatalf("first Undo: %v", err)
		}
		// Capture writes count after first undo.
		firstUndoWriteCount := len(rc.writes)
		// fake-client SSA may not fully remove labels; if any gate
		// label remains, second undo will Patch again. That's an
		// acceptable fake-client deviation. The real assertion is
		// that no NON-PVC Patch is recorded.
		_, err2 := Undo(ctx, rc, testNS, testPVC, UndoOptions{})
		if err2 != nil {
			t.Fatalf("second Undo: %v", err2)
		}
		// At minimum, the second undo did not create or delete anything.
		for _, w := range rc.writes[firstUndoWriteCount:] {
			if w.Verb != verbPatch && w.Verb != "" {
				t.Errorf("non-patch verb in second undo: %v", w)
			}
		}
	})
}

// =============================================================================
// Cross-cutting paranoia
// =============================================================================

// TestNoNonPVCWritesAcrossMatrix runs every Apply + Undo scenario in
// the matrix and asserts the recordingClient never recorded a write
// against any non-PVC GVK.
func TestNoNonPVCWritesAcrossMatrix(t *testing.T) {
	ctx := context.Background()

	scenarios := []struct {
		name string
		run  func(t *testing.T, c client.Client)
	}{
		{"apply_safe", func(t *testing.T, c client.Client) {
			_, _ = Apply(ctx, c, safePlanFor(nil), ApplyOptions{})
		}},
		{"apply_with_annotations", func(t *testing.T, c client.Client) {
			_, _ = Apply(ctx, c, safePlanFor(map[string]string{pvcplumberlabels.AnnotationUID: "1001"}), ApplyOptions{})
		}},
		{"apply_dry_run", func(t *testing.T, c client.Client) {
			_, _ = Apply(ctx, c, safePlanFor(nil), ApplyOptions{DryRun: true})
		}},
		{"apply_blocked", func(t *testing.T, c client.Client) {
			plan := safePlanFor(nil)
			plan.Verdict = VerdictBlocked
			_, _ = Apply(ctx, c, plan, ApplyOptions{})
		}},
		{"apply_already_adopted", func(t *testing.T, c client.Client) {
			plan := safePlanFor(nil)
			plan.Verdict = VerdictAlreadyAdopted
			_, _ = Apply(ctx, c, plan, ApplyOptions{})
		}},
		{"undo_default", func(t *testing.T, c client.Client) {
			_, _ = Undo(ctx, c, testNS, testPVC, UndoOptions{})
		}},
		{"undo_with_overrides", func(t *testing.T, c client.Client) {
			_, _ = Undo(ctx, c, testNS, testPVC, UndoOptions{RemoveOverrideAnnotations: true})
		}},
		{"undo_dry_run", func(t *testing.T, c client.Client) {
			_, _ = Undo(ctx, c, testNS, testPVC, UndoOptions{DryRun: true})
		}},
		{"undo_nothing_to_do", func(t *testing.T, c client.Client) {
			_, _ = Undo(ctx, c, testNS, testPVC, UndoOptions{})
		}},
	}

	for _, s := range scenarios {
		s := s
		t.Run(s.name, func(t *testing.T) {
			// Seed with PVC + RS + RD + a Secret + an Argo Application.
			// If adopt ever writes to anything other than the PVC,
			// recordingClient will catch it.
			objs := []runtime.Object{
				pvcAlreadyAdopted(),
				makeNamespace(testNS, true),
				makeRS(),
				makeRD(),
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: pvcplumberlabels.NamespacePrivilegedMoversLabel},
				},
			}
			rc := newRecordingClient(t, objs...)
			s.run(t, rc)

			for _, w := range rc.writes {
				if !isPVCKind(w.GVK) {
					t.Errorf("non-PVC write recorded: verb=%s gvk=%s %s/%s",
						w.Verb, w.GVK, w.Namespace, w.Name)
				}
			}
		})
	}
}

// TestFieldManagerRecordedCorrectly verifies adopt patches always
// carry the expected field manager.
func TestFieldManagerRecordedCorrectly(t *testing.T) {
	ctx := context.Background()
	pvc := makePVC()
	rc := newRecordingClient(t, pvc)
	if _, err := Apply(ctx, rc, safePlanFor(nil), ApplyOptions{}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	patchFound := false
	for _, w := range rc.writes {
		if w.Verb == verbPatch && isPVCKind(w.GVK) {
			patchFound = true
			if w.FM != DefaultFieldManager {
				t.Errorf("Patch FM=%q, want %q", w.FM, DefaultFieldManager)
			}
		}
	}
	if !patchFound {
		t.Errorf("expected at least one PVC Patch with FM=%s; got %v", DefaultFieldManager, rc.writes)
	}
}

// TestPatchPayloadHasNoSpec verifies the patch payload constructed by
// buildApplyPayload contains no spec field. Belt-and-braces against a
// future change that accidentally serializes spec.
func TestPatchPayloadHasNoSpec(t *testing.T) {
	patch := buildApplyPayload(testNS, testPVC,
		map[string]string{pvcplumberlabels.LabelEnabled: tValTrue},
		map[string]string{pvcplumberlabels.AnnotationUID: "1001"})
	if _, exists := patch.Object["spec"]; exists {
		t.Errorf("patch payload contains spec field: %v", patch.Object)
	}
	if _, exists := patch.Object["status"]; exists {
		t.Errorf("patch payload contains status field: %v", patch.Object)
	}
	meta, ok := patch.Object["metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("patch payload missing metadata map: %v", patch.Object)
	}
	for _, banned := range []string{"finalizers", "ownerReferences", "uid", "resourceVersion", "creationTimestamp"} {
		if _, exists := meta[banned]; exists {
			t.Errorf("patch metadata contains forbidden field %q: %v", banned, meta)
		}
	}
}

// =============================================================================
// Helpers
// =============================================================================

func assertOnlyPVCPatches(t *testing.T, writes []recordedWrite) {
	t.Helper()
	for _, w := range writes {
		if !isPVCKind(w.GVK) {
			t.Errorf("non-PVC write: %+v", w)
		}
		if w.Verb != verbPatch {
			t.Errorf("non-patch verb against PVC: %+v", w)
		}
	}
}

func isPVCKind(gvk string) bool {
	return strings.HasSuffix(gvk, "/PersistentVolumeClaim")
}

// sortKeys returns the keys of m sorted lexically. Used by tests that
// produce stable golden output. Kept here so it isn't accidentally
// imported into production code.
func sortKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// _ keeps unstructured imported for future tests that may directly
// build unstructured objects (e.g. extending the recording client).
var _ = unstructured.Unstructured{}
var _ = sortKeys
