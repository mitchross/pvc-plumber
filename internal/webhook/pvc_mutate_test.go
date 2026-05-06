package webhook

import (
	"context"
	"encoding/json"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	jsonpatchgomod "gomodules.xyz/jsonpatch/v2"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// fakeKopia implements kopiaClient for table-driven tests. It returns a
// canned CheckResult and records the calls it received so tests can assert
// the handler did or did not consult the backend.
type fakeKopia struct {
	result backend.CheckResult
	calls  []string // namespace/pvc keys, in call order
}

func (f *fakeKopia) CheckBackupExists(_ context.Context, ns, pvc string) backend.CheckResult {
	f.calls = append(f.calls, ns+"/"+pvc)
	return f.result
}

// newDecoder builds an admission.Decoder backed by client-go's scheme. The
// scheme already has core/v1 and batch/v1 registered, which is everything
// our handlers decode.
func newDecoder(t *testing.T) admission.Decoder {
	t.Helper()
	return admission.NewDecoder(clientgoscheme.Scheme)
}

// pvcRequest builds a CREATE admission.Request for the given PVC. We don't
// rely on a fully-formed AdmissionReview — the controller-runtime webhook
// machinery only needs Operation, Object.Raw, and a Kind hint to decode.
func pvcRequest(t *testing.T, pvc *corev1.PersistentVolumeClaim) admission.Request {
	t.Helper()
	raw, err := json.Marshal(pvc)
	if err != nil {
		t.Fatalf("marshal pvc: %v", err)
	}
	return admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: raw},
		},
	}
}

func backupPVC(name, ns string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"backup": "hourly"},
		},
	}
}

// applyPatch applies a JSON-patch response to the original raw object so
// tests can assert on the post-mutation shape rather than walking patch ops.
// controller-runtime returns its patches as `[]gomodules.xyz/jsonpatch/v2.Operation`,
// which is wire-compatible with RFC 6902, so we marshal it and feed it to
// `evanphx/json-patch` (an indirect dep) for execution.
func applyPatch(t *testing.T, original []byte, patches []jsonpatchgomod.JsonPatchOperation) []byte {
	t.Helper()
	patchJSON, err := json.Marshal(patches)
	if err != nil {
		t.Fatalf("marshal patch: %v", err)
	}
	decoded, err := jsonpatch.DecodePatch(patchJSON)
	if err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	out, err := decoded.Apply(original)
	if err != nil {
		t.Fatalf("apply patch: %v", err)
	}
	return out
}

func TestPVCMutate_RestoreDecision_PatchesDataSourceRef(t *testing.T) {
	pvc := backupPVC("blue", "media")
	kopia := &fakeKopia{result: backend.CheckResult{
		Exists:        true,
		Decision:      backend.DecisionRestore,
		Authoritative: true,
	}}
	mut := &PVCMutator{Decoder: newDecoder(t), Kopia: kopia}

	req := pvcRequest(t, pvc)
	resp := mut.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected allowed=true, got denied: %v", resp.Result)
	}
	if len(resp.Patches) == 0 {
		t.Fatalf("expected at least one patch op, got none")
	}

	patched := applyPatch(t, req.Object.Raw, resp.Patches)
	got := &corev1.PersistentVolumeClaim{}
	if err := json.Unmarshal(patched, got); err != nil {
		t.Fatalf("unmarshal patched: %v", err)
	}
	if got.Spec.DataSourceRef == nil {
		t.Fatalf("expected dataSourceRef to be set after patch")
	}
	if got.Spec.DataSourceRef.APIGroup == nil || *got.Spec.DataSourceRef.APIGroup != "volsync.backube" {
		t.Errorf("apiGroup mismatch: %+v", got.Spec.DataSourceRef.APIGroup)
	}
	if got.Spec.DataSourceRef.Kind != "ReplicationDestination" {
		t.Errorf("kind mismatch: %s", got.Spec.DataSourceRef.Kind)
	}
	if got.Spec.DataSourceRef.Name != "blue-backup" {
		t.Errorf("name mismatch: %s", got.Spec.DataSourceRef.Name)
	}
}

func TestPVCMutate_FreshDecision_NoPatch(t *testing.T) {
	kopia := &fakeKopia{result: backend.CheckResult{
		Decision:      backend.DecisionFresh,
		Authoritative: true,
	}}
	mut := &PVCMutator{Decoder: newDecoder(t), Kopia: kopia}

	resp := mut.Handle(context.Background(), pvcRequest(t, backupPVC("blue", "media")))
	if !resp.Allowed {
		t.Fatalf("expected allowed=true on fresh decision")
	}
	if len(resp.Patches) != 0 {
		t.Errorf("expected no patches on fresh decision, got %d", len(resp.Patches))
	}
}

func TestPVCMutate_KopiaError_AllowsFailOpen(t *testing.T) {
	// Fail-OPEN is the whole point of the mutator — error must NOT deny.
	// The validator is the fail-closed companion.
	kopia := &fakeKopia{result: backend.CheckResult{
		Decision:      backend.DecisionUnknown,
		Authoritative: false,
		Error:         "kopia repository not connected",
	}}
	mut := &PVCMutator{Decoder: newDecoder(t), Kopia: kopia}

	resp := mut.Handle(context.Background(), pvcRequest(t, backupPVC("blue", "media")))
	if !resp.Allowed {
		t.Fatalf("expected allowed=true on kopia error (fail-open), got denied: %v", resp.Result)
	}
	if len(resp.Patches) != 0 {
		t.Errorf("expected no patches when kopia errors, got %d", len(resp.Patches))
	}
}

func TestPVCMutate_DataSourceRefAlreadySet_NoOverwrite(t *testing.T) {
	pvc := backupPVC("blue", "media")
	pvc.Spec.DataSourceRef = &corev1.TypedObjectReference{
		APIGroup: ptr.To("snapshot.storage.k8s.io"),
		Kind:     "VolumeSnapshot",
		Name:     "operator-chosen",
	}
	kopia := &fakeKopia{result: backend.CheckResult{
		Exists:        true,
		Decision:      backend.DecisionRestore,
		Authoritative: true,
	}}
	mut := &PVCMutator{Decoder: newDecoder(t), Kopia: kopia}

	resp := mut.Handle(context.Background(), pvcRequest(t, pvc))
	if !resp.Allowed {
		t.Fatalf("expected allowed=true when dataSourceRef preset")
	}
	if len(resp.Patches) != 0 {
		t.Errorf("expected no patches (operator chose dataSourceRef), got %d", len(resp.Patches))
	}
	// Defense in depth: we should never even consult Kopia when the caller
	// already specified a data source — the precondition skips before then.
	if len(kopia.calls) != 0 {
		t.Errorf("expected zero kopia calls when dataSourceRef preset, got %d", len(kopia.calls))
	}
}

func TestPVCMutate_NoBackupLabel_AllowsWithoutCheck(t *testing.T) {
	pvc := backupPVC("blue", "media")
	pvc.Labels = nil
	kopia := &fakeKopia{}
	mut := &PVCMutator{Decoder: newDecoder(t), Kopia: kopia}

	resp := mut.Handle(context.Background(), pvcRequest(t, pvc))
	if !resp.Allowed {
		t.Fatalf("expected allowed=true for unlabeled PVC")
	}
	if len(kopia.calls) != 0 {
		t.Errorf("expected zero kopia calls for unlabeled PVC, got %d", len(kopia.calls))
	}
}

func TestPVCMutate_SystemNamespace_AllowsWithoutCheck(t *testing.T) {
	pvc := backupPVC("blue", "kube-system")
	kopia := &fakeKopia{}
	mut := &PVCMutator{
		Decoder:          newDecoder(t),
		Kopia:            kopia,
		SystemNamespaces: map[string]struct{}{"kube-system": {}},
	}

	resp := mut.Handle(context.Background(), pvcRequest(t, pvc))
	if !resp.Allowed {
		t.Fatalf("expected allowed=true for system namespace")
	}
	if len(kopia.calls) != 0 {
		t.Errorf("expected zero kopia calls for system namespace, got %d", len(kopia.calls))
	}
}

func TestPVCMutate_SkipRestore_AllowsWithoutCheck(t *testing.T) {
	pvc := backupPVC("blue", "media")
	pvc.Annotations = map[string]string{"volsync.backup/skip-restore": "true"}
	kopia := &fakeKopia{}
	mut := &PVCMutator{Decoder: newDecoder(t), Kopia: kopia}

	resp := mut.Handle(context.Background(), pvcRequest(t, pvc))
	if !resp.Allowed {
		t.Fatalf("expected allowed=true with skip-restore=true")
	}
	if len(kopia.calls) != 0 {
		t.Errorf("expected zero kopia calls with skip-restore=true, got %d", len(kopia.calls))
	}
}

func TestPVCMutate_NonCreate_AllowsImmediately(t *testing.T) {
	pvc := backupPVC("blue", "media")
	raw, _ := json.Marshal(pvc)
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Update,
		Object:    runtime.RawExtension{Raw: raw},
	}}
	kopia := &fakeKopia{}
	mut := &PVCMutator{Decoder: newDecoder(t), Kopia: kopia}

	resp := mut.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected allowed=true on non-CREATE")
	}
	if len(kopia.calls) != 0 {
		t.Errorf("expected zero kopia calls on non-CREATE, got %d", len(kopia.calls))
	}
}
