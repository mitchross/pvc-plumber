package webhook

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// withRestoreDataSourceRef returns the PVC with the dataSourceRef shape that
// the mutator would have injected on a restore decision.
func withRestoreDataSourceRef(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
	pvc.Spec.DataSourceRef = &corev1.TypedObjectReference{
		APIGroup: ptr.To("volsync.backube"),
		Kind:     "ReplicationDestination",
		Name:     pvc.Name + "-backup",
	}
	return pvc
}

func TestPVCValidate_Unknown_Denies(t *testing.T) {
	kopia := &fakeKopia{result: backend.CheckResult{
		Decision:      backend.DecisionUnknown,
		Authoritative: true, // explicitly authoritative so we test the decision branch
	}}
	val := &PVCValidator{Decoder: newDecoder(t), Kopia: kopia}

	resp := val.Handle(context.Background(), pvcRequest(t, backupPVC("blue", "media")))
	if resp.Allowed {
		t.Fatalf("expected denied on decision=unknown, got allowed")
	}
	if !strings.Contains(resp.Result.Message, "could not make an authoritative") {
		t.Errorf("unexpected deny message: %q", resp.Result.Message)
	}
}

func TestPVCValidate_Error_Denies(t *testing.T) {
	kopia := &fakeKopia{result: backend.CheckResult{
		Decision:      backend.DecisionRestore, // even with restore decision, presence of Error denies
		Authoritative: true,
		Error:         "transient kopia failure",
	}}
	val := &PVCValidator{Decoder: newDecoder(t), Kopia: kopia}

	resp := val.Handle(context.Background(), pvcRequest(t, backupPVC("blue", "media")))
	if resp.Allowed {
		t.Fatalf("expected denied when result.Error set, got allowed")
	}
}

func TestPVCValidate_NotAuthoritative_Denies(t *testing.T) {
	kopia := &fakeKopia{result: backend.CheckResult{
		Decision:      backend.DecisionFresh, // would normally allow, but non-authoritative
		Authoritative: false,
	}}
	val := &PVCValidator{Decoder: newDecoder(t), Kopia: kopia}

	resp := val.Handle(context.Background(), pvcRequest(t, backupPVC("blue", "media")))
	if resp.Allowed {
		t.Fatalf("expected denied on non-authoritative result, got allowed")
	}
}

func TestPVCValidate_RestoreNoDataSourceRef_Denies(t *testing.T) {
	// Belt-and-suspenders rule 3: mutator's call returned non-authoritative
	// (so no dataSourceRef was injected) but validator's call now succeeds
	// with restore. Without this gate, an empty PV would be provisioned over
	// restorable backup data.
	kopia := &fakeKopia{result: backend.CheckResult{
		Exists:        true,
		Decision:      backend.DecisionRestore,
		Authoritative: true,
	}}
	val := &PVCValidator{Decoder: newDecoder(t), Kopia: kopia}

	resp := val.Handle(context.Background(), pvcRequest(t, backupPVC("blue", "media")))
	if resp.Allowed {
		t.Fatalf("expected denied when restore decision but no dataSourceRef")
	}
	if !strings.Contains(resp.Result.Message, "missing the expected dataSourceRef") {
		t.Errorf("unexpected deny message: %q", resp.Result.Message)
	}
}

func TestPVCValidate_RestoreWithCorrectDataSourceRef_Allows(t *testing.T) {
	// Happy path on the restore branch: mutator already injected the right
	// ref and the validator's independent kopia call agrees.
	kopia := &fakeKopia{result: backend.CheckResult{
		Exists:        true,
		Decision:      backend.DecisionRestore,
		Authoritative: true,
	}}
	val := &PVCValidator{Decoder: newDecoder(t), Kopia: kopia}

	pvc := withRestoreDataSourceRef(backupPVC("blue", "media"))
	resp := val.Handle(context.Background(), pvcRequest(t, pvc))
	if !resp.Allowed {
		t.Fatalf("expected allowed when restore + correct dataSourceRef, got denied: %s", resp.Result.Message)
	}
}

func TestPVCValidate_RestoreWithWrongDataSourceRef_Denies(t *testing.T) {
	// Operator (or mutator bug) put the wrong kind/apiGroup/name on the ref.
	// Rule 3's `any` block flips all three NotEquals into denials.
	cases := []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim)
	}{
		{
			name: "wrong kind",
			mutate: func(p *corev1.PersistentVolumeClaim) {
				p.Spec.DataSourceRef.Kind = "VolumeSnapshot"
			},
		},
		{
			name: "wrong apiGroup",
			mutate: func(p *corev1.PersistentVolumeClaim) {
				p.Spec.DataSourceRef.APIGroup = ptr.To("snapshot.storage.k8s.io")
			},
		},
		{
			name: "nil apiGroup",
			mutate: func(p *corev1.PersistentVolumeClaim) {
				p.Spec.DataSourceRef.APIGroup = nil
			},
		},
		{
			name: "wrong name",
			mutate: func(p *corev1.PersistentVolumeClaim) {
				p.Spec.DataSourceRef.Name = "some-other-name"
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kopia := &fakeKopia{result: backend.CheckResult{
				Exists:        true,
				Decision:      backend.DecisionRestore,
				Authoritative: true,
			}}
			val := &PVCValidator{Decoder: newDecoder(t), Kopia: kopia}
			pvc := withRestoreDataSourceRef(backupPVC("blue", "media"))
			tc.mutate(pvc)

			resp := val.Handle(context.Background(), pvcRequest(t, pvc))
			if resp.Allowed {
				t.Fatalf("expected denied for %s, got allowed", tc.name)
			}
		})
	}
}

func TestPVCValidate_FreshDecision_Allows(t *testing.T) {
	kopia := &fakeKopia{result: backend.CheckResult{
		Decision:      backend.DecisionFresh,
		Authoritative: true,
	}}
	val := &PVCValidator{Decoder: newDecoder(t), Kopia: kopia}

	resp := val.Handle(context.Background(), pvcRequest(t, backupPVC("blue", "media")))
	if !resp.Allowed {
		t.Fatalf("expected allowed for fresh authoritative decision, got denied: %s", resp.Result.Message)
	}
}

func TestPVCValidate_SkipRestoreNoReason_Denies(t *testing.T) {
	pvc := backupPVC("blue", "media")
	pvc.Annotations = map[string]string{"volsync.backup/skip-restore": "true"}
	kopia := &fakeKopia{}
	val := &PVCValidator{Decoder: newDecoder(t), Kopia: kopia}

	resp := val.Handle(context.Background(), pvcRequest(t, pvc))
	if resp.Allowed {
		t.Fatalf("expected denied when skip-restore=true and no reason, got allowed")
	}
	if !strings.Contains(resp.Result.Message, "skip-restore-reason") {
		t.Errorf("unexpected deny message: %q", resp.Result.Message)
	}
	// Skip-restore short-circuits before kopia is consulted.
	if len(kopia.calls) != 0 {
		t.Errorf("expected zero kopia calls when skip-restore=true, got %d", len(kopia.calls))
	}
}

func TestPVCValidate_SkipRestoreWithReason_Allows(t *testing.T) {
	pvc := backupPVC("blue", "media")
	pvc.Annotations = map[string]string{
		"volsync.backup/skip-restore":        "true",
		"volsync.backup/skip-restore-reason": "drill 2026-05-01",
	}
	// Even if kopia would have denied, skip-restore=true with a reason
	// short-circuits past the gate. This is the documented escape hatch.
	kopia := &fakeKopia{result: backend.CheckResult{
		Decision:      backend.DecisionUnknown,
		Authoritative: false,
		Error:         "this would normally deny",
	}}
	val := &PVCValidator{Decoder: newDecoder(t), Kopia: kopia}

	resp := val.Handle(context.Background(), pvcRequest(t, pvc))
	if !resp.Allowed {
		t.Fatalf("expected allowed when skip-restore=true with reason, got denied: %s", resp.Result.Message)
	}
	if len(kopia.calls) != 0 {
		t.Errorf("expected zero kopia calls on skip-restore path, got %d", len(kopia.calls))
	}
}

func TestPVCValidate_SystemNamespace_Allows(t *testing.T) {
	pvc := backupPVC("blue", "kube-system")
	kopia := &fakeKopia{result: backend.CheckResult{
		// Even an unknown decision wouldn't deny — system namespace skips first.
		Decision:      backend.DecisionUnknown,
		Authoritative: false,
	}}
	val := &PVCValidator{
		Decoder:          newDecoder(t),
		Kopia:            kopia,
		SystemNamespaces: map[string]struct{}{"kube-system": {}, "volsync-system": {}, "kyverno": {}},
	}

	resp := val.Handle(context.Background(), pvcRequest(t, pvc))
	if !resp.Allowed {
		t.Fatalf("expected allowed for system namespace, got denied: %s", resp.Result.Message)
	}
	if len(kopia.calls) != 0 {
		t.Errorf("expected zero kopia calls for system namespace, got %d", len(kopia.calls))
	}
}

func TestPVCValidate_NoBackupLabel_Allows(t *testing.T) {
	pvc := backupPVC("blue", "media")
	pvc.Labels = nil
	kopia := &fakeKopia{result: backend.CheckResult{
		Decision: backend.DecisionUnknown, // would otherwise deny
	}}
	val := &PVCValidator{Decoder: newDecoder(t), Kopia: kopia}

	resp := val.Handle(context.Background(), pvcRequest(t, pvc))
	if !resp.Allowed {
		t.Fatalf("expected allowed for unlabeled PVC, got denied: %s", resp.Result.Message)
	}
	if len(kopia.calls) != 0 {
		t.Errorf("expected zero kopia calls for unlabeled PVC, got %d", len(kopia.calls))
	}
}
