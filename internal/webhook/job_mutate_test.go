package webhook

import (
	"context"
	"encoding/json"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	testNFSServer = "192.168.10.133"
	testNFSPath   = "/mnt/BigTank/k8s/volsync-kopia-nfs"
)

func volsyncJob(name string, containers []corev1.Container) *batchv1.Job {
	return &batchv1.Job{
		TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "media",
			Labels:    map[string]string{"app.kubernetes.io/created-by": "volsync"},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: containers,
				},
			},
		},
	}
}

func jobRequest(t *testing.T, job *batchv1.Job) admission.Request {
	t.Helper()
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	return admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: raw},
		},
	}
}

func TestJobMutate_VolsyncJob_InjectsVolumeAndMounts(t *testing.T) {
	job := volsyncJob("backup-1", []corev1.Container{
		{Name: "kopia", Image: "kopia:latest"},
	})
	mut := &JobMutator{Decoder: newDecoder(t), NFSServer: testNFSServer, NFSPath: testNFSPath}

	req := jobRequest(t, job)
	resp := mut.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected allowed=true with patches, got denied: %v", resp.Result)
	}
	if len(resp.Patches) == 0 {
		t.Fatalf("expected patches, got none")
	}

	patched := applyPatch(t, req.Object.Raw, resp.Patches)
	got := &batchv1.Job{}
	if err := json.Unmarshal(patched, got); err != nil {
		t.Fatalf("unmarshal patched: %v", err)
	}

	// Volume injected once, with NFS source.
	var found bool
	for _, v := range got.Spec.Template.Spec.Volumes {
		if v.Name != "repository" {
			continue
		}
		found = true
		if v.NFS == nil {
			t.Fatalf("expected NFS volume source on `repository` volume")
		}
		if v.NFS.Server != testNFSServer {
			t.Errorf("NFS.Server: want %q, got %q", testNFSServer, v.NFS.Server)
		}
		if v.NFS.Path != testNFSPath {
			t.Errorf("NFS.Path: want %q, got %q", testNFSPath, v.NFS.Path)
		}
	}
	if !found {
		t.Fatalf("expected `repository` volume in patched spec")
	}

	// Mount appended to every container.
	if len(got.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(got.Spec.Template.Spec.Containers))
	}
	mounts := got.Spec.Template.Spec.Containers[0].VolumeMounts
	if len(mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(mounts))
	}
	if mounts[0].Name != "repository" || mounts[0].MountPath != "/repository" {
		t.Errorf("unexpected mount: %+v", mounts[0])
	}
}

func TestJobMutate_NotVolsync_NoOp(t *testing.T) {
	job := volsyncJob("not-volsync", []corev1.Container{{Name: "main"}})
	job.Labels = map[string]string{"app.kubernetes.io/created-by": "something-else"}
	mut := &JobMutator{Decoder: newDecoder(t), NFSServer: testNFSServer, NFSPath: testNFSPath}

	resp := mut.Handle(context.Background(), jobRequest(t, job))
	if !resp.Allowed {
		t.Fatalf("expected allowed=true on non-volsync Job")
	}
	if len(resp.Patches) != 0 {
		t.Errorf("expected no patches on non-volsync Job, got %d", len(resp.Patches))
	}
}

func TestJobMutate_RepositoryVolumeAlreadyExists_NoOp(t *testing.T) {
	// Idempotency: a re-fired admission (or operator-curated Job) that
	// already has the volume must NOT have it duplicated.
	job := volsyncJob("backup-1", []corev1.Container{{Name: "kopia"}})
	job.Spec.Template.Spec.Volumes = []corev1.Volume{
		{
			Name: "repository",
			VolumeSource: corev1.VolumeSource{
				NFS: &corev1.NFSVolumeSource{Server: testNFSServer, Path: testNFSPath},
			},
		},
	}
	mut := &JobMutator{Decoder: newDecoder(t), NFSServer: testNFSServer, NFSPath: testNFSPath}

	resp := mut.Handle(context.Background(), jobRequest(t, job))
	if !resp.Allowed {
		t.Fatalf("expected allowed=true when repository volume preset")
	}
	if len(resp.Patches) != 0 {
		t.Errorf("expected no patches on idempotent re-run, got %d: %+v", len(resp.Patches), resp.Patches)
	}
}

func TestJobMutate_MultipleContainers_AllGetMount(t *testing.T) {
	// Real volsync mover pods can have sidecars (metrics, log shipping, etc.)
	// and Kopia operates on /repository regardless of which container reads
	// it — every container needs the mount.
	job := volsyncJob("backup-1", []corev1.Container{
		{Name: "kopia"},
		{Name: "sidecar-metrics"},
		{Name: "sidecar-logs", VolumeMounts: []corev1.VolumeMount{{Name: "logs", MountPath: "/logs"}}},
	})
	mut := &JobMutator{Decoder: newDecoder(t), NFSServer: testNFSServer, NFSPath: testNFSPath}

	req := jobRequest(t, job)
	resp := mut.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected allowed=true, got denied: %v", resp.Result)
	}

	patched := applyPatch(t, req.Object.Raw, resp.Patches)
	got := &batchv1.Job{}
	if err := json.Unmarshal(patched, got); err != nil {
		t.Fatalf("unmarshal patched: %v", err)
	}

	for _, c := range got.Spec.Template.Spec.Containers {
		var hasRepo bool
		for _, m := range c.VolumeMounts {
			if m.Name == "repository" && m.MountPath == "/repository" {
				hasRepo = true
				break
			}
		}
		if !hasRepo {
			t.Errorf("container %q missing /repository mount; mounts=%+v", c.Name, c.VolumeMounts)
		}
	}

	// And the pre-existing mount on sidecar-logs should still be there.
	for _, c := range got.Spec.Template.Spec.Containers {
		if c.Name != "sidecar-logs" {
			continue
		}
		var hasLogs bool
		for _, m := range c.VolumeMounts {
			if m.Name == "logs" {
				hasLogs = true
			}
		}
		if !hasLogs {
			t.Errorf("expected pre-existing `logs` mount preserved on sidecar-logs, got %+v", c.VolumeMounts)
		}
	}
}

func TestJobMutate_NoLabel_NoOp(t *testing.T) {
	job := volsyncJob("untagged", []corev1.Container{{Name: "main"}})
	job.Labels = nil
	mut := &JobMutator{Decoder: newDecoder(t), NFSServer: testNFSServer, NFSPath: testNFSPath}

	resp := mut.Handle(context.Background(), jobRequest(t, job))
	if !resp.Allowed {
		t.Fatalf("expected allowed=true on label-less Job")
	}
	if len(resp.Patches) != 0 {
		t.Errorf("expected no patches on label-less Job, got %d", len(resp.Patches))
	}
}
