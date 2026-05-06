package webhook

import (
	"context"
	"encoding/json"
	"net/http"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// VolSync mover Jobs are stamped with this label by the VolSync controller.
// We use it as a defense-in-depth filter — the webhook's objectSelector also
// scopes by it, but a misconfigured webhook config could send us anything.
const (
	volsyncCreatedByLabel = "app.kubernetes.io/created-by"
	volsyncCreatedByValue = "volsync"

	repositoryVolumeName = "repository"
	repositoryMountPath  = "/repository"
)

// JobMutator is the mutating admission handler for batch/v1 Job CREATE
// requests. It replaces the Kyverno ClusterPolicy `volsync-nfs-inject` and
// injects the shared NFS Kopia repository (volume + per-container mount)
// into VolSync mover pods. Without this mount, Kopia inside the mover can't
// reach `/repository` and every backup/restore fails immediately.
//
// Failure mode: fail-IGNORE. The webhook configuration uses
// failurePolicy=Ignore so a webhook outage does not block VolSync entirely
// (preferring degraded backups to a fully wedged cluster). The handler
// itself never denies — it either patches or no-ops.
//
// Idempotency: safe to call multiple times. If a `repository` volume already
// exists (operator pre-populated it, or webhook re-fired during a retry),
// the handler returns a no-op so we don't duplicate the volume entry.
type JobMutator struct {
	Decoder   admission.Decoder
	NFSServer string
	NFSPath   string
}

// Handle implements admission.Handler.
func (h *JobMutator) Handle(_ context.Context, req admission.Request) admission.Response {
	job := &batchv1.Job{}
	if err := h.Decoder.Decode(req, job); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Defense in depth: the webhook objectSelector should already restrict
	// us to volsync-created Jobs, but admit any non-volsync Job unchanged
	// rather than mutate something we don't own.
	if job.Labels[volsyncCreatedByLabel] != volsyncCreatedByValue {
		return admission.Allowed("")
	}

	// Idempotent no-op: if the operator (or a previous admission pass)
	// already wired the repository volume, leave the spec alone. This
	// prevents duplicate volume entries and matches the strategic-merge
	// behaviour of the Kyverno policy this replaces.
	for _, vol := range job.Spec.Template.Spec.Volumes {
		if vol.Name == repositoryVolumeName {
			return admission.Allowed("")
		}
	}

	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: repositoryVolumeName,
		VolumeSource: corev1.VolumeSource{
			NFS: &corev1.NFSVolumeSource{
				Server: h.NFSServer,
				Path:   h.NFSPath,
			},
		},
	})

	// Mount into every container (initContainers excluded — VolSync mover
	// pods don't use init for Kopia, and adding here would surprise users
	// who do). The Kyverno foreach in volsync-nfs-inject also targets
	// `containers` only.
	mount := corev1.VolumeMount{Name: repositoryVolumeName, MountPath: repositoryMountPath}
	for i := range job.Spec.Template.Spec.Containers {
		job.Spec.Template.Spec.Containers[i].VolumeMounts = append(
			job.Spec.Template.Spec.Containers[i].VolumeMounts, mount)
	}

	marshaled, err := json.Marshal(job)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}
