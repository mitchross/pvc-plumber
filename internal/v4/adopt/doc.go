// Package adopt is the read-only adoption planner for pvc-plumber v4.
//
// It inspects a PersistentVolumeClaim and the VolSync ReplicationSource /
// ReplicationDestination that currently exist in the cluster for that PVC,
// then produces an adoption Plan: the verdict, blockers, warnings, and the
// exact labels + annotations a caller would need to write to put the PVC
// under v4 operator management.
//
// Read-only by construction: the package's only entrypoint accepts a
// sigs.k8s.io/controller-runtime client.Reader, not a full client.Client.
// The interface has no Create / Update / Patch / Delete methods, so the
// package cannot mutate cluster state regardless of intent. The fake-client
// write recorder in the tests is belt-and-braces against a future signature
// broadening.
//
// Why this exists: the universal spec.dataSourceRef drift documented in
// docs/pvc-plumber-v4-adopt-cli-spec.md (talos-argocd-proxmox repo) means
// labels-in-Git can't reliably reach a Bound PVC. A live label write under
// a dedicated field manager is safe (selfHeal does not strip foreign-owned
// labels), but it must be preceded by validation against the current
// VolSync shape so the operator doesn't take over a malformed pair. This
// package owns that validation.
//
// Reuse:
//   - internal/v4/labels.Parse parses PVC metadata into the canonical Spec
//     used by the rest of v4.
//   - internal/v4/builder.BuildRS / BuildRD render the expected RS/RD
//     byte-identically to what the reconciler would create.
//   - internal/v4/naming.DefaultRepoSecretName is the canonical kopia repo
//     Secret name.
//   - internal/v4/planner.OwnerClassification values are reused as-is so
//     adopt's verdicts agree with /audit's verdicts.
//
// What this package does NOT do:
//   - It does not create / update / delete RS or RD.
//   - It does not mutate PVC spec.
//   - It does not write any labels or annotations. (That is Patch 7.2's CLI
//     concern; this package only computes what would be written.)
//   - It does not read or modify Argo Application resources.
//   - It does not touch Secrets, ExternalSecrets, or backup data.
package adopt
