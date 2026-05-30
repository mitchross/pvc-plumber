// Package labels owns the v4 label and annotation key constants that the
// pvc-plumber operator reads from PersistentVolumeClaim and Namespace objects.
//
// Phase 2 of docs/pvc-plumber-v4-prd.md in the talos-argocd-proxmox repo.
// The v4 contract is namespaced ("pvc-plumber.io/*"). Legacy keys
// ("backup: hourly|daily") are recognized for inventory and migration but
// MUST NOT be used by future admission webhook objectSelectors — only the
// new namespaced LabelEnabled is a valid opt-in for admission.
//
// This package is pure (no Kubernetes client, no I/O) and safe to import
// from anywhere — controllers, webhooks, CLI tools, tests.
package labels

// New v4 namespaced labels (used by reconciler AND webhook objectSelector).
const (
	// LabelEnabled is the **only** opt-in signal honored by future admission
	// webhooks. PVCs without this label set to "true" are ignored by the
	// webhook and never denied. Reconciler may still read legacy backup:
	// labels for inventory, but admission scope is the namespaced label only.
	LabelEnabled = "pvc-plumber.io/enabled"

	// LabelTier declares the backup cadence: hourly | daily | weekly | manual | disabled.
	// "disabled" tears down RS without removing RD (preserves restore-on-recreate).
	LabelTier = "pvc-plumber.io/tier"

	// LabelManageVolSync gates whether pvc-plumber may create / update /
	// delete VolSync ReplicationSource and ReplicationDestination resources
	// for this PVC. REQUIRED IN ADDITION to LabelEnabled for any write to
	// happen. Missing or "false" means: the operator reports on the PVC
	// (subject to LabelEnabled or the legacy backup label) but never
	// writes child resources for it.
	//
	// Phase 6 of docs/pvc-plumber-v4-prd.md. The two-label model is the
	// migration safety fuse: PRs that flip apps from `backup: hourly` to
	// `pvc-plumber.io/enabled: "true"` can ship in audit mode without any
	// chance of triggering ownership transfer. Operator-managed
	// VolSync resources require a SECOND, explicit PR that adds this
	// label and simultaneously removes inline RS/RD from Git.
	//
	// Strict parsing: "true" / "false" (case-insensitive, whitespace
	// tolerated) are accepted; any other value is recorded in
	// Spec.Errors and the field falls back to false. The strictness
	// difference vs. LabelEnabled is deliberate — this label is a
	// write fuse and silent acceptance of a typo would hide an
	// operator misconfiguration.
	LabelManageVolSync = "pvc-plumber.io/manage-volsync"
)

// New v4 namespaced annotations (per-PVC overrides + free-form metadata).
const (
	// AnnotationMode overrides the global / namespace mode for this PVC:
	// audit | permissive | enforce | strict.
	// (never and force live under AnnotationRestoreMode below — they only
	// affect the restore decision, not the admission discipline.)
	AnnotationMode = "pvc-plumber.io/mode"

	// AnnotationRestoreMode per-PVC restore policy:
	// audit | permissive | enforce | strict | never | force.
	// "never" suppresses dataSourceRef injection entirely.
	// "force" requires a known backup to exist; denies if missing or unknown.
	AnnotationRestoreMode = "pvc-plumber.io/restore-mode"

	// AnnotationUID, AnnotationGID, AnnotationFSGroup set the mover security
	// context. Must be integer strings parseable as int64; 0..2^31-1.
	AnnotationUID     = "pvc-plumber.io/uid"
	AnnotationGID     = "pvc-plumber.io/gid"
	AnnotationFSGroup = "pvc-plumber.io/fsGroup"

	// AnnotationBackupIdentity is used in two places:
	//
	//   (1) On the source PVC: overrides the default <namespace>/<pvc>
	//       identity. Use this when a PVC needs to retain a stable kopia
	//       identity across namespace renames or app migrations.
	//   (2) On operator-generated RS/RD children: the builder stamps the
	//       resolved backup identity here for human/audit consumption.
	//       Stored as an annotation rather than a label because the
	//       default value `<namespace>/<pvc>` contains '/', which is
	//       invalid in K8s label values (regex
	//       `(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?`).
	//       See the 2026-05-28 nginx-example/storage canary incident
	//       in the talos-argocd-proxmox repo at
	//       docs/pvc-plumber-v4-nginx-canary-incident.md.
	//
	// Discovery and ownership classification do NOT match on this annotation.
	// They use LabelManagedByKey / LabelSourceNamespace / LabelSourcePVC.
	AnnotationBackupIdentity = "pvc-plumber.io/backup-identity"

	// AnnotationSkipRestore="true" opts this PVC out of restore-on-recreate
	// for an intentionally-fresh state (e.g., DR drill, test PVC). Strict
	// mode REQUIRES AnnotationSkipRestoreReason to be set and non-empty.
	AnnotationSkipRestore       = "pvc-plumber.io/skip-restore"
	AnnotationSkipRestoreReason = "pvc-plumber.io/skip-restore-reason"

	// AnnotationCacheCapacity overrides the default kopia cacheCapacity
	// (e.g., "4Gi" for large PVCs). Empty string falls back to default.
	AnnotationCacheCapacity = "pvc-plumber.io/cache-capacity"

	// AnnotationStorageClass overrides the storageClassName used by VolSync
	// for the snapshot/restore intermediate PVCs. Empty falls back to the
	// source PVC's storage class.
	AnnotationStorageClass = "pvc-plumber.io/storage-class"

	// AnnotationSnapshotClass overrides the VolumeSnapshotClass kopia mover
	// uses. Empty falls back to operator default.
	AnnotationSnapshotClass = "pvc-plumber.io/snapshot-class"

	// AnnotationMinBackupAge gates RS creation: do not start backing up
	// until the PVC has been Bound for at least this duration. Default is
	// operator config (recommended 2h). Format: time.ParseDuration.
	AnnotationMinBackupAge = "pvc-plumber.io/min-backup-age"
)

// Legacy keys retained for inventory + back-compat reads. These MUST NOT be
// used by admission webhook objectSelectors in v4. See package doc.
const (
	// LegacyLabelBackup is the v1-v3 tier label. Values: "hourly", "daily".
	// In v4 the controller may report on PVCs carrying this label but must
	// NOT treat it as an opt-in signal for admission.
	LegacyLabelBackup = "backup"

	// LegacyLabelBackupExempt was the v1-v3 exemption label. Retained for
	// reporting and the FQ reason-annotation contract enforced by CI.
	LegacyLabelBackupExempt = "backup-exempt"

	// LegacyAnnotationBackupExemptReasonFQ is the **only** valid backup-exempt
	// reason annotation — the bare key was silently ignored in v3 and is a
	// known DR landmine. CI job backup-exempt-contract enforces this in the
	// talos repo.
	LegacyAnnotationBackupExemptReasonFQ = "storage.vanillax.dev/backup-exempt-reason"
)

// Conventions used on operator-generated resources (RS / RD). Future phases
// (Phase 5+) write these labels onto generated objects so the reconciler can
// distinguish operator-owned from Argo-owned resources.
const (
	// LabelManagedByValue is the value of app.kubernetes.io/managed-by that
	// marks a resource as operator-owned. The key itself is the
	// well-known Kubernetes recommendation.
	LabelManagedByKey   = "app.kubernetes.io/managed-by"
	LabelManagedByValue = "pvc-plumber"

	// Source PVC pointer labels on generated children, used by the reconciler
	// to find resources for a given PVC.
	//
	// NOTE: there is intentionally NO `LabelBackupIdentity` constant. The
	// compound backup identity `<namespace>/<pvc>` contains '/', which is
	// invalid in K8s label values. The builder stamps this identity as
	// AnnotationBackupIdentity on RS/RD children instead. Discovery and
	// ownership matching use the per-component labels below, which are
	// always individually valid. See the 2026-05-28 nginx-example/storage
	// canary incident (talos-argocd-proxmox
	// docs/pvc-plumber-v4-nginx-canary-incident.md).
	LabelSourceNamespace = "pvc-plumber.io/source-namespace"
	LabelSourcePVC       = "pvc-plumber.io/source-pvc"
	LabelTierOnChild     = "pvc-plumber.io/tier"
)

// NamespacePrivilegedMoversLabel is the label that the operator and the
// existing ClusterExternalSecret in the talos repo use to decide whether to
// fan out the shared kopia repo Secret to a namespace. The operator does
// NOT manage this label; it reads it.
const NamespacePrivilegedMoversLabel = "volsync.backube/privileged-movers"

// NamespaceManagedLabel opts a namespace in to pvc-plumber operator
// management. It is the namespace-level WRITE GATE (v4.0.1): even when a
// PVC carries BOTH fuse labels (LabelEnabled + LabelManageVolSync), the
// operator will NOT create/update/delete VolSync RS/RD in a namespace that
// lacks this label set to exactly "true".
//
// This is the software replacement for relying on per-namespace
// volsync-writer RoleBindings as the only namespace boundary. With this
// gate enforced in the planner, a single DRY cluster-wide RS/RD write
// ClusterRoleBinding becomes safe: RBAC is the mechanism, this label is
// the boundary. The operator never sets this label; it reads it.
const NamespaceManagedLabel = "pvc-plumber.io/managed-namespace"

// NamespaceManaged reports whether the namespace is opted in to pvc-plumber
// operator management. Strict and FAIL-CLOSED: returns true only for the
// exact value "true" (a typo'd, empty, or mixed-case value fails closed →
// the operator will not write). Pure; nil-safe (a nil map yields false).
func NamespaceManaged(nsLabels map[string]string) bool {
	return nsLabels[NamespaceManagedLabel] == "true"
}
