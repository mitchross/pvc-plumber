// Package builder produces the desired-state VolSync ReplicationSource
// and ReplicationDestination objects for an opted-in PVC. It is pure:
// no Kubernetes client, no I/O, no cluster reads. Callers (the v4
// planner and executor) decide whether to apply the output; the
// builder only describes what SHOULD exist for a given (PVC, labels).
//
// Phase 6 / Patch 6.3 of docs/pvc-plumber-v4-prd.md.
//
// Shape contract: the produced RS/RD must be byte-equivalent to the
// inline pattern already deployed in the talos-argocd-proxmox repo
// (see my-apps/ai/open-webui/pvc.yaml and the runbook at
// my-apps/CLAUDE.md). The cutover (Patch 6.4+) deletes the inline
// version from Git and lets the operator render the same shape with
// `app.kubernetes.io/managed-by: pvc-plumber` instead of `argocd`.
//
// Names: RS = `<pvc>`, RD = `<pvc>-dst`. No `<pvc>-backup` suffix and
// no per-PVC `volsync-<pvc>` ExternalSecret — the shared repo Secret
// `volsync-kopia-repository` is fanned out to namespaces by the
// ClusterExternalSecret in the talos repo; the builder only
// references it by name.
package builder

import (
	"strconv"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// Inputs is the total set of values needed to build RS + RD for a
// PVC. Keeping all inputs in a single value type lets tests build
// fixtures without reaching into Kubernetes types, and lets the
// planner pass a stable snapshot of the PVC state at one moment in
// time (the PVC could change between planner and executor; we want
// the same shape both times).
type Inputs struct {
	// Identity.
	Namespace string
	PVCName   string

	// PVC spec carried over to RS/RD.
	PVCCapacity     string   // e.g. "10Gi" — from .spec.resources.requests.storage
	PVCAccessModes  []string // e.g. ["ReadWriteOnce"] — from .spec.accessModes
	PVCStorageClass string   // e.g. "longhorn" — from .spec.storageClassName; fallback for RD if labels.Spec.StorageClass is empty

	// Operator-resolved labels.Spec (tier, identity override, security
	// context, class overrides). The builder respects every annotation
	// override that the parser exposes — the operator's "single source
	// of truth" for per-PVC backup configuration is the parsed Spec.
	Spec labels.Spec

	// Naming + shared-resource references.
	NamingStrategy    naming.Strategy
	DefaultRepoSecret string // typically naming.DefaultRepoSecretName

	// Cluster-wide defaults the operator config carries. Builder uses
	// these only when labels.Spec doesn't override the per-PVC value.
	DefaultSnapshotClass string // e.g. "longhorn-snapclass"
	DefaultCacheCapacity string // e.g. "2Gi"
	DefaultStorageClass  string // fallback storage class for RD intermediate PVC
	DefaultUID           int64  // 568 in the reference deployment
	DefaultGID           int64  // 568
	DefaultFSGroup       int64  // 568
}

// VolSync API group/version. Kept as package vars so tests in this
// package can produce assertion fixtures with the same GVKs the
// builder emits.
var (
	rsGVK = schema.GroupVersionKind{Group: "volsync.backube", Version: "v1alpha1", Kind: "ReplicationSource"}
	rdGVK = schema.GroupVersionKind{Group: "volsync.backube", Version: "v1alpha1", Kind: "ReplicationDestination"}
)

// Defaults shared between RS and RD. These match the live inline
// RS/RD pattern in the talos repo today. Centralized so a future
// cluster-wide bump (e.g. compression algorithm) is one edit.
const (
	defaultCompression = "zstd-fastest"
	defaultParallelism = int64(2)
	defaultCopyMethod  = "Snapshot"
	defaultRetainH     = int64(24)
	defaultRetainD     = int64(7)
	defaultRetainW     = int64(4)
	defaultRetainM     = int64(2)

	// VolSync retain block keys. These are VolSync-API field names —
	// they happen to mirror the labels.Tier string names, but the two
	// are conceptually distinct sources of truth.
	retainKeyHourly  = "hourly"
	retainKeyDaily   = "daily"
	retainKeyWeekly  = "weekly"
	retainKeyMonthly = "monthly"

	// accessModeRWO is the safe default when a synthetic / test
	// fixture omits PVCAccessModes. Real PVCs always supply their
	// own list. Constant rather than literal so a future cluster
	// that uses RWX for VolSync intermediate PVCs only edits this
	// place.
	accessModeRWO = "ReadWriteOnce"
)

// BuildRS constructs the desired ReplicationSource as an unstructured
// object. The returned object is independent of any cluster state —
// callers may freely mutate or discard it. The builder never reads
// Secrets and never embeds credentials; the only Secret it references
// is by name (`spec.kopia.repository`).
func BuildRS(in Inputs) *unstructured.Unstructured {
	rs := &unstructured.Unstructured{}
	rs.SetGroupVersionKind(rsGVK)
	rs.SetNamespace(in.Namespace)
	rs.SetName(in.PVCName) // RS name is the PVC name verbatim (bare-dst convention)
	rs.SetLabels(commonLabels(in))
	rs.SetAnnotations(commonAnnotations(in))

	identity := naming.IdentityFor(in.Namespace, in.PVCName, in.Spec.BackupIdentity)
	kopia := map[string]interface{}{
		"repository":              repoSecretName(in),
		"username":                identity.Username,
		"hostname":                identity.Hostname,
		"compression":             defaultCompression,
		"parallelism":             defaultParallelism,
		"copyMethod":              defaultCopyMethod,
		"storageClassName":        coalesce(in.Spec.StorageClass, in.PVCStorageClass, in.DefaultStorageClass),
		"volumeSnapshotClassName": coalesce(in.Spec.SnapshotClass, in.DefaultSnapshotClass),
		"cacheCapacity":           coalesce(in.Spec.CacheCapacity, in.DefaultCacheCapacity),
		"retain": map[string]interface{}{
			retainKeyHourly:  defaultRetainH,
			retainKeyDaily:   defaultRetainD,
			retainKeyWeekly:  defaultRetainW,
			retainKeyMonthly: defaultRetainM,
		},
		"moverSecurityContext": moverSecurityContext(in),
	}

	rs.Object["spec"] = map[string]interface{}{
		"sourcePVC": in.PVCName,
		"trigger": map[string]interface{}{
			"schedule": ScheduleFor(in.Namespace, in.PVCName, in.Spec.Tier),
		},
		"kopia": kopia,
	}
	return rs
}

// BuildRD constructs the desired ReplicationDestination as an
// unstructured object. RD is the restore counterpart of RS: a static
// manual trigger that fires only when the spec.trigger.manual string
// changes (the talos repo pins `restore-once`), plus the kopia
// sourceIdentity needed to locate the right snapshot lineage in the
// shared repo.
func BuildRD(in Inputs) *unstructured.Unstructured {
	rd := &unstructured.Unstructured{}
	rd.SetGroupVersionKind(rdGVK)
	rd.SetNamespace(in.Namespace)
	rd.SetName(in.PVCName + "-dst") // RD name is `<pvc>-dst` (bare-dst convention)
	rd.SetLabels(commonLabels(in))
	rd.SetAnnotations(commonAnnotations(in))

	identity := naming.IdentityFor(in.Namespace, in.PVCName, in.Spec.BackupIdentity)

	accessModes := in.PVCAccessModes
	if len(accessModes) == 0 {
		accessModes = []string{accessModeRWO} // safe Longhorn default
	}
	amInterface := make([]interface{}, 0, len(accessModes))
	for _, m := range accessModes {
		amInterface = append(amInterface, m)
	}

	kopia := map[string]interface{}{
		"repository":              repoSecretName(in),
		"username":                identity.Username,
		"hostname":                identity.Hostname,
		"sourceIdentity":          sourceIdentity(in),
		"copyMethod":              defaultCopyMethod,
		"storageClassName":        coalesce(in.Spec.StorageClass, in.PVCStorageClass, in.DefaultStorageClass),
		"volumeSnapshotClassName": coalesce(in.Spec.SnapshotClass, in.DefaultSnapshotClass),
		"cacheCapacity":           coalesce(in.Spec.CacheCapacity, in.DefaultCacheCapacity),
		"accessModes":             amInterface,
		"capacity":                in.PVCCapacity,
		"moverSecurityContext":    moverSecurityContext(in),
	}
	_ = identity // already used above

	rd.Object["spec"] = map[string]interface{}{
		"trigger": map[string]interface{}{
			"manual": "restore-once",
		},
		"kopia": kopia,
	}
	return rd
}

// commonLabels are stamped onto both RS and RD. These are the
// operator's identity stamp — the planner uses them to distinguish
// operator-owned resources from inline-Argo or unmanaged ones.
//
// Every value below must individually pass Kubernetes label-value
// validation (regex `(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?`).
// Compound identities containing '/' belong in annotations, NOT
// labels — see commonAnnotations and the 2026-05-28 nginx-example
// canary incident write-up in the talos-argocd-proxmox repo
// (docs/pvc-plumber-v4-nginx-canary-incident.md).
func commonLabels(in Inputs) map[string]string {
	out := map[string]string{
		labels.LabelManagedByKey:    labels.LabelManagedByValue,
		labels.LabelSourceNamespace: in.Namespace,
		labels.LabelSourcePVC:       in.PVCName,
		labels.LabelTierOnChild:     in.Spec.Tier.String(),
		"volsync.backup/pvc":        in.PVCName, // matches the inline RS/RD convention in the talos repo
	}
	return out
}

// commonAnnotations are stamped onto both RS and RD. Annotation values
// have no character-set restrictions (unlike label values), so the
// compound `<namespace>/<pvc>` backup identity lives here. Discovery
// and ownership classification do not depend on annotations — they
// match on labels — so this is purely human-readable metadata.
func commonAnnotations(in Inputs) map[string]string {
	return map[string]string{
		labels.AnnotationBackupIdentity: backupIdentityValue(in),
	}
}

// backupIdentityValue returns the value used for the
// pvc-plumber.io/backup-identity annotation on RS/RD children. If the
// PVC declared an override via annotation, that wins verbatim;
// otherwise the default <namespace>/<pvc> form. Mirrors
// naming.IdentityFor's override behavior so the child annotation is
// always a stable identity across namespace renames.
func backupIdentityValue(in Inputs) string {
	if in.Spec.BackupIdentity != "" {
		return in.Spec.BackupIdentity
	}
	return in.Namespace + "/" + in.PVCName
}

func sourceIdentity(in Inputs) map[string]interface{} {
	return map[string]interface{}{
		"sourceName":      in.PVCName,
		"sourceNamespace": in.Namespace,
		"sourcePVCName":   in.PVCName,
	}
}

func moverSecurityContext(in Inputs) map[string]interface{} {
	uid := in.DefaultUID
	gid := in.DefaultGID
	fsg := in.DefaultFSGroup
	if in.Spec.UID != nil {
		uid = *in.Spec.UID
	}
	if in.Spec.GID != nil {
		gid = *in.Spec.GID
	}
	if in.Spec.FSGroup != nil {
		fsg = *in.Spec.FSGroup
	}
	return map[string]interface{}{
		"runAsUser":  uid,
		"runAsGroup": gid,
		"fsGroup":    fsg,
	}
}

// repoSecretName returns the kopia repository Secret name to embed in
// `spec.kopia.repository`. Currently always the cluster-wide shared
// Secret; the seam exists so a future per-app override can be wired
// without churning the builder API.
func repoSecretName(in Inputs) string {
	if in.DefaultRepoSecret != "" {
		return in.DefaultRepoSecret
	}
	return naming.DefaultRepoSecretName
}

// coalesce returns the first non-empty string. Convenience used by
// every "labels.Spec override or fall back to cluster default" lookup.
func coalesce(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

// int64ToStr is convenience for tests that compare JSON-serialized
// values. Not used by the builder itself but exported for symmetry.
func int64ToStr(n int64) string { return strconv.FormatInt(n, 10) }
