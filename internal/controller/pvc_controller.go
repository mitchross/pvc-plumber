// Package controller contains controller-runtime reconcilers for pvc-plumber.
//
// PVCReconciler owns the lifecycle of the per-PVC ExternalSecret /
// ReplicationSource / ReplicationDestination triplet (originally ported
// from Kyverno generate rules 5–7) plus the orphan-reaper that the Kyverno
// implementation could not express. The reconciler does not reach into the
// Kopia repository itself — that lives in the webhook layer.
//
// v3.0.0: ensureExternalSecret renders an S3-flavored kopia-credentials
// payload (was: filesystem:///repository). It also runs a one-time
// delete-and-recreate migration when it encounters the legacy v2.x shape,
// so the v2 → v3 cutover doesn't require manual ES recycling.
package controller

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// k8sLabelValueMaxLen is the Kubernetes API server's hard limit on the
// length of a label value (RFC 1123 / Kubernetes API conventions). Label
// selectors that exceed this fail validation with
// "must be no more than 63 characters".
//
// The reconciler uses a label selector keyed off `volsync.backup/pvc=<pvc>`
// to find children for cleanup; PVC names longer than this would either
// fail the selector outright or — depending on apiserver version — silently
// drop the cleanup query. labelSafePVCRef() truncates+hashes any over-limit
// PVC name into a deterministic 28-char value (`pvc-` + first 24 hex chars
// of sha256) that still uniquely identifies the source PVC.
const k8sLabelValueMaxLen = 63

// Label keys applied to every child resource so cleanup can find them by
// selector and so cluster operators can `kubectl get -l ...` to inspect what
// pvc-plumber owns. Drift is intentionally NOT reconciled — operators are
// allowed to hand-edit the children if they need to (e.g. tweaking retain
// counts for a high-churn PVC) without us stomping on the change.
const (
	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "pvc-plumber"
	pvcLabel       = "volsync.backup/pvc"

	backupLabelKey = "backup"
	backupHourly   = "hourly"
	backupDaily    = "daily"

	// dataField is the literal field name `data` used as a key in the
	// unstructured ExternalSecret spec we render — it appears both inside
	// `spec.target.template.data` (the rendered Secret payload key/value
	// map) and at `spec.data` (the list of remoteRef rules). Both are
	// the ES CRD's documented field names; kept as one constant since
	// they're identical strings and goconst treats them as one occurrence
	// set.
	dataField = "data"

	// backupExemptLabel opts a PVC out of all pvc-plumber backup automation
	// (no ES/RS/RD created; admission webhooks short-circuit allow). Pairs
	// with the required `backupExemptReasonAnnot` audit-trail annotation.
	// See v3 spec § "Labels and annotations contract".
	backupExemptLabel = "backup-exempt"

	// labelTrue is the string-literal "true" used as the value of
	// boolean-like labels (e.g. `backup-exempt: "true"`). Kept as a constant
	// so goconst doesn't flag every comparison and so a future flip to a
	// typed bool (or a different sentinel) only changes one place.
	labelTrue = "true"

	// ExternalSecret/spec field name literals used when assembling the
	// unstructured object the reconciler creates. These are the ES v1 CRD's
	// documented field names; centralizing them keeps the rendered map
	// keys consistent and silences goconst on the repeated literals.
	esFieldRefreshInterval = "refreshInterval"
	esFieldName            = "name"
	esFieldTarget          = "target"
	esFieldCreationPolicy  = "creationPolicy"
	esFieldTemplate        = "template"
	esFieldRemoteRef       = "remoteRef"
	esFieldKey             = "key"
	esFieldProperty        = "property"
	esFieldSecretKey       = "secretKey"

	// Per-PVC kopia env-var keys rendered into the ES `target.template.data`
	// map. Mover Jobs read these directly. KOPIA_REPOSITORY shape is
	// `s3://<bucket>` so kopia infers backend=s3.
	kopiaEnvRepository    = "KOPIA_REPOSITORY"
	kopiaEnvPassword      = "KOPIA_PASSWORD"
	kopiaEnvS3Endpoint    = "KOPIA_S3_ENDPOINT"
	kopiaEnvS3Bucket      = "KOPIA_S3_BUCKET"
	kopiaEnvS3DisableTLS  = "KOPIA_S3_DISABLE_TLS"
	awsEnvAccessKeyID     = "AWS_ACCESS_KEY_ID"
	awsEnvSecretAccessKey = "AWS_SECRET_ACCESS_KEY"

	// creationPolicyOwner is the ES creationPolicy value that has the
	// rendered Secret garbage-collected with the ES. Constant so a switch
	// to a different policy is a one-line change.
	creationPolicyOwner = "Owner"
)

// GVKs for the three child kinds. We use unstructured everywhere so the
// operator does not have a hard dependency on volsync / external-secrets
// types — those CRDs ship out-of-band and we don't want to pin their schemas
// in our go.mod.
var (
	esGVK = schema.GroupVersionKind{
		Group:   "external-secrets.io",
		Version: "v1",
		Kind:    "ExternalSecret",
	}
	rsGVK = schema.GroupVersionKind{
		Group:   "volsync.backube",
		Version: "v1alpha1",
		Kind:    "ReplicationSource",
	}
	rdGVK = schema.GroupVersionKind{
		Group:   "volsync.backube",
		Version: "v1alpha1",
		Kind:    "ReplicationDestination",
	}

	childGVKs = []schema.GroupVersionKind{esGVK, rsGVK, rdGVK}
)

// ExternalSecretConfig captures the (defaulted, env-var-overridable)
// rendering parameters the reconciler needs when it templates the per-PVC
// `volsync-<pvc>` ExternalSecret. Centralizing them here keeps the secret
// store / vault item / property names out of code constants — they're
// configuration in v3.0.0 (resolved v2 quirk #1, #2, #3 from
// MIGRATION-v1-to-v2.md § 5).
//
// All fields are required at construction time; cmd/operator/main.go applies
// defaults via internal/config.loadExternalSecretsConfig before passing the
// struct in, so an unconfigured reconciler is a programmer error rather than
// a silent broken-template footgun.
type ExternalSecretConfig struct {
	// SecretStoreName is the ClusterSecretStore name the rendered ES
	// references (default `1password`).
	SecretStoreName string
	// VaultKey is the remoteRef.key — the secret store item that holds
	// the kopia password and S3 admin keys (default `rustfs`).
	VaultKey string
	// KopiaPasswordProperty is the remoteRef.property for the kopia repo
	// password (default `kopia_password`).
	KopiaPasswordProperty string
	// S3AccessKeyProperty is the remoteRef.property for AWS_ACCESS_KEY_ID
	// (default `k8s-admin-access-key`).
	S3AccessKeyProperty string
	// S3SecretKeyProperty is the remoteRef.property for AWS_SECRET_ACCESS_KEY
	// (default `k8s-admin-secret-key`).
	S3SecretKeyProperty string
	// S3Endpoint is rendered into the ES `target.template.data` map as
	// KOPIA_S3_ENDPOINT — VolSync mover Jobs read this to find RustFS.
	S3Endpoint string
	// S3Bucket is rendered as KOPIA_S3_BUCKET. Also baked into the
	// KOPIA_REPOSITORY URL (`s3://<bucket>`) so kopia knows the backend
	// type from the URL alone.
	S3Bucket string
	// S3DisableTLS is rendered as the string "true"/"false" under
	// KOPIA_S3_DISABLE_TLS. In-cluster RustFS over HTTP needs "true".
	S3DisableTLS bool
}

// PVCReconciler reconciles PersistentVolumeClaim objects that carry a
// `backup: hourly|daily` label. It owns the lifecycle of the companion
// ExternalSecret, ReplicationSource, and ReplicationDestination resources.
//
// SystemNamespaces is the set of namespaces the reconciler refuses to
// service. Always set at startup from parseSystemNamespaces() in
// cmd/operator/main.go; a nil map would process every namespace, which would
// be a configuration error.
type PVCReconciler struct {
	client.Client

	// SystemNamespaces is the set of namespaces excluded from backup
	// management (kube-system, volsync-system, kyverno, …). Membership is
	// checked with `_, ok := SystemNamespaces[ns]`.
	SystemNamespaces map[string]struct{}

	// ExternalSecret holds the rendering parameters for per-PVC
	// `volsync-<pvc>` ExternalSecret objects. See ExternalSecretConfig.
	ExternalSecret ExternalSecretConfig
}

// RBAC for this controller is hand-managed in
// `infrastructure/controllers/pvc-plumber/rbac.yaml` (in the
// talos-argocd-proxmox repo). Do NOT add `kubebuilder:rbac` markers here —
// the cluster manifests are the source of truth, and stale markers risk
// drift if anyone runs `controller-gen`. The hand-written ClusterRole
// includes leases (leader election), events (controller-runtime event
// emission), and `patch` verbs that the markers historically omitted; those
// are required for the manager to start at all, so silent regeneration would
// be a hard failure.

// Reconcile is the controller entrypoint. The flow:
//
//  1. System-namespace check FIRST — before any cleanup() call. The
//     reconciler refuses to touch PVCs in infrastructure namespaces (kube-
//     system, longhorn-system, prometheus-stack, …) and, critically, must
//     not even attempt to build a label selector for them. Long-named PVCs
//     in monitoring (e.g. `prometheus-kube-prometheus-stack-prometheus-db-…`,
//     104 chars) would otherwise trip the 63-byte label-value limit on the
//     selector and crash-loop the reconciler. (See the v3.1.0 CHANGELOG for
//     the 2026-05-08 incident this guard prevents.)
//  2. PVC gone, deleting, unlabeled, or backup-exempt → cleanup any
//     orphaned children and exit. cleanup is idempotent and tolerates the
//     "no children to delete" case, so calling it on the happy-path early
//     exits is cheap.
//  3. Otherwise: ensure ExternalSecret + ReplicationDestination immediately
//     (the RD must exist before the PVC binds so it can serve as a
//     dataSourceRef target during restore).
//  4. ReplicationSource is gated: only created once the PVC is Bound AND at
//     least 2h old. The age gate prevents backups from snapshotting an
//     empty volume right after a fresh restore — the application needs time
//     to land its data.
func (r *PVCReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pvc", req.NamespacedName)

	// Step 1: system-namespace short-circuit. Performed BEFORE the Get +
	// cleanup() path so an over-long PVC name in an infra namespace can
	// never reach the label-selector code. Defense-in-depth against a
	// webhook namespaceSelector misconfiguration (the cluster manifest
	// already excludes these namespaces, but if drift between
	// SystemNamespaces and the webhook namespaceSelector ever occurs,
	// THIS branch is what keeps the reconciler safe).
	if _, inSystemNS := r.SystemNamespaces[req.Namespace]; inSystemNS {
		logger.V(1).Info("skipping reconcile in system namespace")
		return ctrl.Result{}, nil
	}

	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, req.NamespacedName, &pvc); err != nil {
		if apierrors.IsNotFound(err) {
			// PVC fully deleted — best-effort orphan reap. The Kyverno
			// implementation could not do this; orphans accumulated forever.
			if cerr := r.cleanup(ctx, req.Namespace, req.Name); cerr != nil {
				return ctrl.Result{}, cerr
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	label := pvc.Labels[backupLabelKey]
	isBackupLabeled := label == backupHourly || label == backupDaily
	// `backup-exempt: "true"` is the explicit "this PVC is intentionally not
	// backed up" opt-out. Treat it the same as label-removed: if a previously
	// backup-labeled PVC has exempt added, reap any managed-by children so we
	// don't keep a stale ES/RS/RD pinned to an exempt PVC.
	isExempt := pvc.Labels[backupExemptLabel] == labelTrue

	if !pvc.DeletionTimestamp.IsZero() || !isBackupLabeled || isExempt {
		if err := r.cleanup(ctx, pvc.Namespace, pvc.Name); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// ExternalSecret + ReplicationDestination land first, regardless of
	// bind state — the RD has to exist before a restoring PVC can point a
	// dataSourceRef at it.
	if err := r.ensureExternalSecret(ctx, &pvc); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureReplicationDestination(ctx, &pvc); err != nil {
		return ctrl.Result{}, err
	}

	// Backup ReplicationSource is gated on (Bound, age ≥ 2h).
	if pvc.Status.Phase != corev1.ClaimBound {
		logger.V(1).Info("pvc not bound yet, requeueing", "phase", pvc.Status.Phase)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	age := time.Since(pvc.CreationTimestamp.Time)
	if age < 2*time.Hour {
		return ctrl.Result{RequeueAfter: 2*time.Hour - age}, nil
	}
	if err := r.ensureReplicationSource(ctx, &pvc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// backupSchedule returns a deterministic crontab string for the given PVC.
// minute = first 4 bytes of sha256(ns + "/" + pvcName) interpreted as
// big-endian uint32, modulo 60. Distributes uniformly across the minute
// field regardless of name-length clustering, which the previous
// length-mod-60 formula suffered from (PVCs with names of similar length
// all landed on the same minute).
//
// Replaces the temporary length-mod approach inherited from the original
// Kyverno generate rule. Existing ReplicationSources keep their generated
// minute (they're idempotent Get-or-Create); only newly-created PVCs get
// the new schedule. The ns/pvcName separator is `"/"` (matching the v3
// spec's `sha256(ns + "/" + pvc)` formula); the previous separator was `-`,
// which the test suite explicitly pinned to keep the formula stable.
func backupSchedule(namespace, pvcName, label string) string {
	sum := sha256.Sum256([]byte(namespace + "/" + pvcName))
	minute := int(binary.BigEndian.Uint32(sum[:4]) % 60)
	if label == backupHourly {
		return fmt.Sprintf("%d * * * *", minute)
	}
	return fmt.Sprintf("%d 2 * * *", minute)
}

// ensureExternalSecret is a Get-or-Create for the per-PVC kopia-credentials
// ES. Drift is NOT reconciled in steady state (we do not Update on
// existing). EXCEPTION: v3.0.0 introduces a one-time migration helper —
// when an ES still carries the legacy `KOPIA_REPOSITORY: filesystem:///repository`
// shape from v2.x, the reconciler deletes and recreates it in the new S3
// shape on the next reconcile pass. This is the ONLY drift correction the
// reconciler performs, and it exists solely so the v2.x → v3.0.0 cutover
// doesn't require manual `kubectl delete externalsecret` for every PVC.
//
// After every existing volsync-* ES has been recycled (one reconcile cycle
// per labeled PVC), the migration is a no-op and stays that way. The
// recycled ES will cause ESO to refresh the underlying Secret with the new
// S3 env vars (KOPIA_REPOSITORY=s3://<bucket>, KOPIA_S3_ENDPOINT, …); on
// VolSync mover Jobs' next run they pick up the new Secret values and
// connect to S3 instead of the legacy NFS mount.
func (r *PVCReconciler) ensureExternalSecret(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	name := "volsync-" + pvc.Name

	// One-time migration: legacy filesystem-shaped ES → S3 shape.
	if recycle, err := r.legacyESNeedsRecycle(ctx, pvc.Namespace, name); err != nil {
		return err
	} else if recycle {
		log.FromContext(ctx).Info("recycling legacy filesystem-shaped ExternalSecret to S3 shape (v3.0.0 migration)",
			"namespace", pvc.Namespace, "name", name)
		stale := &unstructured.Unstructured{}
		stale.SetGroupVersionKind(esGVK)
		stale.SetNamespace(pvc.Namespace)
		stale.SetName(name)
		if err := r.Delete(ctx, stale); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete legacy ES %s/%s: %w", pvc.Namespace, name, err)
		}
		// Fall through to the Create below.
	} else {
		// Steady-state Get-or-Create: skip if already present in v3 shape.
		if exists, err := r.exists(ctx, esGVK, pvc.Namespace, name); err != nil || exists {
			return err
		}
	}

	cfg := r.ExternalSecret
	es := newUnstructured(esGVK, pvc.Namespace, name, pvc.Name)
	es.Object["spec"] = map[string]interface{}{
		esFieldRefreshInterval: "1h",
		"secretStoreRef": map[string]interface{}{
			"kind":      "ClusterSecretStore",
			esFieldName: cfg.SecretStoreName,
		},
		esFieldTarget: map[string]interface{}{
			esFieldName:           name,
			esFieldCreationPolicy: creationPolicyOwner,
			// Retain on ES delete: the rendered Secret is consumed by
			// VolSync mover Jobs that may outlive the ES. Pre-v3 left
			// this default (Delete); v3 makes it explicit.
			"deletionPolicy": "Retain",
			esFieldTemplate: map[string]interface{}{
				"engineVersion": "v2",
				"mergePolicy":   "Merge",
				"metadata": map[string]interface{}{
					"labels": map[string]interface{}{
						managedByLabel: managedByValue,
						pvcLabel:       pvc.Name,
					},
				},
				// Mover Jobs read these env vars directly. Bucket name is
				// embedded in the URL so kopia infers backend=s3.
				dataField: map[string]interface{}{
					kopiaEnvRepository:   "s3://" + cfg.S3Bucket,
					kopiaEnvS3Endpoint:   cfg.S3Endpoint,
					kopiaEnvS3Bucket:     cfg.S3Bucket,
					kopiaEnvS3DisableTLS: boolToString(cfg.S3DisableTLS),
				},
			},
		},
		dataField: []interface{}{
			esRemoteRef(kopiaEnvPassword, cfg.VaultKey, cfg.KopiaPasswordProperty),
			esRemoteRef(awsEnvAccessKeyID, cfg.VaultKey, cfg.S3AccessKeyProperty),
			esRemoteRef(awsEnvSecretAccessKey, cfg.VaultKey, cfg.S3SecretKeyProperty),
		},
	}
	return r.Create(ctx, es)
}

// esRemoteRef returns one entry of the ExternalSecret `spec.data` list — a
// `secretKey` plus a `remoteRef` pointing at one (key, property) pair in the
// configured secret store. Extracted into a helper so the three identical
// shapes in ensureExternalSecret read cleanly and so a future ES schema
// change (e.g. ExternalSecretsV1Beta1 deprecating `remoteRef` keys) is a
// one-place edit.
func esRemoteRef(secretKey, storeKey, property string) map[string]interface{} {
	return map[string]interface{}{
		esFieldSecretKey: secretKey,
		esFieldRemoteRef: map[string]interface{}{
			esFieldKey:      storeKey,
			esFieldProperty: property,
		},
	}
}

// legacyESNeedsRecycle returns true when an ExternalSecret with the given
// name exists and still carries the v2.x filesystem template shape
// (KOPIA_REPOSITORY=filesystem:///repository). Used by ensureExternalSecret
// to drive the one-time v2→v3 migration delete-and-recreate.
//
// We deliberately key off the literal "filesystem://" prefix rather than the
// full string so a partially-migrated ES (e.g. an operator hand-edit that
// adjusted KOPIA_FS_PATH but left KOPIA_REPOSITORY) is still detected as
// legacy and recycled.
func (r *PVCReconciler) legacyESNeedsRecycle(ctx context.Context, namespace, name string) (bool, error) {
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(esGVK)
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, got); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	repo, _, err := unstructured.NestedString(got.Object, "spec", "target", "template", "data", "KOPIA_REPOSITORY")
	if err != nil {
		// Field is the wrong type; nothing we can do beyond letting the
		// next ensure cycle attempt a Create (which will fail with already
		// exists). Treat as non-recycle to avoid clobbering a hand-edited ES.
		return false, nil
	}
	return strings.HasPrefix(repo, "filesystem://"), nil
}

// boolToString renders a Go bool as the lowercase "true"/"false" string the
// ExternalSecret template data map needs. Centralized so a future flip to
// numeric "1"/"0" or capital "True"/"False" only changes one place.
func boolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// ensureReplicationSource is a Get-or-Create for the backup schedule.
// Spec is a direct port of Kyverno rule 6.
func (r *PVCReconciler) ensureReplicationSource(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	name := pvc.Name + "-backup"
	if exists, err := r.exists(ctx, rsGVK, pvc.Namespace, name); err != nil || exists {
		return err
	}

	label := pvc.Labels[backupLabelKey]
	rs := newUnstructured(rsGVK, pvc.Namespace, name, pvc.Name)
	rs.Object["spec"] = map[string]interface{}{
		"sourcePVC": pvc.Name,
		"trigger": map[string]interface{}{
			"schedule": backupSchedule(pvc.Namespace, pvc.Name, label),
		},
		"kopia": map[string]interface{}{
			"repository":  "volsync-" + pvc.Name,
			"compression": "zstd-fastest",
			"parallelism": int64(2),
			"retain": map[string]interface{}{
				"hourly":  int64(24),
				"daily":   int64(7),
				"weekly":  int64(4),
				"monthly": int64(2),
			},
			"copyMethod":              "Snapshot",
			"storageClassName":        "longhorn",
			"volumeSnapshotClassName": "longhorn-snapclass",
			"cacheCapacity":           "2Gi",
			"moverSecurityContext": map[string]interface{}{
				"runAsUser":  int64(568),
				"runAsGroup": int64(568),
				"fsGroup":    int64(568),
			},
		},
	}
	return r.Create(ctx, rs)
}

// ensureReplicationDestination is a Get-or-Create for the restore target.
// Spec is a direct port of Kyverno rule 7. accessModes / capacity come from
// the PVC; everything else is operator-fixed.
func (r *PVCReconciler) ensureReplicationDestination(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	name := pvc.Name + "-backup"
	if exists, err := r.exists(ctx, rdGVK, pvc.Namespace, name); err != nil || exists {
		return err
	}

	accessMode := string(corev1.ReadWriteOnce)
	if len(pvc.Spec.AccessModes) > 0 {
		accessMode = string(pvc.Spec.AccessModes[0])
	}
	var capacity string
	if storage, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		capacity = storage.String()
	}

	rd := newUnstructured(rdGVK, pvc.Namespace, name, pvc.Name)
	rd.Object["spec"] = map[string]interface{}{
		"trigger": map[string]interface{}{
			"manual": "restore-once",
		},
		"kopia": map[string]interface{}{
			"repository":              "volsync-" + pvc.Name,
			"copyMethod":              "Snapshot",
			"storageClassName":        "longhorn",
			"volumeSnapshotClassName": "longhorn-snapclass",
			"accessModes":             []interface{}{accessMode},
			"capacity":                capacity,
			"cacheCapacity":           "2Gi",
			"moverSecurityContext": map[string]interface{}{
				"runAsUser":  int64(568),
				"runAsGroup": int64(568),
				"fsGroup":    int64(568),
			},
		},
	}
	return r.Create(ctx, rd)
}

// cleanup deletes every child resource labeled `volsync.backup/pvc=<ref>`
// in the given namespace. It is the orphan reaper: when a PVC is removed
// (or the backup label is dropped, or someone moves the PVC to a system
// namespace), the children must follow. NotFound on individual deletes
// and missing-CRD errors on List are both swallowed — see
// ignoreNotFoundOrNoMatch.
//
// v3.1.0: the `volsync.backup/pvc=` label value is computed by
// labelSafePVCRef so PVC names exceeding the 63-byte label-value limit
// are mapped to a deterministic 28-char hashed identifier. Without this,
// the reconciler crashed in error-loop on monitoring-stack PVCs whose
// names approach 100 chars (prometheus / alertmanager statefulsets). The
// system-namespace short-circuit at the top of Reconcile() means
// monitoring-stack PVCs never reach this path in normal operation —
// labelSafePVCRef is defense-in-depth for application-namespace PVCs
// that legitimately have long names.
func (r *PVCReconciler) cleanup(ctx context.Context, namespace, name string) error {
	ref := labelSafePVCRef(name)
	for _, gvk := range childGVKs {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk)
		if err := r.List(ctx, list,
			client.InNamespace(namespace),
			client.MatchingLabels{pvcLabel: ref},
		); err != nil {
			if e := ignoreNotFoundOrNoMatch(err); e != nil {
				return e
			}
			// CRD missing or namespace gone — no orphans of this kind
			// could possibly exist; move to the next GVK.
			continue
		}
		for i := range list.Items {
			item := list.Items[i]
			if err := r.Delete(ctx, &item); err != nil {
				if e := ignoreNotFoundOrNoMatch(err); e != nil {
					return e
				}
			}
		}
	}
	return nil
}

// labelSafePVCRef returns a value safe to use as a Kubernetes label value
// (max 63 bytes, RFC 1123 subset) that identifies the PVC. Names short
// enough to fit are returned as-is; names longer than 63 bytes are mapped
// to `pvc-<sha256-prefix>` (28 bytes total).
//
// Determinism is the contract: the same PVC name MUST always map to the
// same ref so List by selector can find children created on a previous
// reconcile. SHA-256 satisfies that, and a 24-char hex prefix (96 bits) is
// far beyond the collision threshold for any realistic cluster size.
//
// Length budget for the hashed form:
//
//	"pvc-" (4) + 24 hex chars = 28 bytes — well under the 63-byte limit.
//
// The "pvc-" prefix marks the value as a hashed reference rather than a
// raw name; cluster operators searching for a child by `kubectl get
// externalsecret -l volsync.backup/pvc=<name>` against a long-named PVC
// will fall back to grepping by the hash, which the operator's reconcile
// log emits whenever it processes a long-named PVC.
func labelSafePVCRef(pvcName string) string {
	if len(pvcName) <= k8sLabelValueMaxLen {
		return pvcName
	}
	sum := sha256.Sum256([]byte(pvcName))
	return "pvc-" + hex.EncodeToString(sum[:12]) // 12 bytes = 24 hex chars
}

// ignoreNotFoundOrNoMatch swallows the two error classes that mean "the
// thing you're looking for doesn't exist, and that's fine":
//
//  1. apierrors.IsNotFound — HTTP 404 from the API server (object or
//     namespace was already deleted, or a Delete raced with another
//     reaper).
//  2. *meta.NoKindMatchError — the REST mapper has no entry for this
//     GVK, which happens when the CRD itself isn't installed in the
//     cluster. This is the normal state at first-boot before VolSync /
//     external-secrets have been applied, and in dev/test clusters that
//     don't run the full stack. Without this branch the reconciler enters
//     an infinite-requeue loop on a backup-labeled PVC during bootstrap.
//
// All other errors propagate.
func ignoreNotFoundOrNoMatch(err error) error {
	if err == nil {
		return nil
	}
	if apierrors.IsNotFound(err) {
		return nil
	}
	if meta.IsNoMatchError(err) {
		return nil
	}
	return err
}

// exists is a small helper that returns (true, nil) if the named object
// exists, (false, nil) if it doesn't, and (false, err) on any other error.
// Centralizing this keeps each ensure* helper a few lines shorter and
// uniform in its idempotency story.
func (r *PVCReconciler) exists(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (bool, error) {
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvk)
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, got)
	switch {
	case err == nil:
		return true, nil
	case apierrors.IsNotFound(err):
		return false, nil
	default:
		return false, err
	}
}

// newUnstructured builds a child object with the standard managed-by /
// pvc-pointer labels already applied. Spec gets filled in by the caller.
//
// v3.1.0: pvcLabel uses labelSafePVCRef(pvcName) so long PVC names produce
// a label value that survives the 63-byte limit. Children created at v3.0.0
// against short-named PVCs keep their raw name labels and remain findable;
// only newly-created long-name children carry the hashed ref. cleanup()
// hashes consistently so the selector matches whatever the reconciler
// emitted at create time.
func newUnstructured(gvk schema.GroupVersionKind, namespace, name, pvcName string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(namespace)
	u.SetName(name)
	u.SetLabels(map[string]string{
		managedByLabel: managedByValue,
		pvcLabel:       labelSafePVCRef(pvcName),
	})
	return u
}

// SetupWithManager registers the reconciler with a controller-runtime
// manager and watches PersistentVolumeClaim. Children are NOT owned via
// owner references — we want them to outlive a transient PVC churn so a
// quick delete-and-recreate doesn't lose the schedule object — so there is
// no Owns() call here. The Reconcile loop's cleanup() is the only
// authoritative deleter.
func (r *PVCReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolumeClaim{}).
		Named("pvc-plumber-pvc").
		Complete(r)
}
