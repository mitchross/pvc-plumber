// Package controller contains controller-runtime reconcilers for pvc-plumber.
//
// Phase 2a: PVCReconciler ports Kyverno generate rules 5–7 (the
// ExternalSecret / ReplicationSource / ReplicationDestination triplet) plus
// the orphan-reaper logic that the previous Kyverno implementation could not
// express. The reconciler is the source of truth for the lifecycle of the
// generated children — it never reaches into Kopia or NFS itself; that's the
// webhook layer's job.
package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

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

// PVCReconciler reconciles PersistentVolumeClaim objects that carry a
// `backup: hourly|daily` label. It owns the lifecycle of the companion
// ExternalSecret, ReplicationSource, and ReplicationDestination resources.
//
// SystemNamespaces is the set of namespaces the reconciler refuses to
// service. It is configured at startup from a comma-separated env var (Phase
// 3 wires that — for now main.go leaves it nil, which means "service every
// namespace"). A nil map is safe: `_, ok := nilMap[k]` always returns false.
type PVCReconciler struct {
	client.Client

	// SystemNamespaces is the set of namespaces excluded from backup
	// management (kube-system, volsync-system, kyverno, …). Membership is
	// checked with `_, ok := SystemNamespaces[ns]`.
	SystemNamespaces map[string]struct{}
}

//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
//+kubebuilder:rbac:groups=external-secrets.io,resources=externalsecrets,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups=volsync.backube,resources=replicationsources;replicationdestinations,verbs=get;list;watch;create;update;delete

// Reconcile is the controller entrypoint. The flow:
//
//  1. PVC gone, deleting, unlabeled, or in a system namespace → cleanup any
//     orphaned children and exit. cleanup is idempotent and tolerates the
//     "no children to delete" case, so calling it on the happy-path early
//     exits is cheap.
//  2. Otherwise: ensure ExternalSecret + ReplicationDestination immediately
//     (the RD must exist before the PVC binds so it can serve as a
//     dataSourceRef target during restore).
//  3. ReplicationSource is gated: only created once the PVC is Bound AND at
//     least 2h old. The age gate prevents backups from snapshotting an
//     empty volume right after a fresh restore — the application needs time
//     to land its data.
func (r *PVCReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pvc", req.NamespacedName)

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
	_, inSystemNS := r.SystemNamespaces[pvc.Namespace]

	if !pvc.DeletionTimestamp.IsZero() || !isBackupLabeled || inSystemNS {
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

// backupSchedule mirrors Kyverno rule 6 EXACTLY. The minute is computed from
// the *length of the joined "ns-pvcName" string* — not len(ns)+len(pvcName)
// (which differs by 1 for the dash). This is a length-mod scheme, not a
// real hash; it's good enough for spreading <50 PVCs across the minute
// field but clusters once inventory grows. Replacing this with a sha-derived
// minute is a Phase 4 problem, not a Phase 2 problem.
func backupSchedule(namespace, pvcName, label string) string {
	minute := len(namespace+"-"+pvcName) % 60
	if label == backupHourly {
		return fmt.Sprintf("%d * * * *", minute)
	}
	return fmt.Sprintf("%d 2 * * *", minute)
}

// ensureExternalSecret is a Get-or-Create for the kopia-credentials ES.
// Drift is NOT reconciled (we do not Update on existing). Spec is a direct
// port of Kyverno rule 5.
func (r *PVCReconciler) ensureExternalSecret(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	name := "volsync-" + pvc.Name
	if exists, err := r.exists(ctx, esGVK, pvc.Namespace, name); err != nil || exists {
		return err
	}

	es := newUnstructured(esGVK, pvc.Namespace, name, pvc.Name)
	es.Object["spec"] = map[string]interface{}{
		"refreshInterval": "1h",
		"secretStoreRef": map[string]interface{}{
			"kind": "ClusterSecretStore",
			"name": "1password",
		},
		"target": map[string]interface{}{
			"name":           name,
			"creationPolicy": "Owner",
			"template": map[string]interface{}{
				"engineVersion": "v2",
				"mergePolicy":   "Merge",
				"metadata": map[string]interface{}{
					"labels": map[string]interface{}{
						managedByLabel: managedByValue,
						pvcLabel:       pvc.Name,
					},
				},
				"data": map[string]interface{}{
					"KOPIA_REPOSITORY": "filesystem:///repository",
					"KOPIA_FS_PATH":    "/repository",
				},
			},
		},
		"data": []interface{}{
			map[string]interface{}{
				"secretKey": "KOPIA_PASSWORD",
				"remoteRef": map[string]interface{}{
					"key":      "rustfs",
					"property": "kopia_password",
				},
			},
		},
	}
	return r.Create(ctx, es)
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

// cleanup deletes every child resource labeled `volsync.backup/pvc=<name>`
// in the given namespace. It is the orphan reaper: when a PVC is removed
// (or the backup label is dropped, or someone moves the PVC to a system
// namespace), the children must follow. NotFound on individual deletes is
// ignored — concurrent reaping is a feature, not a bug.
func (r *PVCReconciler) cleanup(ctx context.Context, namespace, name string) error {
	for _, gvk := range childGVKs {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk)
		if err := r.List(ctx, list,
			client.InNamespace(namespace),
			client.MatchingLabels{pvcLabel: name},
		); err != nil {
			// CRD not installed yet → NoMatch / NotFound. Either way, no
			// orphans of this kind exist; move on.
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		for i := range list.Items {
			item := list.Items[i]
			if err := r.Delete(ctx, &item); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
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
func newUnstructured(gvk schema.GroupVersionKind, namespace, name, pvcName string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(namespace)
	u.SetName(name)
	u.SetLabels(map[string]string{
		managedByLabel: managedByValue,
		pvcLabel:       pvcName,
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
