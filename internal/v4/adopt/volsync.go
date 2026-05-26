package adopt

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/planner"
)

// VolSync GVK fragments. Local constants so the unstructured client
// doesn't need the VolSync API module on the classpath.
const (
	volsyncGroup    = "volsync.backube"
	volsyncVersion  = "v1alpha1"
	kindRS          = "ReplicationSource"
	kindRD          = "ReplicationDestination"
	managedByArgoCD = "argocd"
	managedByHelm   = "Helm"
	managedByKustom = "kustomize"
)

// volsyncRSGVK / volsyncRDGVK are the GVKs adopt reads.
var (
	volsyncRSGVK = schema.GroupVersionKind{Group: volsyncGroup, Version: volsyncVersion, Kind: kindRS}
	volsyncRDGVK = schema.GroupVersionKind{Group: volsyncGroup, Version: volsyncVersion, Kind: kindRD}
)

// readReplicationSource looks up RS/<pvc-name> in <namespace>. Returns
// (nil, nil) if absent. Errors only on real infrastructure faults
// (RBAC denial, network).
func readReplicationSource(ctx context.Context, c client.Reader, namespace, pvcName string) (*unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(volsyncRSGVK)
	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pvcName}, u)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get ReplicationSource %s/%s: %w", namespace, pvcName, err)
	}
	return u, nil
}

// readReplicationDestination looks up RD/<pvc-name>-dst. Same contract
// as readReplicationSource.
func readReplicationDestination(ctx context.Context, c client.Reader, namespace, pvcName string) (*unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(volsyncRDGVK)
	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pvcName + "-dst"}, u)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get ReplicationDestination %s/%s-dst: %w", namespace, pvcName, err)
	}
	return u, nil
}

// classifyOwner returns the planner.OwnerClassification value for the
// observed RS+RD pair. Uses RS's app.kubernetes.io/managed-by label as
// the canonical signal; RD is checked only for the "no RS" edge case.
// Mirrors internal/controller's classifier so adopt's verdicts agree
// with /audit verbatim.
func classifyOwner(rs, rd *unstructured.Unstructured) planner.OwnerClassification {
	primary := rs
	if primary == nil {
		primary = rd
	}
	if primary == nil {
		return planner.OwnerNone
	}
	mb := primary.GetLabels()[labels.LabelManagedByKey]
	switch mb {
	case labels.LabelManagedByValue:
		return planner.OwnerPVCPlumber
	case managedByArgoCD, managedByHelm, managedByKustom:
		// "argocd" is the canonical Argo CD value; "Helm" and "kustomize"
		// are common upstream-chart values that should be treated as
		// GitOps-owned. We classify all three as InlineArgo for the
		// purposes of adopt (the operator does not write to any of them).
		return planner.OwnerInlineArgo
	case "":
		return planner.OwnerUnmanagedOrGitopsObserved
	default:
		return planner.OwnerUnknown
	}
}

// extractCurrent reads the observed shape fields from RS + RD into a
// CurrentVolSyncSummary. Missing fields stay at their zero value;
// callers compare to ExpectedVolSyncSummary to surface drift.
func extractCurrent(rs, rd *unstructured.Unstructured) CurrentVolSyncSummary {
	out := CurrentVolSyncSummary{
		RSPresent: rs != nil,
		RDPresent: rd != nil,
		Owner:     classifyOwner(rs, rd),
	}
	// RS-side fields (canonical source).
	if rs != nil {
		out.RepoSecret = nestedString(rs.Object, "spec", "kopia", "repository")
		out.Username = nestedString(rs.Object, "spec", "kopia", "username")
		out.Hostname = nestedString(rs.Object, "spec", "kopia", "hostname")
		out.CopyMethod = nestedString(rs.Object, "spec", "kopia", "copyMethod")
		out.SnapshotClass = nestedString(rs.Object, "spec", "kopia", "volumeSnapshotClassName")
		out.CacheCapacity = nestedString(rs.Object, "spec", "kopia", "cacheCapacity")
		out.StorageClass = nestedString(rs.Object, "spec", "kopia", "storageClassName")
		out.UID = nestedInt64(rs.Object, "spec", "kopia", "moverSecurityContext", "runAsUser")
		out.GID = nestedInt64(rs.Object, "spec", "kopia", "moverSecurityContext", "runAsGroup")
		out.FSGroup = nestedInt64(rs.Object, "spec", "kopia", "moverSecurityContext", "fsGroup")
		out.Schedule = nestedString(rs.Object, "spec", "trigger", "schedule")
		out.LastSyncTime = nestedTime(rs.Object, "status", "lastSyncTime")
	}
	// RD-only fallbacks (only used when RS is absent — RS values win).
	if rs == nil && rd != nil {
		out.RepoSecret = nestedString(rd.Object, "spec", "kopia", "repository")
		out.Username = nestedString(rd.Object, "spec", "kopia", "username")
		out.Hostname = nestedString(rd.Object, "spec", "kopia", "hostname")
		out.CopyMethod = nestedString(rd.Object, "spec", "kopia", "copyMethod")
		out.SnapshotClass = nestedString(rd.Object, "spec", "kopia", "volumeSnapshotClassName")
		out.CacheCapacity = nestedString(rd.Object, "spec", "kopia", "cacheCapacity")
		out.StorageClass = nestedString(rd.Object, "spec", "kopia", "storageClassName")
	}
	return out
}

func nestedString(obj map[string]interface{}, fields ...string) string {
	v, _, _ := unstructured.NestedString(obj, fields...)
	return v
}

// nestedInt64 reads a YAML integer that may have been deserialized as
// either int64 or float64 (controller-runtime fake clients normalize
// numeric scalars to float64 when round-tripping through JSON).
func nestedInt64(obj map[string]interface{}, fields ...string) *int64 {
	raw, found, _ := unstructured.NestedFieldNoCopy(obj, fields...)
	if !found {
		return nil
	}
	switch v := raw.(type) {
	case int64:
		return &v
	case int:
		x := int64(v)
		return &x
	case float64:
		x := int64(v)
		return &x
	default:
		return nil
	}
}

// nestedTime extracts a metav1.Time-shaped RFC3339 timestamp from an
// unstructured status field. Returns nil if absent or malformed.
func nestedTime(obj map[string]interface{}, fields ...string) *metav1.Time {
	s, _, _ := unstructured.NestedString(obj, fields...)
	if s == "" {
		return nil
	}
	t := metav1.Time{}
	if err := t.UnmarshalQueryParameter(s); err != nil {
		// Fall back to JSON unmarshalling, which accepts the RFC3339
		// form metav1.Time serializes in stored objects.
		jsonBytes := []byte("\"" + s + "\"")
		if err2 := t.UnmarshalJSON(jsonBytes); err2 != nil {
			return nil
		}
	}
	return &t
}
