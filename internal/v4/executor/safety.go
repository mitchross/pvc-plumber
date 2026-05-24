package executor

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
)

// Hard allow-list. The executor will mutate exactly two kinds, ever:
// VolSync ReplicationSource and ReplicationDestination. Anything else
// — Secrets, ExternalSecrets, PVCs, webhook configurations, ServiceAccounts,
// Pods — is refused at the executor boundary, even if the planner is
// buggy or compromised and produces an op carrying a foreign GVK.
//
// The GVK constants are intentionally re-declared here instead of imported
// from internal/v4/planner. Each package owns its own safety contract:
// the executor's allow-list is independent of what the planner *says* it
// will produce. If the planner package ever adds a new emit-able GVK,
// this allow-list must be updated explicitly — silent shadowing via an
// import would be a regression.
const (
	volsyncGroup   = "volsync.backube"
	volsyncVersion = "v1alpha1"
	kindRS         = "ReplicationSource"
	kindRD         = "ReplicationDestination"
)

// AllowedRSGVK / AllowedRDGVK are the only GVKs IsAllowedGVK will return
// true for. Exported so test code can compare against them without
// re-declaring its own copies.
var (
	AllowedRSGVK = schema.GroupVersionKind{Group: volsyncGroup, Version: volsyncVersion, Kind: kindRS}
	AllowedRDGVK = schema.GroupVersionKind{Group: volsyncGroup, Version: volsyncVersion, Kind: kindRD}

	allowedGVKs = map[schema.GroupVersionKind]struct{}{
		AllowedRSGVK: {},
		AllowedRDGVK: {},
	}
)

// IsAllowedGVK reports whether the executor is permitted to mutate
// resources of the given GVK. True only for VolSync ReplicationSource
// and ReplicationDestination. Pure function — no I/O.
func IsAllowedGVK(gvk schema.GroupVersionKind) bool {
	_, ok := allowedGVKs[gvk]
	return ok
}

// IsOperatorOwned reports whether the given live resource carries the
// canonical pvc-plumber managed-by label. Used by execUpdate and
// execDelete to gate mutations on ownership: if the live object isn't
// labeled by us, we MUST NOT update or delete it (it belongs to Argo,
// some other GitOps path, or a hand-applied YAML — adopting silently
// would be a contract violation).
//
// Pure function. Nil-safe: returns false on nil input.
func IsOperatorOwned(live *unstructured.Unstructured) bool {
	if live == nil {
		return false
	}
	return live.GetLabels()[labels.LabelManagedByKey] == labels.LabelManagedByValue
}
