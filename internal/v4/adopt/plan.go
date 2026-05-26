package adopt

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/planner"
)

// Verdict is the top-level outcome of PlanFor.
type Verdict string

const (
	// VerdictSafeToAdopt means every check passed and the caller may
	// proceed to write LabelsToWrite / AnnotationsToWrite as-is.
	VerdictSafeToAdopt Verdict = "safe-to-adopt"

	// VerdictSafeToAdoptWithWarnings means the write is safe but the
	// operator should review Warnings first. This is the steady-state
	// verdict for drift-affected PVCs (dataSourceRef drift is a warning,
	// not a blocker, by design).
	VerdictSafeToAdoptWithWarnings Verdict = "safe-to-adopt-with-warnings"

	// VerdictAlreadyAdopted is the strict-equality state: BOTH v4 gates
	// are live AND RS/RD exist AND RS/RD are managed-by=pvc-plumber AND
	// their shape matches expected. Labels present without operator-owned
	// matching RS/RD are NOT AlreadyAdopted — those are intermediate
	// handoff states that emit warnings instead.
	VerdictAlreadyAdopted Verdict = "already-adopted"

	// VerdictBlocked means at least one hard-stop check fired. The
	// caller MUST NOT write labels. Blockers describe what to fix.
	VerdictBlocked Verdict = "blocked"
)

// BlockerClass identifies why an adoption was refused. Stable strings
// suitable for metrics labels and JSON output.
type BlockerClass string

const (
	BlockerSystemNamespace         BlockerClass = "system-namespace"
	BlockerPVCNotFound             BlockerClass = "pvc-not-found"
	BlockerPVCNotBound             BlockerClass = "pvc-not-bound"
	BlockerBackupExempt            BlockerClass = "backup-exempt"
	BlockerInvalidTier             BlockerClass = "invalid-tier"
	BlockerSpecParseError          BlockerClass = "spec-parse-error"
	BlockerMissingPrivilegedMovers BlockerClass = "missing-privileged-movers"
	BlockerRSMissing               BlockerClass = "rs-missing"
	BlockerRDMissing               BlockerClass = "rd-missing"
	BlockerOwnerUnknown            BlockerClass = "owner-unknown"
	BlockerRepoMismatch            BlockerClass = "repo-mismatch"
	BlockerUIDMismatch             BlockerClass = "uid-mismatch"
	BlockerGIDMismatch             BlockerClass = "gid-mismatch"
	BlockerFSGroupMismatch         BlockerClass = "fs-group-mismatch"
	BlockerSnapshotClassMismatch   BlockerClass = "snapshot-class-mismatch"
	BlockerCacheCapacityMismatch   BlockerClass = "cache-capacity-mismatch"
	BlockerStorageClassMismatch    BlockerClass = "storage-class-mismatch"
	BlockerCopyMethodMismatch      BlockerClass = "copy-method-mismatch"
	BlockerStaleBackup             BlockerClass = "stale-backup"
	BlockerNoSuccessfulBackup      BlockerClass = "no-successful-backup"
)

// WarningClass identifies a soft signal that doesn't block adoption.
type WarningClass string

const (
	WarningDataSourceRefDrift             WarningClass = "data-source-ref-drift"
	WarningArgoComparisonErrorLikely      WarningClass = "argo-comparison-error-likely"
	WarningLegacyBackupLabel              WarningClass = "legacy-backup-label-present"
	WarningUnmanagedOwnerShapeMatches     WarningClass = "unmanaged-owner-shape-matches"
	WarningCronWillBeRecomputed           WarningClass = "cron-will-be-recomputed"
	WarningLabelsPresentButHandoffPending WarningClass = "labels-present-but-handoff-pending"
	WarningLabelsPresentResourcesMissing  WarningClass = "labels-present-but-resources-not-managed"
)

// Blocker is a single hard-stop reason adoption was refused.
type Blocker struct {
	Class          BlockerClass
	Detail         string
	ResolvableWith string
}

// Warning is a single soft signal.
type Warning struct {
	Class  WarningClass
	Detail string
}

// PVCSummary is the observed PVC state echoed back to the caller for
// reporting and dry-run output.
type PVCSummary struct {
	Namespace            string
	Name                 string
	Phase                corev1.PersistentVolumeClaimPhase
	StorageClass         string
	AccessModes          []corev1.PersistentVolumeAccessMode
	Capacity             resource.Quantity
	DataSourceRef        string // live value; may differ from expected "<pvc>-dst"
	Exempt               bool
	ExemptReason         string
	PrivilegedMovers     bool
	HasLegacyBackupLabel bool
	V4GatesLive          bool // BOTH pvc-plumber.io/enabled and manage-volsync == true
}

// CurrentVolSyncSummary is the observed (RS, RD) pair. Pointer-typed
// integers distinguish "not present in RS spec" from "explicitly 0".
type CurrentVolSyncSummary struct {
	RSPresent     bool
	RDPresent     bool
	Owner         planner.OwnerClassification
	RepoSecret    string
	Username      string
	Hostname      string
	CopyMethod    string
	SnapshotClass string
	CacheCapacity string
	StorageClass  string
	UID           *int64
	GID           *int64
	FSGroup       *int64
	Schedule      string
	LastSyncTime  *metav1.Time
}

// ExpectedVolSyncSummary is what the builder would produce for the
// effective (PVC + Inputs + Defaults) tuple. Same shape as
// CurrentVolSyncSummary by design.
type ExpectedVolSyncSummary struct {
	RSName        string
	RDName        string
	RepoSecret    string
	Username      string
	Hostname      string
	CopyMethod    string
	SnapshotClass string
	CacheCapacity string
	StorageClass  string
	UID           int64
	GID           int64
	FSGroup       int64
	Schedule      string
}

// Plan is the full output of PlanFor.
type Plan struct {
	Verdict  Verdict
	Blockers []Blocker
	Warnings []Warning

	PVC      PVCSummary
	Current  CurrentVolSyncSummary
	Expected ExpectedVolSyncSummary

	// Parsed labels.Spec, surfaced for callers that want to inspect
	// derived fields (Origin, ManageVolSync, etc.) without re-parsing.
	Spec labels.Spec

	// LabelsToWrite are the v4 two-gate fuse labels (always 3 keys when
	// the verdict is Safe*). Empty for AlreadyAdopted and Blocked.
	LabelsToWrite map[string]string

	// AnnotationsToWrite are emitted only when an override differs from
	// the cluster default. Empty when all overrides match defaults.
	AnnotationsToWrite map[string]string
}

// hasBlocker reports whether the plan contains a blocker of the given
// class. Used by validate.go for ordering decisions and by tests.
func (p Plan) hasBlocker(class BlockerClass) bool {
	for _, b := range p.Blockers {
		if b.Class == class {
			return true
		}
	}
	return false
}

// hasWarning reports whether the plan contains a warning of the given
// class. Symmetrical to hasBlocker.
func (p Plan) hasWarning(class WarningClass) bool {
	for _, w := range p.Warnings {
		if w.Class == class {
			return true
		}
	}
	return false
}
