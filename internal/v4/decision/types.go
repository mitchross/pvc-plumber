// Package decision is the pure decision engine for pvc-plumber v4. It
// composes inputs gathered by the webhook/reconciler/CLI (PVC labels,
// backup cache state, operator mode) into an Output that tells the
// caller whether to admit, mutate, deny, what to emit, and why.
//
// Phase 2 of docs/pvc-plumber-v4-prd.md in the talos-argocd-proxmox repo.
//
// This package has no Kubernetes client, no Kopia binary, no I/O. Pure
// function composition only. The 18-case failure matrix in §10 of the PRD
// is implemented as table-driven tests.
package decision

import (
	"time"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/mode"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// BackupState describes what the backup-truth cache (or live query) says
// about a PVC's kopia identity. Unknown is the third state — used when
// the cache hasn't loaded yet or the backend is unreachable.
type BackupState int

const (
	BackupUnknown BackupState = iota
	BackupMissing
	BackupExists
)

func (b BackupState) String() string {
	switch b {
	case BackupExists:
		return "exists"
	case BackupMissing:
		return "missing"
	default:
		return "unknown"
	}
}

// CacheFreshness is the operator's confidence in its BackupState answer.
// Stale + Exists/Missing is a real result that strict mode refuses to act
// on; Stale + Unknown is the worst case.
type CacheFreshness int

const (
	CacheFreshnessUnknown CacheFreshness = iota
	CacheFresh
	CacheStale
)

func (c CacheFreshness) String() string {
	switch c {
	case CacheFresh:
		return "fresh"
	case CacheStale:
		return "stale"
	default:
		return "unknown"
	}
}

// IdentityRef points at another PVC's backup identity, used for duplicate
// detection. The reconciler builds this slice by walking known identities;
// pass empty if duplicate detection is not desired at this call site.
type IdentityRef struct {
	Namespace string
	PVCName   string
	Identity  string
}

// Config carries the operator-level knobs the decision engine needs.
// Filled by the caller from its own config layer.
type Config struct {
	// NamingStrategy picks the convention for generated child names.
	NamingStrategy naming.Strategy

	// DefaultRepoSecretName is the kopia repo Secret the operator
	// references when a PVC has no override. Recommend keeping at
	// naming.DefaultRepoSecretName ("volsync-kopia-repository").
	DefaultRepoSecretName string

	// DefaultMinBackupAge is the gating window for fresh PVCs that have
	// no restore in flight. Recommend 2h for production.
	DefaultMinBackupAge time.Duration

	// ExcludedNamespaces is the set of namespaces in which all PVCs are
	// no-op for the decision engine (kube-system, volsync-system, etc.).
	// Defense-in-depth against webhook namespaceSelector drift.
	ExcludedNamespaces map[string]struct{}
}

// Input is the complete, pure input to Decide.
type Input struct {
	Namespace string
	PVCName   string

	// LabelSpec is the parsed labels/annotations from package labels.
	LabelSpec labels.Spec

	// Resolved is the mode + restore mode after precedence resolution.
	Resolved mode.Resolved

	// BackupState + CacheFreshness convey what the controller's cache
	// knows. Set BackupState=BackupUnknown when the cache hasn't loaded
	// or the backend is unreachable.
	BackupState    BackupState
	CacheFreshness CacheFreshness

	// KnownIdentities is optional; if non-empty, the engine checks for
	// duplicate backup identities and may flag (strict deny / others warn).
	KnownIdentities []IdentityRef

	// Config is the operator-wide config.
	Config Config

	// Now is the wall-clock time. Injected for testability; defaults to
	// time.Now() if zero.
	Now time.Time
}

// ReasonCode is a stable identifier for *why* the engine produced a given
// Output. Suitable for metrics labels and event reasons. Add new codes
// rather than overloading existing ones.
type ReasonCode string

const (
	ReasonAllowedNotOptedIn            ReasonCode = "AllowedNotOptedIn"
	ReasonAllowedExempt                ReasonCode = "AllowedExempt"
	ReasonAllowedExcludedNamespace     ReasonCode = "AllowedExcludedNamespace"
	ReasonAllowedFreshNoBackup         ReasonCode = "AllowedFreshNoBackup"
	ReasonAllowedRestoreInjected       ReasonCode = "AllowedRestoreInjected"
	ReasonAllowedSkipRestoreWithReason ReasonCode = "AllowedSkipRestoreWithReason"
	ReasonAllowedRestoreModeNever      ReasonCode = "AllowedRestoreModeNever"
	ReasonAllowedAuditModeWouldDeny    ReasonCode = "AllowedAuditModeWouldDeny"
	ReasonAllowedPermissiveWarn        ReasonCode = "AllowedPermissiveWarn"
	ReasonAllowedStaleCachePermissive  ReasonCode = "AllowedStaleCachePermissive"

	ReasonDeniedExemptMissingReason      ReasonCode = "DeniedExemptMissingReason"
	ReasonDeniedSkipRestoreMissingReason ReasonCode = "DeniedSkipRestoreMissingReason"
	ReasonDeniedInvalidConfig            ReasonCode = "DeniedInvalidConfig"
	ReasonDeniedBackupUnknownStrict      ReasonCode = "DeniedBackupUnknownStrict"
	ReasonDeniedBackupUnknownEnforce     ReasonCode = "DeniedBackupUnknownEnforce"
	ReasonDeniedCacheStaleStrict         ReasonCode = "DeniedCacheStaleStrict"
	ReasonDeniedRestoreForceNoBackup     ReasonCode = "DeniedRestoreForceNoBackup"
	ReasonDeniedRestoreForceUnknown      ReasonCode = "DeniedRestoreForceUnknown"
	ReasonDeniedDuplicateIdentityStrict  ReasonCode = "DeniedDuplicateIdentityStrict"
)

// Severity classifies the human urgency of the output's Message.
type Severity int

const (
	SeverityInfo Severity = iota
	SeverityWarn
	SeverityError // typically paired with Admit=false
)

func (s Severity) String() string {
	switch s {
	case SeverityWarn:
		return "warn"
	case SeverityError:
		return "error"
	default:
		return "info"
	}
}

// DataSourceRef is the mutation the admission mutator should apply to a
// PVC's spec.dataSourceRef when Output.Mutate is true.
type DataSourceRef struct {
	APIGroup string
	Kind     string
	Name     string
}

// Event captures a Kubernetes Event the caller should emit on the PVC.
type Event struct {
	Reason  string
	Type    string // "Normal" or "Warning"
	Message string
}

// Output is the complete decision. All fields populated; the caller picks
// what's relevant for its surface (admission vs reconcile).
type Output struct {
	// Admit is true when the PVC may proceed. For mutating webhooks this
	// is irrelevant (mutators always admit); for validating webhooks this
	// is the deny decision.
	Admit bool

	// Mutate is true when the mutator should inject DataSourceRef onto
	// the PVC. Always false when Admit is false.
	Mutate        bool
	DataSourceRef *DataSourceRef

	// ReasonCode + Message + Severity describe why this Output was
	// produced. ReasonCode is the stable label suitable for metrics;
	// Message is the human form suitable for events and logs.
	ReasonCode ReasonCode
	Message    string
	Severity   Severity

	// Effective records the resolved mode + restore mode for traceability.
	EffectiveMode        mode.Mode
	EffectiveModeSource  mode.Source
	EffectiveRestoreMode mode.RestoreMode

	// BackupIdentity is the resolved kopia identity for the PVC (either
	// the override or the default <namespace>/<pvc>).
	BackupIdentity string

	// Names is the deterministic naming for generated children.
	Names naming.Names

	// Events the caller should emit on the PVC.
	Events []Event

	// MetricsIncrement is the list of metric counter labels the caller
	// should record. Strings, not Prometheus types, to keep this pure.
	MetricsIncrement []string

	// ParseErrors aggregates any errors from labels.Spec.Errors plus
	// any mode-resolution errors. Useful for audit reporting even when
	// the engine still admits.
	ParseErrors []error
}
