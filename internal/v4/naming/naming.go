// Package naming computes deterministic names for the VolSync resources
// pvc-plumber generates for an opted-in PersistentVolumeClaim. Pure, no I/O.
//
// Phase 2 of docs/pvc-plumber-v4-prd.md in the talos-argocd-proxmox repo.
//
// The current inline pattern in the talos repo (post-c401822a) uses:
//
//	ReplicationSource      : <pvc>
//	ReplicationDestination : <pvc>-dst
//	Repository secret      : volsync-kopia-repository (shared, via ClusterES)
//
// This package's default strategy matches that convention so Phase 7 cutover
// is a labels/declaration change, not a rename. A legacy strategy exists for
// adopting v3-operator-era objects whose RS and RD were both named
// "<pvc>-backup" (see Phase 6 of the PRD).
package naming

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Strategy selects a deterministic naming scheme for generated children.
type Strategy int

const (
	// StrategyBareDst is the v4 default. It matches the talos-argocd-proxmox
	// inline pattern: RS name == PVC name; RD name == PVC name + "-dst".
	StrategyBareDst Strategy = iota

	// StrategyLegacyBackup matches the v3 operator's convention: both RS
	// and RD named "<pvc>-backup". Used by Phase 6 adoption to claim
	// orphaned cluster objects without renaming them. Avoid for new
	// resources; the kopia identity stays the same regardless.
	StrategyLegacyBackup

	// StrategyDstSrc is a future option discussed in PRD §17 (kept here as
	// a stub so callers can plumb the choice through without code changes
	// when we decide). Today identical to StrategyBareDst.
	StrategyDstSrc
)

func (s Strategy) String() string {
	switch s {
	case StrategyLegacyBackup:
		return "legacy-backup"
	case StrategyDstSrc:
		return "dst-src"
	default:
		return "bare-dst"
	}
}

// ParseStrategy accepts "bare-dst", "legacy-backup", or "dst-src"
// (case-insensitive). Empty string returns the default StrategyBareDst with
// nil error. Any other value errors.
func ParseStrategy(raw string) (Strategy, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "bare-dst":
		return StrategyBareDst, nil
	case "legacy-backup":
		return StrategyLegacyBackup, nil
	case "dst-src":
		return StrategyDstSrc, nil
	default:
		return StrategyBareDst, fmt.Errorf("invalid naming strategy %q (expected bare-dst|legacy-backup|dst-src)", raw)
	}
}

// Names is the result of Compute: the four names the operator needs to
// generate or adopt for one PVC.
type Names struct {
	// PVC is the source PVC name, echoed back for convenience.
	PVC string
	// RS is the ReplicationSource metadata.name.
	RS string
	// RD is the ReplicationDestination metadata.name.
	RD string
	// RepoSecret is the kopia repository Secret reference. Empty means
	// "use the operator default (volsync-kopia-repository)".
	RepoSecret string
	// LabelSelectorValue is the value of the volsync.backup/pvc label put
	// on generated children, used by the reconciler reaper to find them.
	// Equals PVC for short names; a hashed safe-ref for names >63 bytes.
	LabelSelectorValue string
}

// Compute returns the names for the given source PVC under the given
// strategy. pvcName must be non-empty; otherwise zero-value Names is
// returned. repoSecret is passed through; pass "" to fall back to the
// operator default at the call site.
func Compute(strategy Strategy, pvcName, repoSecret string) Names {
	if pvcName == "" {
		return Names{}
	}
	n := Names{
		PVC:                pvcName,
		RepoSecret:         repoSecret,
		LabelSelectorValue: LabelSafeRef(pvcName),
	}
	switch strategy {
	case StrategyLegacyBackup:
		n.RS = pvcName + "-backup"
		n.RD = pvcName + "-backup"
	case StrategyDstSrc:
		n.RS = pvcName
		n.RD = pvcName + "-dst"
	default: // StrategyBareDst
		n.RS = pvcName
		n.RD = pvcName + "-dst"
	}
	return n
}

// MaxLabelValueLen is the Kubernetes label-value length limit. Resources
// with names longer than this cannot be selected by the reaper without
// hashing.
const MaxLabelValueLen = 63

// LabelSafeRef returns a deterministic, label-safe value for the given
// resource name. Short names pass through unchanged; long names become
// "pvc-<24 hex chars>" derived from sha256(name). The "pvc-" prefix
// matches the v3.1.0 reconciler convention so adoption in Phase 6 is
// label-compatible with existing cluster state.
func LabelSafeRef(name string) string {
	if len(name) <= MaxLabelValueLen {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	return "pvc-" + hex.EncodeToString(sum[:])[:24]
}

// DefaultRepoSecretName is the shared kopia repo Secret materialized by
// the ClusterExternalSecret in infrastructure/storage/volsync-backup-cluster/
// of the talos repo. Operator config can override; this is the default used
// when no override is set.
const DefaultRepoSecretName = "volsync-kopia-repository"

// KopiaIdentity holds the per-PVC kopia username/hostname pair. The shared
// repo at s3://volsync-kopia/cluster keys snapshots on these.
//
// Current convention in the talos repo (verified via inventory script):
//
//	username : <pvc-name>
//	hostname : <namespace>
//
// This package surfaces the convention so callers don't reinvent it.
type KopiaIdentity struct {
	Username string
	Hostname string
}

// IdentityFor returns the kopia identity for the given (namespace, pvc).
// The optional backupIdentityOverride (from pvc-plumber.io/backup-identity
// annotation) lets a PVC declare a stable identity across renames. When
// set, the override populates *both* username and hostname unchanged —
// the override is opaque to the operator.
func IdentityFor(namespace, pvcName, backupIdentityOverride string) KopiaIdentity {
	if backupIdentityOverride != "" {
		// The override is an opaque identifier. We use it as the username
		// and leave hostname blank for the override case; callers needing
		// the legacy two-field shape can split on "/" themselves. Keeping
		// this explicit avoids guessing semantics.
		return KopiaIdentity{Username: backupIdentityOverride}
	}
	return KopiaIdentity{Username: pvcName, Hostname: namespace}
}
