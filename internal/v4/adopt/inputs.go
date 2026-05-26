package adopt

import (
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// Inputs is the total context PlanFor needs to produce an adoption plan.
//
// All fields are caller-supplied. The package does not read environment
// variables; the CLI wrapper (Patch 7.2) is responsible for loading
// PVC_PLUMBER_DEFAULT_* into Defaults and surfacing override flags into
// the *int64 / string fields.
//
// Pointer-typed overrides distinguish "operator did not pass an override"
// (nil) from "operator explicitly passed 0" (&0). Nil → fall back to
// Defaults. The same convention the runtimeconfig package uses.
type Inputs struct {
	Namespace string
	PVCName   string

	// Tier is the requested cadence: "hourly" / "daily" / "weekly" /
	// "manual" / "disabled". Parsed by validate.go via the same
	// vocabulary the labels package accepts. Empty string is treated
	// as invalid (operator must pick a tier explicitly).
	Tier string

	// Optional overrides. nil / "" → use Defaults.
	UID            *int64
	GID            *int64
	FSGroup        *int64
	SnapshotClass  string
	CacheCapacity  string
	StorageClass   string
	RepoSecret     string
	NamingStrategy naming.Strategy

	// Defaults carries the operator-resolved cluster defaults. The CLI
	// wrapper populates this from PVC_PLUMBER_DEFAULT_* env vars via the
	// runtimeconfig package. Empty / nil fields fall back to the values
	// the builder already encodes (naming.DefaultRepoSecretName, etc.)
	// at render time.
	Defaults Defaults

	// Freshness gating.
	//
	// RequireFreshBackup=true → evaluate the freshness window for the
	// chosen tier. Default value (zero-value false) skips freshness
	// checks entirely, suitable for callers that just want a shape
	// preview.
	//
	// AllowStaleBackup=true overrides BlockerStaleBackup.
	// AllowNoSuccessfulBackup=true overrides BlockerNoSuccessfulBackup.
	RequireFreshBackup      bool
	AllowStaleBackup        bool
	AllowNoSuccessfulBackup bool
}

// Defaults are the cluster-wide values the adopt planner uses when an
// Inputs override is absent. Mirrors the subset of runtimeconfig.Config
// that the builder consumes.
type Defaults struct {
	UID, GID, FSGroup int64
	SnapshotClass     string
	CacheCapacity     string
	StorageClass      string
	RepoSecret        string
}

// effectiveUID resolves the override-vs-default UID. The same shape is
// used for GID / FSGroup. Returned value is what the operator would
// write into moverSecurityContext.runAsUser; the caller decides whether
// to emit a pvc-plumber.io/uid annotation (only when override differs
// from Defaults.UID).
func (in Inputs) effectiveUID() int64 {
	if in.UID != nil {
		return *in.UID
	}
	return in.Defaults.UID
}

func (in Inputs) effectiveGID() int64 {
	if in.GID != nil {
		return *in.GID
	}
	return in.Defaults.GID
}

func (in Inputs) effectiveFSGroup() int64 {
	if in.FSGroup != nil {
		return *in.FSGroup
	}
	return in.Defaults.FSGroup
}

func (in Inputs) effectiveRepoSecret() string {
	if in.RepoSecret != "" {
		return in.RepoSecret
	}
	if in.Defaults.RepoSecret != "" {
		return in.Defaults.RepoSecret
	}
	return naming.DefaultRepoSecretName
}
