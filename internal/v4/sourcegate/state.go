// Package sourcegate models the state machine that gates ReplicationSource
// creation for an opted-in PVC. Pure types and transition rules; no I/O.
//
// Phase 2 of docs/pvc-plumber-v4-prd.md in the talos-argocd-proxmox repo.
//
// The gate prevents the "empty baseline" failure mode where:
//
//  1. A PVC is created fresh.
//  2. The app boots and writes nothing yet.
//  3. The ReplicationSource fires its first scheduled backup.
//  4. The empty volume gets captured as the latest snapshot.
//  5. Future restores bind from that empty snapshot.
//
// Source gating defers RS creation until: the PVC is Bound; any required
// restore has completed; and the PVC has been Bound for at least
// MinBackupAge (default 2h).
package sourcegate

import (
	"fmt"
	"time"
)

// State enumerates the five gating states a PVC's ReplicationSource can be
// in. They are mutually exclusive at any given moment.
type State int

const (
	// Unknown is the zero value, used before any evaluation.
	Unknown State = iota

	// WaitingForPVCBound means the PVC has been created but its
	// .status.phase is not yet "Bound". Common during fresh deploys.
	WaitingForPVCBound

	// WaitingForRestore means the PVC has a dataSourceRef pointing at a
	// ReplicationDestination and the populator has not yet finished. The
	// app should not have started writing user data yet; backing up now
	// would capture a mid-restore state.
	WaitingForRestore

	// WaitingForMinAge means the PVC is Bound, no restore is in flight, but
	// MinBackupAge has not yet elapsed since the PVC's bind timestamp.
	// Protects against backing up a freshly-created empty volume.
	WaitingForMinAge

	// Ready means all gates have cleared; the reconciler may create or
	// update the ReplicationSource.
	Ready

	// Disabled means the PVC's effective tier is "disabled" or
	// `pvc-plumber.io/enabled` is not "true". The reconciler MUST remove
	// any existing operator-owned ReplicationSource. (RD is kept for
	// restore-on-recreate unless the PVC is also exempt.)
	Disabled

	// Error means the gate evaluation itself failed (e.g., couldn't read
	// PVC status). The reconciler should preserve any existing RS state
	// rather than churning it on transient errors.
	Error
)

// String returns the lowercase identifier used in events, metrics, and the
// `pvc-plumber.io/source-state` annotation written onto the PVC.
func (s State) String() string {
	switch s {
	case WaitingForPVCBound:
		return "waiting_for_pvc_bound"
	case WaitingForRestore:
		return "waiting_for_restore"
	case WaitingForMinAge:
		return "waiting_for_min_age"
	case Ready:
		return "ready"
	case Disabled:
		return "disabled"
	case Error:
		return "error"
	default:
		return "unknown"
	}
}

// PVCPhase mirrors the v1 PersistentVolumeClaimPhase strings the reconciler
// reads from k8s.io/api. Kept as a plain enum so this package doesn't pull
// in the k8s API types — callers translate.
type PVCPhase int

const (
	PhaseUnknown PVCPhase = iota
	PhasePending
	PhaseBound
	PhaseLost
)

// Inputs is the pure data needed to evaluate the gate. The caller supplies
// these from cluster reads; this package never reaches out for them.
type Inputs struct {
	// Enabled is whether the PVC is opted in via pvc-plumber.io/enabled.
	Enabled bool

	// TierDisabled is true when the parsed tier is "disabled".
	TierDisabled bool

	// PVCPhase is the source PVC's .status.phase translated from the k8s
	// PersistentVolumeClaimPhase string.
	PVCPhase PVCPhase

	// BoundAt is the wall-clock time the PVC reached Bound. Zero value
	// means "not yet bound" — used together with PVCPhase != PhaseBound.
	BoundAt time.Time

	// HasDataSourceRef is true when the PVC's spec.dataSourceRef points
	// at a ReplicationDestination.
	HasDataSourceRef bool

	// RestoreComplete is true when the ReplicationDestination reports the
	// populator has finished restoring data. Only consulted when
	// HasDataSourceRef is true.
	RestoreComplete bool

	// MinBackupAge is the duration that must elapse after BoundAt before
	// RS may run. Default at the call site (recommended 2h).
	MinBackupAge time.Duration

	// Now is the current time, injected for testability.
	Now time.Time

	// EvaluationError, if non-nil, short-circuits to State=Error. The
	// caller's reconciler can use this to preserve existing RS state
	// across transient read errors.
	EvaluationError error
}

// Evaluate is the pure transition function. It returns the gating State
// plus a short human reason suitable for an Event message or PVC status
// annotation. Returns Error if Inputs.EvaluationError is non-nil.
func Evaluate(in Inputs) (state State, reason string) {
	if in.EvaluationError != nil {
		return Error, fmt.Sprintf("gate evaluation failed: %v", in.EvaluationError)
	}
	if !in.Enabled || in.TierDisabled {
		if !in.Enabled {
			return Disabled, "pvc-plumber.io/enabled is not true"
		}
		return Disabled, "tier=disabled"
	}
	if in.PVCPhase != PhaseBound {
		return WaitingForPVCBound, fmt.Sprintf("PVC phase is %s, need Bound", phaseString(in.PVCPhase))
	}
	if in.HasDataSourceRef && !in.RestoreComplete {
		return WaitingForRestore, "PVC has dataSourceRef but restore has not completed"
	}
	if in.MinBackupAge > 0 {
		if in.BoundAt.IsZero() {
			// Defensive: phase=Bound but no BoundAt — treat as just-bound.
			return WaitingForMinAge, fmt.Sprintf("PVC has no BoundAt timestamp; deferring for %s", in.MinBackupAge)
		}
		elapsed := in.Now.Sub(in.BoundAt)
		if elapsed < in.MinBackupAge {
			remaining := in.MinBackupAge - elapsed
			return WaitingForMinAge, fmt.Sprintf("PVC bound %s ago; need %s more before first backup", elapsed.Truncate(time.Second), remaining.Truncate(time.Second))
		}
	}
	return Ready, "all gates cleared"
}

func phaseString(p PVCPhase) string {
	switch p {
	case PhasePending:
		return "Pending"
	case PhaseBound:
		return "Bound"
	case PhaseLost:
		return "Lost"
	default:
		return "Unknown"
	}
}

// AllowsRSCreate reports whether the reconciler should ensure a
// ReplicationSource exists in this state. Only Ready returns true. All
// other states either gate (Waiting*) or actively remove (Disabled) the
// RS, or preserve existing state (Error).
func (s State) AllowsRSCreate() bool { return s == Ready }

// TearsDownRS reports whether the reconciler should delete any existing
// operator-owned RS in this state. Only Disabled returns true.
func (s State) TearsDownRS() bool { return s == Disabled }

// IsTerminal reports whether the state will not change on its own with
// time. Disabled and Error are terminal until external action; the
// Waiting* states naturally transition with PVC progress and clock.
func (s State) IsTerminal() bool { return s == Disabled || s == Error }
