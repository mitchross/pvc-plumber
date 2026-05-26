package adopt

import (
	"time"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
)

// freshnessWindow returns the maximum age the latest backup may have
// for the given tier before BlockerStaleBackup fires. Returns
// (0, false) for tiers that have no freshness gate (disabled, manual,
// unspecified).
//
// Windows are tier-relative with cadence slack:
//
//	hourly  → 3h   (3x cadence)
//	daily   → 48h  (2x cadence)
//	weekly  → 216h ≈ 9d (1.3x cadence)
//	manual  → no window (operator-driven cadence)
//	disabled→ no window (no backup expected)
func freshnessWindow(tier labels.Tier) (time.Duration, bool) {
	switch tier {
	case labels.TierHourly:
		return 3 * time.Hour, true
	case labels.TierDaily:
		return 48 * time.Hour, true
	case labels.TierWeekly:
		return 9 * 24 * time.Hour, true
	default:
		return 0, false
	}
}

// freshnessBlockers evaluates Inputs.RequireFreshBackup against
// current.LastSyncTime. Returns the blockers that the caller should
// append (after applying AllowStale / AllowNoSuccessful flags).
//
// now is injected for testability. Pass time.Now() in production.
func freshnessBlockers(in Inputs, tier labels.Tier, current CurrentVolSyncSummary, now time.Time) []Blocker {
	if !in.RequireFreshBackup {
		return nil
	}
	window, gated := freshnessWindow(tier)
	if !gated {
		// tier is disabled / manual / unspecified — no freshness gate.
		return nil
	}
	if !current.RSPresent {
		// No RS at all — freshness is moot (the RS-missing blocker
		// will already have fired elsewhere). Don't double-report.
		return nil
	}
	if current.LastSyncTime == nil || current.LastSyncTime.IsZero() {
		if in.AllowNoSuccessfulBackup {
			return nil
		}
		return []Blocker{{
			Class:          BlockerNoSuccessfulBackup,
			Detail:         "RS has no lastSyncTime; no successful backup recorded",
			ResolvableWith: "AllowNoSuccessfulBackup=true (CLI: --allow-no-successful-backup --force)",
		}}
	}
	age := now.Sub(current.LastSyncTime.Time)
	if age <= window {
		return nil
	}
	if in.AllowStaleBackup {
		return nil
	}
	return []Blocker{{
		Class:          BlockerStaleBackup,
		Detail:         "latest backup is older than the tier window",
		ResolvableWith: "AllowStaleBackup=true (CLI: --allow-stale-backup)",
	}}
}
