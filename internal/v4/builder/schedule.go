package builder

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
)

// ScheduleFor returns a deterministic crontab string for the given
// (namespace, pvc, tier). The formula is byte-equivalent to v3's
// internal/controller/pvc_controller.go::backupSchedule for backward
// compatibility — a v3-managed RS being cut over to v4 picks the same
// minute and avoids unnecessary schedule churn on the apiserver.
//
// minute = first 4 bytes of sha256(ns + "/" + pvcName) interpreted
// as big-endian uint32, modulo 60. Uniform distribution across the
// minute field regardless of name-length clustering (the previous
// length-mod-60 formula clumped same-length PVCs on the same minute).
//
// Tier mapping:
//   - hourly:   "<m> * * * *"             — every hour at minute m
//   - daily:    "<m> 2 * * *"             — 02:m every day
//   - weekly:   "<m> 2 * * 0"             — 02:m on Sunday
//   - manual:   "<m> 2 * * *"             — daily-format fallback, NOT
//     rendered into RS specs; BuildRS gives TierManual a
//     spec.trigger.manual instead. This return value exists only so
//     ScheduleFor stays total.
//   - disabled: "<m> 2 * * *"             — same fallback; planner suppresses
//     the RS entirely
//   - unspecified: "<m> 2 * * *"          — daily fallback for unset tier
//     (the planner surfaces a note that the default was applied)
//
// Pure: no I/O. The same (ns, pvc, tier) input always produces the
// same output, which means callers can safely log the predicted
// schedule and dry-run-diff the v4 plan without touching the cluster.
func ScheduleFor(namespace, pvcName string, tier labels.Tier) string {
	minute := scheduleMinute(namespace, pvcName)
	switch tier {
	case labels.TierHourly:
		return fmt.Sprintf("%d * * * *", minute)
	case labels.TierWeekly:
		return fmt.Sprintf("%d 2 * * 0", minute)
	case labels.TierDaily, labels.TierManual, labels.TierDisabled, labels.TierUnspecified:
		return fmt.Sprintf("%d 2 * * *", minute)
	default:
		// Unknown tier value (shouldn't happen — parseTier rejects
		// these). Defensive daily fallback rather than panic.
		return fmt.Sprintf("%d 2 * * *", minute)
	}
}

// scheduleMinute hashes (namespace, pvc) into a [0, 60) minute slot
// using the exact algorithm v3 used: sha256 first 4 bytes →
// big-endian uint32 → mod 60. Tested in schedule_test.go against
// representative real-world (ns, pvc) pairs from the talos repo.
func scheduleMinute(namespace, pvcName string) int {
	sum := sha256.Sum256([]byte(namespace + "/" + pvcName))
	return int(binary.BigEndian.Uint32(sum[:4]) % 60)
}
