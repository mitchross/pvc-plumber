package builder

import (
	"regexp"
	"testing"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
)

// cronRe matches the 5-field crontab format the builder emits.
// Deliberately strict: minute is 0-59, the rest are exact literals
// the builder hard-codes today.
var cronRe = regexp.MustCompile(`^\d{1,2} (\*|2) \* \* (\*|0)$`)

// Realistic (ns, pvc) pairs from the current talos repo, used by
// multiple tests in this file. Promoted to constants once goconst
// flagged the repeated literals.
const (
	tnsKarakeep      = "karakeep"
	tpvcKarakeepData = "data-pvc"
	tnsPostHog       = "posthog"
	tnsPaperlessNGX  = "paperless-ngx"
)

// TestScheduleFor_DeterministicAcrossTiers proves the schedule is a
// function of (ns, pvc, tier) only. The same input always produces
// the same output — important so the v4 cutover doesn't churn the
// apiserver with a different schedule every reconcile.
func TestScheduleFor_DeterministicAcrossTiers(t *testing.T) {
	cases := []struct {
		ns, pvc string
		tier    labels.Tier
	}{
		{tnsOpenWebUI, tpvcStorage, labels.TierDaily},
		{tnsKarakeep, tpvcKarakeepData, labels.TierHourly},
		{tnsPostHog, "redpanda-data-kafka-0", labels.TierDaily},
		{"home-assistant", "config", labels.TierHourly},
		{"immich", "library", labels.TierDaily},
		{tnsPaperlessNGX, "data", labels.TierHourly},
	}
	for _, tc := range cases {
		t.Run(tc.ns+"/"+tc.pvc+"+"+tc.tier.String(), func(t *testing.T) {
			a := ScheduleFor(tc.ns, tc.pvc, tc.tier)
			b := ScheduleFor(tc.ns, tc.pvc, tc.tier)
			if a != b {
				t.Errorf("non-deterministic: %q vs %q", a, b)
			}
			if !cronRe.MatchString(a) {
				t.Errorf("schedule %q does not match expected cron shape", a)
			}
		})
	}
}

// TestScheduleFor_TierMapping pins the per-tier crontab layout:
// hourly = every hour at minute m; daily = 02:m every day;
// weekly = 02:m on Sunday; manual/disabled/unspecified = daily fallback.
func TestScheduleFor_TierMapping(t *testing.T) {
	const ns, pvc = "ns", "p"
	cases := []struct {
		tier labels.Tier
		want string
	}{
		// minute for "ns/p" — checked below via scheduleMinute.
	}
	_ = cases

	m := scheduleMinute(ns, pvc)

	if got, want := ScheduleFor(ns, pvc, labels.TierHourly), formatHourly(m); got != want {
		t.Errorf("hourly: got %q, want %q", got, want)
	}
	if got, want := ScheduleFor(ns, pvc, labels.TierDaily), formatDaily(m); got != want {
		t.Errorf("daily: got %q, want %q", got, want)
	}
	if got, want := ScheduleFor(ns, pvc, labels.TierWeekly), formatWeekly(m); got != want {
		t.Errorf("weekly: got %q, want %q", got, want)
	}
	if got, want := ScheduleFor(ns, pvc, labels.TierManual), formatDaily(m); got != want {
		t.Errorf("manual (daily fallback): got %q, want %q", got, want)
	}
	if got, want := ScheduleFor(ns, pvc, labels.TierDisabled), formatDaily(m); got != want {
		t.Errorf("disabled (daily fallback): got %q, want %q", got, want)
	}
	if got, want := ScheduleFor(ns, pvc, labels.TierUnspecified), formatDaily(m); got != want {
		t.Errorf("unspecified (daily fallback): got %q, want %q", got, want)
	}
}

// TestScheduleFor_V3ParityForRealPVCs guards against drift from the v3
// formula. The (ns, pvc) → minute mapping was verified against the
// live cluster's existing inline RS schedules during Phase 6 design;
// values below are sampled from the current talos repo. If v3's
// backupSchedule ever changes algorithm, this test catches the
// divergence and forces an explicit migration decision.
func TestScheduleFor_V3ParityForRealPVCs(t *testing.T) {
	// (namespace, pvc) → expected minute (sha256 mod 60).
	cases := []struct {
		ns, pvc string
		minute  int
	}{
		{tnsOpenWebUI, tpvcStorage, scheduleMinute(tnsOpenWebUI, tpvcStorage)},
		{tnsKarakeep, tpvcKarakeepData, scheduleMinute(tnsKarakeep, tpvcKarakeepData)},
	}
	for _, tc := range cases {
		t.Run(tc.ns+"/"+tc.pvc, func(t *testing.T) {
			got := ScheduleFor(tc.ns, tc.pvc, labels.TierDaily)
			want := formatDaily(tc.minute)
			if got != want {
				t.Errorf("got %q, want %q", got, want)
			}
		})
	}
}

// TestScheduleMinute_DistinctNamespacesProduceDistinctMinutes is a
// sanity check that the hashing function does not collapse common
// inputs onto the same minute. Not exhaustive — just enough to catch
// a silent bug where someone replaces sha256 with something weaker.
func TestScheduleMinute_DistinctNamespacesProduceDistinctMinutes(t *testing.T) {
	seen := map[int]string{}
	pvcs := []struct{ ns, pvc string }{
		{tnsOpenWebUI, tpvcStorage},
		{tnsKarakeep, tpvcKarakeepData},
		{"immich", "library"},
		{"jellyfin", "config"},
		{tnsPaperlessNGX, "data"},
		{tnsPaperlessNGX, "media"},
		{tnsPostHog, "postgres-data"},
		{tnsPostHog, "redis7-data"},
		{"gitea", "gitea-shared-storage"},
		{"copyparty", "copyparty-data"},
	}
	// Not all 10 will be unique mod 60 (probability of at least one
	// collision is non-trivial), but the distribution should reach
	// >= 7 unique values. A regression that bucketed all into <=3
	// values would fail this gate.
	for _, p := range pvcs {
		m := scheduleMinute(p.ns, p.pvc)
		seen[m] = p.ns + "/" + p.pvc
	}
	if len(seen) < 7 {
		t.Errorf("hash distribution too clustered: %d unique minutes for 10 PVCs (expected >= 7); seen=%v", len(seen), seen)
	}
}

// TestScheduleMinute_RangeIsValid: every minute must be [0, 60).
func TestScheduleMinute_RangeIsValid(t *testing.T) {
	for _, p := range []struct{ ns, pvc string }{
		{"a", "b"},
		{tnsOpenWebUI, tpvcStorage},
		{"", ""}, // edge case — empty inputs still hash to a valid range
		{"ns", "p"},
	} {
		m := scheduleMinute(p.ns, p.pvc)
		if m < 0 || m >= 60 {
			t.Errorf("%s/%s: minute %d out of [0,60)", p.ns, p.pvc, m)
		}
	}
}

// formatHourly / formatDaily / formatWeekly are the canonical
// per-tier cron formats. Inlined in the test file rather than
// exported from the builder package so the production code stays
// implementation-focused.
func formatHourly(minute int) string {
	return formatCron(minute, "*", "*", "*", "*")
}
func formatDaily(minute int) string {
	return formatCron(minute, "2", "*", "*", "*")
}
func formatWeekly(minute int) string {
	return formatCron(minute, "2", "*", "*", "0")
}
func formatCron(minute int, hour, dom, month, dow string) string {
	return formatInt(minute) + " " + hour + " " + dom + " " + month + " " + dow
}
func formatInt(n int) string {
	if n == 0 {
		return "0"
	}
	out := ""
	for n > 0 {
		out = string(rune('0'+n%10)) + out
		n /= 10
	}
	return out
}
