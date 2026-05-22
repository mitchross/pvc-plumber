package sourcegate

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestEvaluate(t *testing.T) {
	t0 := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	twoH := 2 * time.Hour

	cases := []struct {
		name    string
		in      Inputs
		wantSt  State
		wantSub string // substring expected in reason; "" skips check
	}{
		{
			name: "not enabled → Disabled",
			in: Inputs{
				Enabled:  false,
				PVCPhase: PhaseBound,
				Now:      t0,
			},
			wantSt:  Disabled,
			wantSub: "enabled",
		},
		{
			name: "enabled but tier=disabled → Disabled",
			in: Inputs{
				Enabled:      true,
				TierDisabled: true,
				PVCPhase:     PhaseBound,
				Now:          t0,
			},
			wantSt:  Disabled,
			wantSub: "tier=disabled",
		},
		{
			name: "Pending PVC → WaitingForPVCBound",
			in: Inputs{
				Enabled:  true,
				PVCPhase: PhasePending,
				Now:      t0,
			},
			wantSt:  WaitingForPVCBound,
			wantSub: "Pending",
		},
		{
			name: "Lost PVC → WaitingForPVCBound",
			in: Inputs{
				Enabled:  true,
				PVCPhase: PhaseLost,
				Now:      t0,
			},
			wantSt:  WaitingForPVCBound,
			wantSub: "Lost",
		},
		{
			name: "Bound + dataSourceRef + restore incomplete → WaitingForRestore",
			in: Inputs{
				Enabled:          true,
				PVCPhase:         PhaseBound,
				HasDataSourceRef: true,
				RestoreComplete:  false,
				BoundAt:          t0.Add(-3 * time.Hour),
				MinBackupAge:     twoH,
				Now:              t0,
			},
			wantSt:  WaitingForRestore,
			wantSub: "restore has not completed",
		},
		{
			name: "Bound + restore complete + minAge not elapsed → WaitingForMinAge",
			in: Inputs{
				Enabled:          true,
				PVCPhase:         PhaseBound,
				HasDataSourceRef: true,
				RestoreComplete:  true,
				BoundAt:          t0.Add(-30 * time.Minute),
				MinBackupAge:     twoH,
				Now:              t0,
			},
			wantSt:  WaitingForMinAge,
			wantSub: "more before first backup",
		},
		{
			name: "Bound + no dataSourceRef + just-bound → WaitingForMinAge",
			in: Inputs{
				Enabled:      true,
				PVCPhase:     PhaseBound,
				BoundAt:      t0.Add(-5 * time.Minute),
				MinBackupAge: twoH,
				Now:          t0,
			},
			wantSt:  WaitingForMinAge,
			wantSub: "before first backup",
		},
		{
			name: "Bound + no dataSourceRef + minAge elapsed → Ready",
			in: Inputs{
				Enabled:      true,
				PVCPhase:     PhaseBound,
				BoundAt:      t0.Add(-3 * time.Hour),
				MinBackupAge: twoH,
				Now:          t0,
			},
			wantSt:  Ready,
			wantSub: "cleared",
		},
		{
			name: "Bound + restore done + minAge elapsed → Ready",
			in: Inputs{
				Enabled:          true,
				PVCPhase:         PhaseBound,
				HasDataSourceRef: true,
				RestoreComplete:  true,
				BoundAt:          t0.Add(-3 * time.Hour),
				MinBackupAge:     twoH,
				Now:              t0,
			},
			wantSt: Ready,
		},
		{
			name: "MinBackupAge=0 + Bound → Ready immediately",
			in: Inputs{
				Enabled:      true,
				PVCPhase:     PhaseBound,
				BoundAt:      t0.Add(-1 * time.Second),
				MinBackupAge: 0,
				Now:          t0,
			},
			wantSt: Ready,
		},
		{
			name: "Bound but BoundAt missing → defensive WaitingForMinAge",
			in: Inputs{
				Enabled:      true,
				PVCPhase:     PhaseBound,
				BoundAt:      time.Time{},
				MinBackupAge: twoH,
				Now:          t0,
			},
			wantSt:  WaitingForMinAge,
			wantSub: "no BoundAt timestamp",
		},
		{
			name: "EvaluationError → Error short-circuits before other checks",
			in: Inputs{
				Enabled:         true,
				PVCPhase:        PhaseBound,
				EvaluationError: errors.New("API server timeout"),
				Now:             t0,
			},
			wantSt:  Error,
			wantSub: "API server timeout",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, reason := Evaluate(tc.in)
			if st != tc.wantSt {
				t.Errorf("state: got %v, want %v (reason: %s)", st, tc.wantSt, reason)
			}
			if tc.wantSub != "" && !strings.Contains(reason, tc.wantSub) {
				t.Errorf("reason missing substring %q: got %q", tc.wantSub, reason)
			}
		})
	}
}

func TestStatePredicates(t *testing.T) {
	cases := []struct {
		s            State
		allowsCreate bool
		tearsDown    bool
		terminal     bool
	}{
		{Ready, true, false, false},
		{Disabled, false, true, true},
		{Error, false, false, true},
		{WaitingForPVCBound, false, false, false},
		{WaitingForRestore, false, false, false},
		{WaitingForMinAge, false, false, false},
		{Unknown, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.s.String(), func(t *testing.T) {
			if got := tc.s.AllowsRSCreate(); got != tc.allowsCreate {
				t.Errorf("AllowsRSCreate: got %v, want %v", got, tc.allowsCreate)
			}
			if got := tc.s.TearsDownRS(); got != tc.tearsDown {
				t.Errorf("TearsDownRS: got %v, want %v", got, tc.tearsDown)
			}
			if got := tc.s.IsTerminal(); got != tc.terminal {
				t.Errorf("IsTerminal: got %v, want %v", got, tc.terminal)
			}
		})
	}
}

func TestStateString(t *testing.T) {
	// Confirm the exact strings that get emitted as event reasons,
	// metric labels, and the PVC annotation value.
	want := map[State]string{
		Unknown:            "unknown",
		WaitingForPVCBound: "waiting_for_pvc_bound",
		WaitingForRestore:  "waiting_for_restore",
		WaitingForMinAge:   "waiting_for_min_age",
		Ready:              "ready",
		Disabled:           "disabled",
		Error:              "error",
	}
	for st, w := range want {
		if got := st.String(); got != w {
			t.Errorf("State(%d).String() = %q, want %q", st, got, w)
		}
	}
}
