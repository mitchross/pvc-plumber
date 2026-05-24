package controller

import (
	"testing"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
)

func TestClassifyLabelSource(t *testing.T) {
	cases := []struct {
		name        string
		spec        labels.Spec
		wantSrc     LabelSource
		wantOptedIn bool
	}{
		{
			name:        "OriginNone → LabelSourceNone (not opted in)",
			spec:        labels.Spec{Origin: labels.OriginNone},
			wantSrc:     LabelSourceNone,
			wantOptedIn: false,
		},
		{
			name:        "OriginNew (pvc-plumber.io/enabled=true) → LabelSourceV4 (opted in)",
			spec:        labels.Spec{Origin: labels.OriginNew, Enabled: true},
			wantSrc:     LabelSourceV4,
			wantOptedIn: true,
		},
		{
			name:        "OriginLegacyOnly (backup: hourly only) → LabelSourceLegacy (opted in for audit)",
			spec:        labels.Spec{Origin: labels.OriginLegacyOnly, LegacyTier: labels.TierHourly},
			wantSrc:     LabelSourceLegacy,
			wantOptedIn: true,
		},
		{
			name:        "OriginBoth (v4 + legacy) → LabelSourceBoth (opted in)",
			spec:        labels.Spec{Origin: labels.OriginBoth, Enabled: true, LegacyTier: labels.TierDaily},
			wantSrc:     LabelSourceBoth,
			wantOptedIn: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyLabelSource(tc.spec)
			if got != tc.wantSrc {
				t.Errorf("ClassifyLabelSource: got %q, want %q", got, tc.wantSrc)
			}
			if optedIn := IsAuditOptedIn(got); optedIn != tc.wantOptedIn {
				t.Errorf("IsAuditOptedIn(%q): got %v, want %v", got, optedIn, tc.wantOptedIn)
			}
		})
	}
}

// TestClassifyLabelSource_DefensiveUnknownOrigin: any unrecognized Origin
// value (e.g., if labels package adds new constants) defaults to None.
func TestClassifyLabelSource_DefensiveUnknownOrigin(t *testing.T) {
	spec := labels.Spec{Origin: labels.Origin(999)}
	if got := ClassifyLabelSource(spec); got != LabelSourceNone {
		t.Errorf("unknown Origin(999): got %q, want %q", got, LabelSourceNone)
	}
}

// TestClassifyLabelSource_LegacyContractItemOne: Phase 5 contract item #1 —
// `backup: hourly|daily` PVCs MUST classify as legacy (not none) so the
// audit reconciler treats them as opted in. This is the regression guard
// for the "PVC with only backup: hourly → skipped-not-opted-in" tweak.
func TestClassifyLabelSource_LegacyContractItemOne(t *testing.T) {
	for _, tier := range []labels.Tier{labels.TierHourly, labels.TierDaily, labels.TierWeekly, labels.TierManual, labels.TierDisabled} {
		t.Run(tier.String(), func(t *testing.T) {
			spec := labels.Spec{Origin: labels.OriginLegacyOnly, LegacyTier: tier}
			src := ClassifyLabelSource(spec)
			if src != LabelSourceLegacy {
				t.Fatalf("legacy tier=%s: got %q, want LabelSourceLegacy", tier, src)
			}
			if !IsAuditOptedIn(src) {
				t.Errorf("legacy tier=%s: IsAuditOptedIn returned false; contract item #1 requires true", tier)
			}
		})
	}
}

// TestIsAuditOptedIn_Direct exercises each value of the LabelSource enum
// independently of ClassifyLabelSource so a future refactor of one doesn't
// silently break the other.
func TestIsAuditOptedIn_Direct(t *testing.T) {
	cases := map[LabelSource]bool{
		LabelSourceNone:   false,
		LabelSourceV4:     true,
		LabelSourceLegacy: true,
		LabelSourceBoth:   true,
	}
	for src, want := range cases {
		if got := IsAuditOptedIn(src); got != want {
			t.Errorf("IsAuditOptedIn(%q): got %v, want %v", src, got, want)
		}
	}
}
