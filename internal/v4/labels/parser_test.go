package labels

import (
	"strings"
	"testing"
	"time"
)

func TestParse_OptInSignals(t *testing.T) {
	cases := []struct {
		name     string
		labels   map[string]string
		anns     map[string]string
		wantOrig Origin
		wantEnab bool
		wantTier Tier
		wantErrs int
	}{
		{
			name:     "no opt-in",
			wantOrig: OriginNone,
		},
		{
			name:     "new label only",
			labels:   map[string]string{LabelEnabled: "true", LabelTier: "hourly"},
			wantOrig: OriginNew,
			wantEnab: true,
			wantTier: TierHourly,
		},
		{
			name:     "legacy only",
			labels:   map[string]string{LegacyLabelBackup: "daily"},
			wantOrig: OriginLegacyOnly,
			wantTier: TierDaily,
		},
		{
			name:     "both — admission honors new, migration sees both",
			labels:   map[string]string{LabelEnabled: "true", LabelTier: "weekly", LegacyLabelBackup: "daily"},
			wantOrig: OriginBoth,
			wantEnab: true,
			wantTier: TierWeekly, // new takes precedence
		},
		{
			name:     "enabled=false (case insensitive) is ignored",
			labels:   map[string]string{LabelEnabled: "False"},
			wantOrig: OriginNone,
		},
		{
			name:     "enabled with whitespace",
			labels:   map[string]string{LabelEnabled: "  true  ", LabelTier: "manual"},
			wantOrig: OriginNew,
			wantEnab: true,
			wantTier: TierManual,
		},
		{
			name:     "invalid tier surfaces as error, tier stays unspecified",
			labels:   map[string]string{LabelEnabled: "true", LabelTier: "every-5-min"},
			wantOrig: OriginNew,
			wantEnab: true,
			wantTier: TierUnspecified,
			wantErrs: 1,
		},
		{
			name:     "invalid legacy tier surfaces as error and does not classify origin",
			labels:   map[string]string{LegacyLabelBackup: "monthly"},
			wantOrig: OriginNone, // because LegacyTier is unspecified after parse error
			wantErrs: 1,
		},
		{
			name:     "tier=disabled is a valid declaration (RS suppressed)",
			labels:   map[string]string{LabelEnabled: "true", LabelTier: "disabled"},
			wantOrig: OriginNew,
			wantEnab: true,
			wantTier: TierDisabled,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := Parse(tc.labels, tc.anns)
			if s.Origin != tc.wantOrig {
				t.Errorf("Origin: got %v, want %v", s.Origin, tc.wantOrig)
			}
			if s.Enabled != tc.wantEnab {
				t.Errorf("Enabled: got %v, want %v", s.Enabled, tc.wantEnab)
			}
			if s.Tier != tc.wantTier {
				t.Errorf("Tier: got %v, want %v", s.Tier, tc.wantTier)
			}
			if len(s.Errors) != tc.wantErrs {
				t.Errorf("Errors: got %d (%v), want %d", len(s.Errors), s.Errors, tc.wantErrs)
			}
		})
	}
}

func TestParse_BackupExemptContract(t *testing.T) {
	cases := []struct {
		name       string
		labels     map[string]string
		anns       map[string]string
		wantKind   ExemptKind
		wantReason string
		wantErrs   int
	}{
		{
			name:     "no exempt label",
			wantKind: ExemptNone,
		},
		{
			name:       "valid: exempt=true + FQ reason",
			labels:     map[string]string{LegacyLabelBackupExempt: "true"},
			anns:       map[string]string{LegacyAnnotationBackupExemptReasonFQ: "NAS-backed, non-snapshottable"},
			wantKind:   ExemptValid,
			wantReason: "NAS-backed, non-snapshottable",
		},
		{
			name:     "INVALID: exempt=true with no reason annotation at all",
			labels:   map[string]string{LegacyLabelBackupExempt: "true"},
			wantKind: ExemptMissingReason,
			wantErrs: 1,
		},
		{
			name:   "INVALID: exempt=true with empty FQ reason (whitespace only)",
			labels: map[string]string{LegacyLabelBackupExempt: "true"},
			anns: map[string]string{
				LegacyAnnotationBackupExemptReasonFQ: "   ",
			},
			wantKind: ExemptMissingReason,
			wantErrs: 1,
		},
		{
			name:   "INVALID: exempt=true with ONLY the bare reason key (silent-fail landmine)",
			labels: map[string]string{LegacyLabelBackupExempt: "true"},
			anns: map[string]string{
				"backup-exempt-reason": "would silently fail without FQ key",
			},
			wantKind: ExemptMissingReason,
			wantErrs: 1,
		},
		{
			name:     "exempt=false treated as none",
			labels:   map[string]string{LegacyLabelBackupExempt: "false"},
			wantKind: ExemptNone,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := Parse(tc.labels, tc.anns)
			if s.ExemptKind != tc.wantKind {
				t.Errorf("ExemptKind: got %v, want %v", s.ExemptKind, tc.wantKind)
			}
			if s.ExemptReason != tc.wantReason {
				t.Errorf("ExemptReason: got %q, want %q", s.ExemptReason, tc.wantReason)
			}
			if len(s.Errors) != tc.wantErrs {
				t.Errorf("Errors: got %d (%v), want %d", len(s.Errors), s.Errors, tc.wantErrs)
			}
		})
	}
}

func TestParse_UIDGIDValidation(t *testing.T) {
	cases := []struct {
		name     string
		anns     map[string]string
		wantUID  *int64
		wantGID  *int64
		wantFSG  *int64
		wantErrs int
	}{
		{
			name: "valid 568/568/568",
			anns: map[string]string{
				AnnotationUID:     "568",
				AnnotationGID:     "568",
				AnnotationFSGroup: "568",
			},
			wantUID: int64Ptr(568),
			wantGID: int64Ptr(568),
			wantFSG: int64Ptr(568),
		},
		{
			name:     "non-numeric",
			anns:     map[string]string{AnnotationUID: "abc"},
			wantErrs: 1,
		},
		{
			name:     "negative",
			anns:     map[string]string{AnnotationGID: "-1"},
			wantErrs: 1,
		},
		{
			name:     "out of range high",
			anns:     map[string]string{AnnotationFSGroup: "999999999999"},
			wantErrs: 1,
		},
		{
			name:     "empty string explicitly set is an error (typo detection)",
			anns:     map[string]string{AnnotationUID: ""},
			wantErrs: 1,
		},
		{
			name:    "zero is allowed (root — rare but valid)",
			anns:    map[string]string{AnnotationUID: "0"},
			wantUID: int64Ptr(0),
		},
		{
			name:    "whitespace tolerated",
			anns:    map[string]string{AnnotationUID: "  100  "},
			wantUID: int64Ptr(100),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := Parse(nil, tc.anns)
			if !int64PtrEq(s.UID, tc.wantUID) {
				t.Errorf("UID: got %v, want %v", s.UID, tc.wantUID)
			}
			if !int64PtrEq(s.GID, tc.wantGID) {
				t.Errorf("GID: got %v, want %v", s.GID, tc.wantGID)
			}
			if !int64PtrEq(s.FSGroup, tc.wantFSG) {
				t.Errorf("FSGroup: got %v, want %v", s.FSGroup, tc.wantFSG)
			}
			if len(s.Errors) != tc.wantErrs {
				t.Errorf("Errors: got %d (%v), want %d", len(s.Errors), s.Errors, tc.wantErrs)
			}
		})
	}
}

func TestParse_SkipRestoreContract(t *testing.T) {
	cases := []struct {
		name       string
		anns       map[string]string
		wantSkip   bool
		wantReason string
	}{
		{name: "neither", wantSkip: false, wantReason: ""},
		{
			name:       "skip=true with reason",
			anns:       map[string]string{AnnotationSkipRestore: "true", AnnotationSkipRestoreReason: "DR drill 2026-06-01"},
			wantSkip:   true,
			wantReason: "DR drill 2026-06-01",
		},
		{
			name:     "skip=true without reason — parser does NOT reject; engine will",
			anns:     map[string]string{AnnotationSkipRestore: "true"},
			wantSkip: true,
		},
		{
			name:     "skip=false",
			anns:     map[string]string{AnnotationSkipRestore: "false"},
			wantSkip: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := Parse(nil, tc.anns)
			if s.SkipRestore != tc.wantSkip {
				t.Errorf("SkipRestore: got %v, want %v", s.SkipRestore, tc.wantSkip)
			}
			if s.SkipRestoreReason != tc.wantReason {
				t.Errorf("SkipRestoreReason: got %q, want %q", s.SkipRestoreReason, tc.wantReason)
			}
		})
	}
}

func TestParse_MinBackupAge(t *testing.T) {
	cases := []struct {
		name     string
		anns     map[string]string
		wantDur  time.Duration
		wantSet  bool
		wantErrs int
	}{
		{name: "unset", wantSet: false},
		{
			name:    "2h",
			anns:    map[string]string{AnnotationMinBackupAge: "2h"},
			wantDur: 2 * time.Hour,
			wantSet: true,
		},
		{
			name:    "30m",
			anns:    map[string]string{AnnotationMinBackupAge: "30m"},
			wantDur: 30 * time.Minute,
			wantSet: true,
		},
		{
			name:     "garbage",
			anns:     map[string]string{AnnotationMinBackupAge: "two hours"},
			wantErrs: 1,
		},
		{
			name:     "negative",
			anns:     map[string]string{AnnotationMinBackupAge: "-1h"},
			wantErrs: 1,
		},
		{
			name:    "empty string is unset (not an error)",
			anns:    map[string]string{AnnotationMinBackupAge: ""},
			wantSet: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := Parse(nil, tc.anns)
			if s.MinBackupAge != tc.wantDur {
				t.Errorf("MinBackupAge: got %v, want %v", s.MinBackupAge, tc.wantDur)
			}
			if s.MinBackupAgeSet != tc.wantSet {
				t.Errorf("MinBackupAgeSet: got %v, want %v", s.MinBackupAgeSet, tc.wantSet)
			}
			if len(s.Errors) != tc.wantErrs {
				t.Errorf("Errors: got %d (%v), want %d", len(s.Errors), s.Errors, tc.wantErrs)
			}
		})
	}
}

func TestParse_FreeFormAnnotations(t *testing.T) {
	anns := map[string]string{
		AnnotationMode:           "  strict ",
		AnnotationRestoreMode:    "force",
		AnnotationBackupIdentity: "immich-library",
		AnnotationStorageClass:   "longhorn",
		AnnotationSnapshotClass:  "longhorn-snapclass",
		AnnotationCacheCapacity:  "4Gi",
	}
	s := Parse(nil, anns)
	if s.Mode != "strict" {
		t.Errorf("Mode (whitespace trim): got %q, want %q", s.Mode, "strict")
	}
	if s.RestoreMode != "force" {
		t.Errorf("RestoreMode: got %q, want %q", s.RestoreMode, "force")
	}
	if s.BackupIdentity != "immich-library" {
		t.Errorf("BackupIdentity: got %q, want %q", s.BackupIdentity, "immich-library")
	}
	if s.StorageClass != "longhorn" {
		t.Errorf("StorageClass: got %q", s.StorageClass)
	}
	if s.SnapshotClass != "longhorn-snapclass" {
		t.Errorf("SnapshotClass: got %q", s.SnapshotClass)
	}
	if s.CacheCapacity != "4Gi" {
		t.Errorf("CacheCapacity: got %q", s.CacheCapacity)
	}
}

func TestNamespaceHasPrivilegedMovers(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]string
		want bool
	}{
		{name: "nil map", want: false},
		{name: "absent", in: map[string]string{}, want: false},
		{name: "true lowercase", in: map[string]string{NamespacePrivilegedMoversLabel: "true"}, want: true},
		{name: "True case", in: map[string]string{NamespacePrivilegedMoversLabel: "True"}, want: true},
		{name: "TRUE upper", in: map[string]string{NamespacePrivilegedMoversLabel: "TRUE"}, want: true},
		{name: "false", in: map[string]string{NamespacePrivilegedMoversLabel: "false"}, want: false},
		{name: "garbage", in: map[string]string{NamespacePrivilegedMoversLabel: "yes"}, want: false},
		{name: "whitespace true", in: map[string]string{NamespacePrivilegedMoversLabel: "  true "}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := NamespaceHasPrivilegedMovers(tc.in); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseTier_Errors(t *testing.T) {
	_, err := parseTier("monthly")
	if err == nil {
		t.Fatal("expected error for 'monthly', got nil")
	}
	if !strings.Contains(err.Error(), "invalid tier") {
		t.Errorf("error message: got %q, want substring 'invalid tier'", err.Error())
	}
}

func int64Ptr(v int64) *int64 { return &v }
func int64PtrEq(a, b *int64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
