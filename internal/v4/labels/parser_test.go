package labels

import (
	"strings"
	"testing"
	"time"
)

// Test-scope constants. The bool label values (labelTrue, labelFalse)
// live in parser.go because the strict-parse path references them
// too — keeping a single source of truth.
const (
	testReasonNAS = "NAS-backed, non-snapshottable"
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
			labels:   map[string]string{LabelEnabled: labelTrue, LabelTier: tierStrHourly},
			wantOrig: OriginNew,
			wantEnab: true,
			wantTier: TierHourly,
		},
		{
			name:     "legacy only",
			labels:   map[string]string{LegacyLabelBackup: tierStrDaily},
			wantOrig: OriginLegacyOnly,
			wantTier: TierDaily,
		},
		{
			name:     "both — admission honors new, migration sees both",
			labels:   map[string]string{LabelEnabled: labelTrue, LabelTier: tierStrWeekly, LegacyLabelBackup: tierStrDaily},
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
			labels:   map[string]string{LabelEnabled: "  true  ", LabelTier: tierStrManual},
			wantOrig: OriginNew,
			wantEnab: true,
			wantTier: TierManual,
		},
		{
			name:     "invalid tier surfaces as error, tier stays unspecified",
			labels:   map[string]string{LabelEnabled: labelTrue, LabelTier: "every-5-min"},
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
			labels:   map[string]string{LabelEnabled: labelTrue, LabelTier: tierStrDisabled},
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
			labels:     map[string]string{LegacyLabelBackupExempt: labelTrue},
			anns:       map[string]string{LegacyAnnotationBackupExemptReasonFQ: testReasonNAS},
			wantKind:   ExemptValid,
			wantReason: testReasonNAS,
		},
		{
			name:     "INVALID: exempt=true with no reason annotation at all",
			labels:   map[string]string{LegacyLabelBackupExempt: labelTrue},
			wantKind: ExemptMissingReason,
			wantErrs: 1,
		},
		{
			name:   "INVALID: exempt=true with empty FQ reason (whitespace only)",
			labels: map[string]string{LegacyLabelBackupExempt: labelTrue},
			anns: map[string]string{
				LegacyAnnotationBackupExemptReasonFQ: "   ",
			},
			wantKind: ExemptMissingReason,
			wantErrs: 1,
		},
		{
			name:   "INVALID: exempt=true with ONLY the bare reason key (silent-fail landmine)",
			labels: map[string]string{LegacyLabelBackupExempt: labelTrue},
			anns: map[string]string{
				"backup-exempt-reason": "would silently fail without FQ key",
			},
			wantKind: ExemptMissingReason,
			wantErrs: 1,
		},
		{
			name:     "exempt=false treated as none",
			labels:   map[string]string{LegacyLabelBackupExempt: labelFalse},
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
			anns:       map[string]string{AnnotationSkipRestore: labelTrue, AnnotationSkipRestoreReason: "DR drill 2026-06-01"},
			wantSkip:   true,
			wantReason: "DR drill 2026-06-01",
		},
		{
			name:     "skip=true without reason — parser does NOT reject; engine will",
			anns:     map[string]string{AnnotationSkipRestore: labelTrue},
			wantSkip: true,
		},
		{
			name:     "skip=false",
			anns:     map[string]string{AnnotationSkipRestore: labelFalse},
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
		{name: "true lowercase", in: map[string]string{NamespacePrivilegedMoversLabel: labelTrue}, want: true},
		{name: "True case", in: map[string]string{NamespacePrivilegedMoversLabel: "True"}, want: true},
		{name: "TRUE upper", in: map[string]string{NamespacePrivilegedMoversLabel: "TRUE"}, want: true},
		{name: "false", in: map[string]string{NamespacePrivilegedMoversLabel: labelFalse}, want: false},
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

// TestParse_ManageVolSync covers the Patch 6.1 contract: the new
// `pvc-plumber.io/manage-volsync` label is parsed strictly, never
// inferred from legacy / enabled labels, and is required (with Enabled)
// for any write to happen. Planner enforcement of those interactions
// lives in later patches; this test only proves the parser shape.
func TestParse_ManageVolSync(t *testing.T) {
	cases := []struct {
		name              string
		labels            map[string]string
		anns              map[string]string
		wantEnabled       bool
		wantManageVolSync bool
		wantOrigin        Origin
		wantExemptKind    ExemptKind
		wantErrs          int
	}{
		// --- Bare parsing of the new label ---
		{
			name:       "missing manage-volsync → false, no error",
			wantOrigin: OriginNone,
		},
		{
			name:              "manage-volsync=true → true, no error",
			labels:            map[string]string{LabelManageVolSync: labelTrue},
			wantManageVolSync: true,
			wantOrigin:        OriginNone, // not an opt-in by itself
		},
		{
			name:       "manage-volsync=false explicit → false, no error",
			labels:     map[string]string{LabelManageVolSync: labelFalse},
			wantOrigin: OriginNone,
		},
		{
			name:       "manage-volsync=garbage → error, field stays false",
			labels:     map[string]string{LabelManageVolSync: "yes"},
			wantOrigin: OriginNone,
			wantErrs:   1,
		},
		{
			name:       "manage-volsync=empty string → error, field stays false (typo detection)",
			labels:     map[string]string{LabelManageVolSync: ""},
			wantOrigin: OriginNone,
			wantErrs:   1,
		},
		{
			name:              "manage-volsync=True (case-insensitive)",
			labels:            map[string]string{LabelManageVolSync: "True"},
			wantManageVolSync: true,
			wantOrigin:        OriginNone,
		},
		{
			name:              "manage-volsync=TRUE (uppercase)",
			labels:            map[string]string{LabelManageVolSync: "TRUE"},
			wantManageVolSync: true,
			wantOrigin:        OriginNone,
		},
		{
			name:              "manage-volsync=  true  (whitespace tolerated)",
			labels:            map[string]string{LabelManageVolSync: "  true  "},
			wantManageVolSync: true,
			wantOrigin:        OriginNone,
		},
		{
			name:       "manage-volsync=False (case-insensitive false)",
			labels:     map[string]string{LabelManageVolSync: "False"},
			wantOrigin: OriginNone,
		},

		// --- Interaction with other opt-in labels (write-gate INDEPENDENCE) ---
		{
			name:              "enabled=true WITHOUT manage-volsync → opt-in but not write-eligible",
			labels:            map[string]string{LabelEnabled: labelTrue},
			wantEnabled:       true,
			wantManageVolSync: false,
			wantOrigin:        OriginNew,
		},
		{
			name:              "legacy backup label WITHOUT manage-volsync → audit opt-in but write-gate stays off",
			labels:            map[string]string{LegacyLabelBackup: tierStrHourly},
			wantManageVolSync: false,
			wantOrigin:        OriginLegacyOnly,
		},
		{
			name:              "legacy backup label PLUS manage-volsync=true → parser allows it; planner must still NOT write (legacy-only is not write-eligible)",
			labels:            map[string]string{LegacyLabelBackup: tierStrDaily, LabelManageVolSync: labelTrue},
			wantManageVolSync: true,
			wantOrigin:        OriginLegacyOnly, // legacy alone does not flip to OriginNew
		},
		{
			name:              "enabled=true + manage-volsync=true → fully write-eligible at the parser level",
			labels:            map[string]string{LabelEnabled: labelTrue, LabelManageVolSync: labelTrue},
			wantEnabled:       true,
			wantManageVolSync: true,
			wantOrigin:        OriginNew,
		},
		{
			name:              "enabled=true + manage-volsync=false → opt-in, writes explicitly disabled",
			labels:            map[string]string{LabelEnabled: labelTrue, LabelManageVolSync: labelFalse},
			wantEnabled:       true,
			wantManageVolSync: false,
			wantOrigin:        OriginNew,
		},

		// --- Backup-exempt parses cleanly alongside manage-volsync; planner enforces precedence ---
		{
			name:   "backup-exempt + manage-volsync=true parses cleanly (planner must skip writes)",
			labels: map[string]string{LegacyLabelBackupExempt: labelTrue, LabelManageVolSync: labelTrue},
			anns: map[string]string{
				LegacyAnnotationBackupExemptReasonFQ: testReasonNAS,
			},
			wantManageVolSync: true,
			wantOrigin:        OriginNone,
			wantExemptKind:    ExemptValid,
		},
		{
			name:              "all three: enabled + manage-volsync + backup-exempt parses cleanly (planner must skip writes)",
			labels:            map[string]string{LabelEnabled: labelTrue, LabelManageVolSync: labelTrue, LegacyLabelBackupExempt: labelTrue},
			anns:              map[string]string{LegacyAnnotationBackupExemptReasonFQ: "NAS"},
			wantEnabled:       true,
			wantManageVolSync: true,
			wantOrigin:        OriginNew,
			wantExemptKind:    ExemptValid,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := Parse(tc.labels, tc.anns)
			if s.Enabled != tc.wantEnabled {
				t.Errorf("Enabled: got %v, want %v", s.Enabled, tc.wantEnabled)
			}
			if s.ManageVolSync != tc.wantManageVolSync {
				t.Errorf("ManageVolSync: got %v, want %v", s.ManageVolSync, tc.wantManageVolSync)
			}
			if s.Origin != tc.wantOrigin {
				t.Errorf("Origin: got %v, want %v", s.Origin, tc.wantOrigin)
			}
			if s.ExemptKind != tc.wantExemptKind {
				t.Errorf("ExemptKind: got %v, want %v", s.ExemptKind, tc.wantExemptKind)
			}
			if len(s.Errors) != tc.wantErrs {
				t.Errorf("Errors: got %d (%v), want %d", len(s.Errors), s.Errors, tc.wantErrs)
			}
		})
	}
}

// TestParse_ManageVolSync_ZeroValueDefault is a paranoia guard: the
// Spec zero value must have ManageVolSync=false. A future struct
// reorder or default-flip is the kind of single-character mistake
// that would silently re-enable writes on every PVC. Catch it here.
func TestParse_ManageVolSync_ZeroValueDefault(t *testing.T) {
	var s Spec
	if s.ManageVolSync {
		t.Fatal("Spec{} zero value has ManageVolSync=true; write-gate must default to false")
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
