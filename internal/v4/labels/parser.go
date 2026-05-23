package labels

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// String forms of Tier. Private constants so the linter doesn't flag
// repeated literals; tests in this package reference these too.
const (
	tierStrHourly   = "hourly"
	tierStrDaily    = "daily"
	tierStrWeekly   = "weekly"
	tierStrManual   = "manual"
	tierStrDisabled = "disabled"
)

// Tier is the backup cadence declared on a PVC.
type Tier int

const (
	TierUnspecified Tier = iota
	TierHourly
	TierDaily
	TierWeekly
	TierManual
	TierDisabled
)

func (t Tier) String() string {
	switch t {
	case TierHourly:
		return tierStrHourly
	case TierDaily:
		return tierStrDaily
	case TierWeekly:
		return tierStrWeekly
	case TierManual:
		return tierStrManual
	case TierDisabled:
		return tierStrDisabled
	default:
		return "unspecified"
	}
}

// parseTier accepts "hourly", "daily", "weekly", "manual", or "disabled"
// (case-insensitive). Empty string returns TierUnspecified, nil error.
// Any other value returns an error.
func parseTier(raw string) (Tier, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return TierUnspecified, nil
	case tierStrHourly:
		return TierHourly, nil
	case tierStrDaily:
		return TierDaily, nil
	case tierStrWeekly:
		return TierWeekly, nil
	case tierStrManual:
		return TierManual, nil
	case tierStrDisabled:
		return TierDisabled, nil
	default:
		return TierUnspecified, fmt.Errorf("invalid tier %q (expected hourly|daily|weekly|manual|disabled)", raw)
	}
}

// Origin records how a PVC was opted in to the operator, for inventory and
// migration reporting. Admission must only honor New; the others are
// observational.
type Origin int

const (
	// OriginNone means no opt-in signal was found.
	OriginNone Origin = iota
	// OriginNew means LabelEnabled="true". This is the only origin that
	// future admission webhooks should match on.
	OriginNew
	// OriginLegacyOnly means LegacyLabelBackup is set but LabelEnabled is
	// not. The controller may include such PVCs in reports but the webhook
	// objectSelector MUST exclude them.
	OriginLegacyOnly
	// OriginBoth means both LegacyLabelBackup and LabelEnabled are present.
	// Treated as New for admission purposes; reported as Both for migration
	// progress dashboards.
	OriginBoth
)

func (o Origin) String() string {
	switch o {
	case OriginNew:
		return "new"
	case OriginLegacyOnly:
		return "legacy-only"
	case OriginBoth:
		return "both"
	default:
		return "none"
	}
}

// ExemptKind classifies how a PVC is exempted from backup.
type ExemptKind int

const (
	ExemptNone ExemptKind = iota
	// ExemptValid: LegacyLabelBackupExempt="true" AND the FQ reason
	// annotation is set and non-empty.
	ExemptValid
	// ExemptMissingReason: LegacyLabelBackupExempt="true" but the FQ
	// reason annotation is missing or empty. This is a contract violation
	// (the bare reason key was historically a silent-fail DR landmine).
	ExemptMissingReason
)

// Spec is the parsed, strongly-typed view of a PVC's pvc-plumber-relevant
// labels and annotations. Construction is total: every field has a sensible
// zero value, and parse errors are accumulated in Errors so the caller can
// decide whether to deny (strict mode) or warn (audit / permissive).
type Spec struct {
	// Opt-in / classification.
	Origin     Origin
	Enabled    bool   // LabelEnabled value, lowercased+trimmed=="true"
	Tier       Tier   // parsed from LabelTier (new) or LegacyLabelBackup
	LegacyTier Tier   // parsed from LegacyLabelBackup only; for migration reports
	LegacyRaw  string // raw legacy "backup" label value, for events/logs

	// Backup-exempt.
	ExemptKind   ExemptKind
	ExemptReason string

	// Per-PVC overrides.
	Mode           string // raw mode string; resolution to mode.Mode happens in package mode
	RestoreMode    string // raw restore-mode string
	BackupIdentity string // empty = derive default <namespace>/<name>
	StorageClass   string
	SnapshotClass  string
	CacheCapacity  string

	// Mover security context.
	UID     *int64
	GID     *int64
	FSGroup *int64

	// Skip-restore.
	SkipRestore       bool
	SkipRestoreReason string

	// Minimum age before RS may start.
	MinBackupAge    time.Duration
	MinBackupAgeSet bool

	// Accumulated parse errors (one per malformed key). Non-nil slice if any.
	Errors []error
}

// Parse builds a Spec from the metadata.labels and metadata.annotations of a
// PVC. Both maps may be nil. The returned Spec is always usable; any parse
// failures are appended to Spec.Errors and the field falls back to its zero
// value (or the legacy default, where applicable).
//
// Parse is pure: no I/O, no Kubernetes client. Safe for use in webhook hot
// paths and tests.
func Parse(pvcLabels, pvcAnnotations map[string]string) Spec {
	s := Spec{}

	// LabelEnabled.
	if v, ok := pvcLabels[LabelEnabled]; ok {
		s.Enabled = strings.EqualFold(strings.TrimSpace(v), "true")
	}

	// Legacy backup tier.
	if v, ok := pvcLabels[LegacyLabelBackup]; ok {
		s.LegacyRaw = v
		if lt, err := parseTier(v); err != nil {
			s.Errors = append(s.Errors, fmt.Errorf("%s: %w", LegacyLabelBackup, err))
		} else {
			s.LegacyTier = lt
		}
	}

	// Origin classification.
	switch {
	case s.Enabled && s.LegacyTier != TierUnspecified:
		s.Origin = OriginBoth
	case s.Enabled:
		s.Origin = OriginNew
	case s.LegacyTier != TierUnspecified:
		s.Origin = OriginLegacyOnly
	default:
		s.Origin = OriginNone
	}

	// New tier (preferred over legacy).
	if v, ok := pvcLabels[LabelTier]; ok {
		if t, err := parseTier(v); err != nil {
			s.Errors = append(s.Errors, fmt.Errorf("%s: %w", LabelTier, err))
		} else {
			s.Tier = t
		}
	}
	// If new tier is unspecified but legacy is set, fall back to legacy for
	// reconciler/reporting; admission must still gate on s.Enabled only.
	if s.Tier == TierUnspecified {
		s.Tier = s.LegacyTier
	}

	// Backup-exempt.
	if v, ok := pvcLabels[LegacyLabelBackupExempt]; ok && strings.EqualFold(strings.TrimSpace(v), "true") {
		reason := strings.TrimSpace(pvcAnnotations[LegacyAnnotationBackupExemptReasonFQ])
		if reason == "" {
			s.ExemptKind = ExemptMissingReason
			s.Errors = append(s.Errors, fmt.Errorf("%s=true requires annotation %s to be set and non-empty",
				LegacyLabelBackupExempt, LegacyAnnotationBackupExemptReasonFQ))
		} else {
			s.ExemptKind = ExemptValid
			s.ExemptReason = reason
		}
	}

	// Modes (raw strings only; resolution lives in package mode).
	s.Mode = strings.TrimSpace(pvcAnnotations[AnnotationMode])
	s.RestoreMode = strings.TrimSpace(pvcAnnotations[AnnotationRestoreMode])

	// Backup identity override.
	s.BackupIdentity = strings.TrimSpace(pvcAnnotations[AnnotationBackupIdentity])

	// Class + capacity overrides.
	s.StorageClass = strings.TrimSpace(pvcAnnotations[AnnotationStorageClass])
	s.SnapshotClass = strings.TrimSpace(pvcAnnotations[AnnotationSnapshotClass])
	s.CacheCapacity = strings.TrimSpace(pvcAnnotations[AnnotationCacheCapacity])

	// UID / GID / fsGroup.
	if v, ok := pvcAnnotations[AnnotationUID]; ok {
		if uid, err := parseUserID(v); err != nil {
			s.Errors = append(s.Errors, fmt.Errorf("%s: %w", AnnotationUID, err))
		} else {
			s.UID = &uid
		}
	}
	if v, ok := pvcAnnotations[AnnotationGID]; ok {
		if gid, err := parseUserID(v); err != nil {
			s.Errors = append(s.Errors, fmt.Errorf("%s: %w", AnnotationGID, err))
		} else {
			s.GID = &gid
		}
	}
	if v, ok := pvcAnnotations[AnnotationFSGroup]; ok {
		if fsg, err := parseUserID(v); err != nil {
			s.Errors = append(s.Errors, fmt.Errorf("%s: %w", AnnotationFSGroup, err))
		} else {
			s.FSGroup = &fsg
		}
	}

	// Skip-restore.
	if v, ok := pvcAnnotations[AnnotationSkipRestore]; ok {
		s.SkipRestore = strings.EqualFold(strings.TrimSpace(v), "true")
	}
	s.SkipRestoreReason = strings.TrimSpace(pvcAnnotations[AnnotationSkipRestoreReason])

	// MinBackupAge.
	if v, ok := pvcAnnotations[AnnotationMinBackupAge]; ok && v != "" {
		if d, err := time.ParseDuration(v); err != nil {
			s.Errors = append(s.Errors, fmt.Errorf("%s: %w", AnnotationMinBackupAge, err))
		} else if d < 0 {
			s.Errors = append(s.Errors, fmt.Errorf("%s: must be non-negative, got %s", AnnotationMinBackupAge, d))
		} else {
			s.MinBackupAge = d
			s.MinBackupAgeSet = true
		}
	}

	return s
}

// parseUserID parses a UID/GID/fsGroup annotation value. Range is the POSIX
// reasonable range [0, 2^31-1]. Empty values return an error so the caller
// can distinguish "unset" (no annotation key) from "set to empty" (typo).
func parseUserID(raw string) (int64, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return 0, errors.New("value is empty")
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("not an integer: %q", raw)
	}
	if n < 0 || n > (1<<31)-1 {
		return 0, fmt.Errorf("out of range [0, 2^31-1]: %d", n)
	}
	return n, nil
}

// NamespaceHasPrivilegedMovers returns true if the namespace carries the
// volsync.backube/privileged-movers="true" label, which is the gate the
// ClusterExternalSecret in the talos repo uses to fan out the shared kopia
// repo Secret. The operator never sets this label; it reads it.
func NamespaceHasPrivilegedMovers(nsLabels map[string]string) bool {
	return strings.EqualFold(strings.TrimSpace(nsLabels[NamespacePrivilegedMoversLabel]), "true")
}
