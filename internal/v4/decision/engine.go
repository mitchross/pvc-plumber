package decision

import (
	"fmt"
	"strings"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/mode"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// VolSyncAPIGroup is the apiGroup the mutator writes into dataSourceRef.
const VolSyncAPIGroup = "volsync.backube"

// VolSyncRDKind is the kind for the dataSourceRef target.
const VolSyncRDKind = "ReplicationDestination"

// Repeated string literals lifted to private constants so the linter
// doesn't flag occurrences. eventTypeWarning matches corev1.EventTypeWarning;
// it's a local const so the decision package can stay pure (no k8s imports).
const (
	eventTypeWarning              = "Warning"
	eventReasonBackupStateUnknown = "BackupStateUnknown"
)

// Decide is the pure decision function. Given an Input it returns an
// Output describing the admission verdict, whether to mutate, the
// dataSourceRef target (if any), and the reason in machine + human form.
//
// Decide is total: it always returns a well-formed Output. Errors found
// during input resolution (bad labels, bad annotations, mode-parse
// failures) are aggregated into Output.ParseErrors; whether they translate
// to a deny depends on the effective Mode.
//
// Ordering of branches (deliberately documented because subtle):
//
//	(1) ExcludedNamespaces  → always admit, no mutate (defense-in-depth).
//	(2) Not opted in        → always admit, no mutate.
//	(3) Backup-exempt valid → always admit, no mutate.
//	(4) Backup-exempt with missing reason → deny (contract violation).
//	(5) Skip-restore without reason → deny in audit/permissive too;
//	    this is a malformed declaration, not a state-of-backend issue.
//	(6) Parse errors → strict + enforce deny; audit/permissive warn.
//	(7) Restore-mode = never → admit, no mutate.
//	(8) Restore-mode = force → BackupExists ⇒ inject; else deny.
//	(9) Default restore path → fan out on (BackupState, CacheFreshness, Mode).
//	(10) Duplicate identity → strict deny; others warn.
//
// Audit mode tightening: even when an earlier rule WOULD have denied,
// audit mode allows. The Output still records ReasonCode=DeniedXxx so the
// caller can emit an "would-have-denied" event and metric; the Admit and
// Mutate flags reflect audit's observe-only contract via Severity=Warn or
// ReasonAllowedAuditModeWouldDeny. See applyAuditOverride.
func Decide(in Input) Output {
	out := Output{
		Admit:                true, // default; deny paths flip this
		EffectiveMode:        in.Resolved.Mode,
		EffectiveModeSource:  in.Resolved.ModeSource,
		EffectiveRestoreMode: in.Resolved.Restore,
		Names: naming.Compute(
			in.Config.NamingStrategy,
			in.PVCName,
			in.Config.DefaultRepoSecretName,
		),
		BackupIdentity: resolveIdentity(in),
	}

	// (1) Excluded namespace — defense-in-depth.
	if _, excluded := in.Config.ExcludedNamespaces[in.Namespace]; excluded {
		out.ReasonCode = ReasonAllowedExcludedNamespace
		out.Message = fmt.Sprintf("namespace %q is excluded from pvc-plumber by operator config", in.Namespace)
		out.Severity = SeverityInfo
		return out
	}

	// (2) Not opted in. We do read legacy labels for inventory but the
	// admission decision ignores them per PRD constraint 3.
	if !in.LabelSpec.Enabled {
		out.ReasonCode = ReasonAllowedNotOptedIn
		out.Severity = SeverityInfo
		if in.LabelSpec.LegacyTier != labels.TierUnspecified {
			out.Message = fmt.Sprintf("PVC carries legacy label %q=%s but no %s=true; not opted in",
				labels.LegacyLabelBackup, in.LabelSpec.LegacyRaw, labels.LabelEnabled)
		} else {
			out.Message = fmt.Sprintf("PVC has no %s=true label; ignored", labels.LabelEnabled)
		}
		return out
	}

	// (3) Backup-exempt with a valid FQ reason annotation.
	if in.LabelSpec.ExemptKind == labels.ExemptValid {
		out.ReasonCode = ReasonAllowedExempt
		out.Message = fmt.Sprintf("PVC is backup-exempt: %s", in.LabelSpec.ExemptReason)
		out.Severity = SeverityInfo
		return out
	}

	// (4) Backup-exempt with missing reason: a known DR landmine. Deny.
	if in.LabelSpec.ExemptKind == labels.ExemptMissingReason {
		out.ReasonCode = ReasonDeniedExemptMissingReason
		out.Message = fmt.Sprintf("backup-exempt=true requires annotation %s to be set and non-empty",
			labels.LegacyAnnotationBackupExemptReasonFQ)
		out.Severity = SeverityError
		out.Admit = false
		return applyAuditOverride(in, out)
	}

	// (5) Skip-restore without reason: contract violation.
	if in.LabelSpec.SkipRestore && in.LabelSpec.SkipRestoreReason == "" {
		out.ReasonCode = ReasonDeniedSkipRestoreMissingReason
		out.Message = fmt.Sprintf("%s=true requires %s to be set and non-empty",
			labels.AnnotationSkipRestore, labels.AnnotationSkipRestoreReason)
		out.Severity = SeverityError
		out.Admit = false
		return applyAuditOverride(in, out)
	}

	// Aggregate parse errors from the label spec into Output for inspection.
	if len(in.LabelSpec.Errors) > 0 {
		out.ParseErrors = append(out.ParseErrors, in.LabelSpec.Errors...)
	}

	// (6) Parse errors (invalid tier, invalid UID/GID, malformed min-age, …).
	if len(in.LabelSpec.Errors) > 0 {
		switch in.Resolved.Mode {
		case mode.Strict, mode.Enforce:
			out.ReasonCode = ReasonDeniedInvalidConfig
			out.Message = "invalid label/annotation values: " + joinErrorMessages(in.LabelSpec.Errors)
			out.Severity = SeverityError
			out.Admit = false
			return applyAuditOverride(in, out)
		case mode.Audit, mode.Permissive:
			out.Events = append(out.Events, Event{
				Reason:  "InvalidLabelOrAnnotation",
				Type:    eventTypeWarning,
				Message: joinErrorMessages(in.LabelSpec.Errors),
			})
			out.Severity = SeverityWarn
			// Fall through to normal restore logic so the PVC is admitted.
		}
	}

	// (7) Restore-mode = never: admit, no mutate. RS may still be created
	// by the reconciler (this is admission-time only).
	if in.Resolved.Restore == mode.RestoreNever {
		out.ReasonCode = ReasonAllowedRestoreModeNever
		out.Message = "restore-mode=never; not injecting dataSourceRef"
		out.Severity = SeverityInfo
		return out
	}

	// (5b) Skip-restore WITH reason: admit, do not inject, emit event.
	if in.LabelSpec.SkipRestore {
		out.ReasonCode = ReasonAllowedSkipRestoreWithReason
		out.Message = fmt.Sprintf("skip-restore acknowledged: %s", in.LabelSpec.SkipRestoreReason)
		out.Severity = SeverityInfo
		out.Events = append(out.Events, Event{
			Reason:  "RestoreSkipped",
			Type:    "Normal",
			Message: in.LabelSpec.SkipRestoreReason,
		})
		return out
	}

	// (8) Restore-mode = force: backup MUST exist, else deny.
	if in.Resolved.Restore == mode.RestoreForce {
		switch in.BackupState {
		case BackupExists:
			out.Mutate = true
			out.DataSourceRef = &DataSourceRef{
				APIGroup: VolSyncAPIGroup,
				Kind:     VolSyncRDKind,
				Name:     out.Names.RD,
			}
			out.ReasonCode = ReasonAllowedRestoreInjected
			out.Message = fmt.Sprintf("restore-mode=force and backup exists; injected dataSourceRef → %s", out.Names.RD)
			out.Severity = SeverityInfo
			out.MetricsIncrement = append(out.MetricsIncrement, "pvc_plumber_restore_injections_total")
		case BackupMissing:
			out.Admit = false
			out.ReasonCode = ReasonDeniedRestoreForceNoBackup
			out.Message = "restore-mode=force but no backup exists for this PVC's identity"
			out.Severity = SeverityError
			out = applyAuditOverride(in, out)
		default: // BackupUnknown
			out.Admit = false
			out.ReasonCode = ReasonDeniedRestoreForceUnknown
			out.Message = "restore-mode=force but backup state is unknown (cache cold or backend unreachable)"
			out.Severity = SeverityError
			out = applyAuditOverride(in, out)
		}
		return out
	}

	// (9) Default restore path: fan out on (BackupState, CacheFreshness, Mode).
	switch in.BackupState {
	case BackupExists:
		// Cache stale + Exists: strict denies (can't trust the answer).
		if in.CacheFreshness == CacheStale {
			if in.Resolved.Mode == mode.Strict {
				out.Admit = false
				out.ReasonCode = ReasonDeniedCacheStaleStrict
				out.Message = fmt.Sprintf("strict mode: backup-truth cache is stale for identity %s; refusing to act on stale data", out.BackupIdentity)
				out.Severity = SeverityError
				return applyAuditOverride(in, out)
			}
			out.Events = append(out.Events, Event{
				Reason:  "CacheStale",
				Type:    eventTypeWarning,
				Message: fmt.Sprintf("cache is stale for identity %s but reports BackupExists; proceeding under %s mode", out.BackupIdentity, in.Resolved.Mode),
			})
			out.MetricsIncrement = append(out.MetricsIncrement, "pvc_plumber_cache_stale_warn_total")
		}
		// Mutate only when the mode authorizes it. Audit observes only.
		if in.Resolved.Mode.MutatesOnExists() {
			out.Mutate = true
			out.DataSourceRef = &DataSourceRef{
				APIGroup: VolSyncAPIGroup,
				Kind:     VolSyncRDKind,
				Name:     out.Names.RD,
			}
			out.ReasonCode = ReasonAllowedRestoreInjected
			out.Message = fmt.Sprintf("backup exists for %s; injected dataSourceRef → %s", out.BackupIdentity, out.Names.RD)
			out.Severity = SeverityInfo
			out.MetricsIncrement = append(out.MetricsIncrement, "pvc_plumber_restore_injections_total", "pvc_plumber_backup_exists_total")
		} else {
			// Audit mode: would inject. Record but don't mutate.
			out.ReasonCode = ReasonAllowedAuditModeWouldDeny
			out.Message = fmt.Sprintf("audit mode: would inject dataSourceRef → %s (backup exists for %s)", out.Names.RD, out.BackupIdentity)
			out.Severity = SeverityInfo
			out.Events = append(out.Events, Event{
				Reason:  "WouldInject",
				Type:    "Normal",
				Message: out.Message,
			})
		}

	case BackupMissing:
		// Cache stale + Missing: same dance as Exists.
		if in.CacheFreshness == CacheStale && in.Resolved.Mode == mode.Strict {
			out.Admit = false
			out.ReasonCode = ReasonDeniedCacheStaleStrict
			out.Message = fmt.Sprintf("strict mode: backup-truth cache is stale for identity %s; refusing to act on stale data", out.BackupIdentity)
			out.Severity = SeverityError
			return applyAuditOverride(in, out)
		}
		out.ReasonCode = ReasonAllowedFreshNoBackup
		out.Message = fmt.Sprintf("no backup exists for %s; allowing fresh PVC", out.BackupIdentity)
		out.Severity = SeverityInfo
		out.MetricsIncrement = append(out.MetricsIncrement, "pvc_plumber_fresh_pvc_total")
		if in.CacheFreshness == CacheStale {
			out.Events = append(out.Events, Event{
				Reason:  "CacheStale",
				Type:    eventTypeWarning,
				Message: fmt.Sprintf("cache is stale; reporting BackupMissing for %s under %s mode", out.BackupIdentity, in.Resolved.Mode),
			})
		}

	default: // BackupUnknown
		switch in.Resolved.Mode {
		case mode.Strict:
			out.Admit = false
			out.ReasonCode = ReasonDeniedBackupUnknownStrict
			out.Message = fmt.Sprintf("strict mode: backup state is unknown for %s; refusing to create potentially-empty protected PVC", out.BackupIdentity)
			out.Severity = SeverityError
			out.MetricsIncrement = append(out.MetricsIncrement, "pvc_plumber_backup_unknown_total")
			return applyAuditOverride(in, out)
		case mode.Enforce:
			out.Admit = false
			out.ReasonCode = ReasonDeniedBackupUnknownEnforce
			out.Message = fmt.Sprintf("enforce mode: backup state is unknown for %s", out.BackupIdentity)
			out.Severity = SeverityError
			out.MetricsIncrement = append(out.MetricsIncrement, "pvc_plumber_backup_unknown_total")
			return applyAuditOverride(in, out)
		case mode.Permissive:
			out.ReasonCode = ReasonAllowedPermissiveWarn
			out.Message = fmt.Sprintf("permissive mode: backup state is unknown for %s; allowing with warning", out.BackupIdentity)
			out.Severity = SeverityWarn
			out.Events = append(out.Events, Event{
				Reason:  eventReasonBackupStateUnknown,
				Type:    eventTypeWarning,
				Message: out.Message,
			})
			out.MetricsIncrement = append(out.MetricsIncrement, "pvc_plumber_backup_unknown_total")
		default: // Audit (and Unspecified handled defensively)
			out.ReasonCode = ReasonAllowedPermissiveWarn
			out.Message = fmt.Sprintf("audit mode: backup state is unknown for %s; observe-only", out.BackupIdentity)
			out.Severity = SeverityWarn
			out.Events = append(out.Events, Event{
				Reason:  eventReasonBackupStateUnknown,
				Type:    eventTypeWarning,
				Message: out.Message,
			})
			out.MetricsIncrement = append(out.MetricsIncrement, "pvc_plumber_backup_unknown_total")
		}
	}

	// (10) Duplicate backup identity. Only meaningful when caller passed
	// KnownIdentities; otherwise this is a no-op for Phase 2.
	if dup := findDuplicate(in); dup != nil {
		if in.Resolved.Mode == mode.Strict {
			out.Admit = false
			out.Mutate = false
			out.DataSourceRef = nil
			out.ReasonCode = ReasonDeniedDuplicateIdentityStrict
			out.Message = fmt.Sprintf("strict mode: backup identity %q already in use by %s/%s",
				dup.Identity, dup.Namespace, dup.PVCName)
			out.Severity = SeverityError
			return applyAuditOverride(in, out)
		}
		out.Events = append(out.Events, Event{
			Reason:  "DuplicateBackupIdentity",
			Type:    eventTypeWarning,
			Message: fmt.Sprintf("backup identity %q is also in use by %s/%s", dup.Identity, dup.Namespace, dup.PVCName),
		})
		out.MetricsIncrement = append(out.MetricsIncrement, "pvc_plumber_duplicate_identity_total")
	}

	return out
}

// resolveIdentity returns the effective kopia backup identity for the PVC.
// If the PVC has set pvc-plumber.io/backup-identity, that opaque value is
// used; otherwise the default <namespace>/<pvc> is constructed.
func resolveIdentity(in Input) string {
	if in.LabelSpec.BackupIdentity != "" {
		return in.LabelSpec.BackupIdentity
	}
	return in.Namespace + "/" + in.PVCName
}

// findDuplicate returns the first matching IdentityRef from
// in.KnownIdentities whose Identity matches the current PVC's resolved
// identity. Self-references (same namespace+pvc) are skipped.
func findDuplicate(in Input) *IdentityRef {
	id := resolveIdentity(in)
	for i, candidate := range in.KnownIdentities {
		if candidate.Namespace == in.Namespace && candidate.PVCName == in.PVCName {
			continue
		}
		if candidate.Identity == id {
			return &in.KnownIdentities[i]
		}
	}
	return nil
}

// applyAuditOverride converts a deny verdict into an audit "would-deny"
// allow when the effective mode is Audit. The original ReasonCode is
// preserved so the caller can still emit the correct metric and event;
// only Admit, Mutate, and Severity are softened.
func applyAuditOverride(in Input, out Output) Output {
	if in.Resolved.Mode != mode.Audit {
		return out
	}
	// Preserve the original deny reason for traceability, but flip the
	// effect.
	originalReason := out.ReasonCode
	out.Admit = true
	out.Mutate = false
	out.DataSourceRef = nil
	out.Severity = SeverityWarn
	out.Events = append(out.Events, Event{
		Reason:  "WouldDeny",
		Type:    eventTypeWarning,
		Message: fmt.Sprintf("audit mode override: would have denied with reason=%s — %s", originalReason, out.Message),
	})
	// Add a metric label so audit-mode would-denies can be tracked.
	out.MetricsIncrement = append(out.MetricsIncrement, "pvc_plumber_audit_would_deny_total")
	return out
}

func joinErrorMessages(errs []error) string {
	parts := make([]string, 0, len(errs))
	for _, e := range errs {
		parts = append(parts, e.Error())
	}
	return strings.Join(parts, "; ")
}
