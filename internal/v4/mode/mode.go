// Package mode owns the v4 operator-mode taxonomy and per-PVC override
// precedence rules. It is pure: no Kubernetes client, no I/O.
//
// Phase 2 of docs/pvc-plumber-v4-prd.md in the talos-argocd-proxmox repo.
package mode

import (
	"fmt"
	"strings"
)

// String forms of the Mode and RestoreMode enums. These constants exist
// to satisfy goconst (and to give tests + ParseMode a single source of
// truth for the wire string). Private; cross-package tests use their own
// local constants.
const (
	modeStrUnspecified = "unspecified"
	modeStrAudit       = "audit"
	modeStrPermissive  = "permissive"
	modeStrEnforce     = "enforce"
	modeStrStrict      = "strict"
	restoreStrNever    = "never"
	restoreStrForce    = "force"
)

// Mode is the global / per-namespace / per-PVC operator behavior.
type Mode int

const (
	// Unspecified is the zero value; resolution falls back through
	// the precedence chain to the operator default (typically Audit).
	Unspecified Mode = iota

	// Audit observes and reports only. Webhooks never deny; reconciler may
	// compute the expected RS/RD/Secret shape and emit events, but does not
	// create or mutate cluster resources.
	Audit

	// Permissive generates resources and warns on unknown backup truth, but
	// admission allows all opted-in PVC creates.
	Permissive

	// Enforce mutates PVC creates by injecting dataSourceRef when a known
	// backup exists, and denies on invalid config (bad tier, bad UID/GID,
	// duplicate identity in strict, etc.). Behavior on unknown backup truth
	// depends on Strict vs Enforce — Enforce allows unknown with warning.
	Enforce

	// Strict adds to Enforce: denies on cache-stale, denies on unknown
	// backup truth, requires skip-restore-reason, denies duplicate identity.
	// Strict is the destination mode after restore drills pass, not the
	// bootstrap default.
	Strict
)

func (m Mode) String() string {
	switch m {
	case Audit:
		return modeStrAudit
	case Permissive:
		return modeStrPermissive
	case Enforce:
		return modeStrEnforce
	case Strict:
		return modeStrStrict
	default:
		return modeStrUnspecified
	}
}

// ParseMode accepts "audit", "permissive", "enforce", or "strict"
// (case-insensitive, whitespace tolerated). Empty string returns
// (Unspecified, nil). Any other value returns an error.
func ParseMode(raw string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return Unspecified, nil
	case modeStrAudit:
		return Audit, nil
	case modeStrPermissive:
		return Permissive, nil
	case modeStrEnforce:
		return Enforce, nil
	case modeStrStrict:
		return Strict, nil
	default:
		return Unspecified, fmt.Errorf("invalid mode %q (expected audit|permissive|enforce|strict)", raw)
	}
}

// RestoreMode is the per-PVC restore policy override. It composes with Mode
// to determine whether dataSourceRef is injected on a PVC create.
//
//	never : suppress dataSourceRef injection entirely (PVC starts empty)
//	force : require a known backup; deny if missing or unknown
//	(audit|permissive|enforce|strict) : same semantics as the matching Mode,
//	scoped to the restore decision only.
type RestoreMode int

const (
	RestoreUnspecified RestoreMode = iota
	RestoreAudit
	RestorePermissive
	RestoreEnforce
	RestoreStrict
	RestoreNever
	RestoreForce
)

func (r RestoreMode) String() string {
	switch r {
	case RestoreAudit:
		return modeStrAudit
	case RestorePermissive:
		return modeStrPermissive
	case RestoreEnforce:
		return modeStrEnforce
	case RestoreStrict:
		return modeStrStrict
	case RestoreNever:
		return restoreStrNever
	case RestoreForce:
		return restoreStrForce
	default:
		return modeStrUnspecified
	}
}

// ParseRestoreMode accepts the six restore modes (case-insensitive). Empty
// string returns (RestoreUnspecified, nil). Any other value errors.
func ParseRestoreMode(raw string) (RestoreMode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return RestoreUnspecified, nil
	case modeStrAudit:
		return RestoreAudit, nil
	case modeStrPermissive:
		return RestorePermissive, nil
	case modeStrEnforce:
		return RestoreEnforce, nil
	case modeStrStrict:
		return RestoreStrict, nil
	case restoreStrNever:
		return RestoreNever, nil
	case restoreStrForce:
		return RestoreForce, nil
	default:
		return RestoreUnspecified, fmt.Errorf("invalid restore-mode %q (expected audit|permissive|enforce|strict|never|force)", raw)
	}
}

// Source describes which layer set a Mode, for traceability in events and
// metrics.
type Source int

const (
	SourceDefault       Source = iota // operator config default
	SourceGlobal                      // operator config explicit setting
	SourceNamespace                   // namespace label or ConfigMap (future)
	SourcePVCAnnotation               // pvc-plumber.io/mode on the PVC
)

func (s Source) String() string {
	switch s {
	case SourcePVCAnnotation:
		return "pvc-annotation"
	case SourceNamespace:
		return "namespace"
	case SourceGlobal:
		return "global"
	default:
		return "default"
	}
}

// Resolved captures the result of running the precedence chain: the
// effective Mode, the effective RestoreMode, and which Source(s) won.
type Resolved struct {
	Mode          Mode
	ModeSource    Source
	Restore       RestoreMode
	RestoreSource Source
}

// PrecedenceInputs feeds Resolve. Empty raw strings mean "not set at that
// layer"; the chain walks PVC → Namespace → Global → Default.
type PrecedenceInputs struct {
	PVCMode              string
	PVCRestoreMode       string
	NamespaceMode        string
	NamespaceRestoreMode string
	GlobalMode           Mode
	GlobalRestore        RestoreMode
	Default              Mode
	DefaultRestore       RestoreMode
}

// Resolve walks PVC annotation > namespace setting > global config > default.
// Parse errors at any layer are returned as a non-nil error and Resolved
// holds the next-layer fallback (so a malformed PVC annotation does not
// brick admission — the global default still applies and the caller can
// decide to deny or warn based on the error and current mode).
func Resolve(in PrecedenceInputs) (Resolved, error) {
	var errs []error

	// Mode: PVC > namespace > global > default.
	resolvedMode, modeSrc := in.Default, SourceDefault
	if in.GlobalMode != Unspecified {
		resolvedMode = in.GlobalMode
		modeSrc = SourceGlobal
	}
	if in.NamespaceMode != "" {
		if m, err := ParseMode(in.NamespaceMode); err != nil {
			errs = append(errs, fmt.Errorf("namespace mode: %w", err))
		} else if m != Unspecified {
			resolvedMode = m
			modeSrc = SourceNamespace
		}
	}
	if in.PVCMode != "" {
		if m, err := ParseMode(in.PVCMode); err != nil {
			errs = append(errs, fmt.Errorf("pvc mode: %w", err))
		} else if m != Unspecified {
			resolvedMode = m
			modeSrc = SourcePVCAnnotation
		}
	}

	// Restore mode: PVC > namespace > global > default.
	resolvedRestore, restoreSrc := in.DefaultRestore, SourceDefault
	if in.GlobalRestore != RestoreUnspecified {
		resolvedRestore = in.GlobalRestore
		restoreSrc = SourceGlobal
	}
	if in.NamespaceRestoreMode != "" {
		if r, err := ParseRestoreMode(in.NamespaceRestoreMode); err != nil {
			errs = append(errs, fmt.Errorf("namespace restore-mode: %w", err))
		} else if r != RestoreUnspecified {
			resolvedRestore = r
			restoreSrc = SourceNamespace
		}
	}
	if in.PVCRestoreMode != "" {
		if r, err := ParseRestoreMode(in.PVCRestoreMode); err != nil {
			errs = append(errs, fmt.Errorf("pvc restore-mode: %w", err))
		} else if r != RestoreUnspecified {
			resolvedRestore = r
			restoreSrc = SourcePVCAnnotation
		}
	}

	res := Resolved{
		Mode:          resolvedMode,
		ModeSource:    modeSrc,
		Restore:       resolvedRestore,
		RestoreSource: restoreSrc,
	}
	if len(errs) == 0 {
		return res, nil
	}
	return res, joinErrors(errs)
}

// SafeBootstrapDefaults returns the recommended initial defaults for the
// operator: audit globally, audit for restore. Phase 2-3 deploys this; later
// phases flip to permissive (Phase 6) and enforce (Phase 10).
func SafeBootstrapDefaults() (mode Mode, restore RestoreMode) {
	return Audit, RestoreAudit
}

// DeniesUnknown reports whether the given mode denies admission when backup
// truth is unknown for an opted-in PVC. Used by the decision engine.
func (m Mode) DeniesUnknown() bool { return m == Strict }

// DeniesStale reports whether the given mode denies admission when the
// backup cache is stale. Only Strict denies on stale.
func (m Mode) DeniesStale() bool { return m == Strict }

// MutatesOnExists reports whether the given mode injects dataSourceRef when
// a backup is known to exist.
func (m Mode) MutatesOnExists() bool {
	return m == Permissive || m == Enforce || m == Strict
}

// WritesResources reports whether the reconciler may create or update
// child RS/RD/Secret resources in this mode. Audit observes only.
func (m Mode) WritesResources() bool {
	return m == Permissive || m == Enforce || m == Strict
}

// joinErrors concatenates a slice of errors into one. Simple; sufficient
// for our diagnostic needs. Avoids importing errors.Join for older Go.
type multiError struct{ errs []error }

func (m *multiError) Error() string {
	parts := make([]string, 0, len(m.errs))
	for _, e := range m.errs {
		parts = append(parts, e.Error())
	}
	return strings.Join(parts, "; ")
}

func joinErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	if len(errs) == 1 {
		return errs[0]
	}
	return &multiError{errs: errs}
}
