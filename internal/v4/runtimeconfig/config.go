// Package runtimeconfig resolves the operator's runtime configuration from
// environment variables. It exists separately from the v3 internal/config
// package so the v4 mode-aware boot path doesn't tangle with the legacy
// HTTP-only config layer.
//
// Phase 2.5 of docs/pvc-plumber-v4-prd.md in the talos-argocd-proxmox repo.
//
// The primary input is PVC_PLUMBER_MODE. It accepts the four operator-mode
// values (audit | permissive | enforce | strict) and defaults to audit when
// unset, malformed, or empty. The default is safe-by-design: an audit-mode
// binary cannot perform cluster writes regardless of what other code paths
// might try to do (see internal/v4/auditclient).
package runtimeconfig

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/mitchross/pvc-plumber/internal/v4/mode"
)

// EnvKey is the environment variable name parsed by Load.
const EnvKey = "PVC_PLUMBER_MODE"

// Env var names for the v4 builder/executor defaults consumed in
// permissive (and later enforce/strict) mode. The names are stable
// strings; tests rely on them, and the runbook/cutover docs quote
// them verbatim. See RequireV4WriteDefaults for the validation
// contract that ties these to PVC_PLUMBER_MODE.
const (
	EnvDefaultSnapshotClass = "PVC_PLUMBER_DEFAULT_SNAPSHOT_CLASS"
	EnvDefaultCacheCapacity = "PVC_PLUMBER_DEFAULT_CACHE_CAPACITY"
	EnvDefaultStorageClass  = "PVC_PLUMBER_DEFAULT_STORAGE_CLASS"
	EnvDefaultUID           = "PVC_PLUMBER_DEFAULT_UID"
	EnvDefaultGID           = "PVC_PLUMBER_DEFAULT_GID"
	EnvDefaultFSGroup       = "PVC_PLUMBER_DEFAULT_FSGROUP"
)

// Config is the resolved runtime configuration. Add fields here as the
// operator gains more env-driven knobs; Phase 2.5 added Mode and Patch
// 6.8a added the six v4 write-mode defaults.
type Config struct {
	// Mode is the resolved operator mode after parsing PVC_PLUMBER_MODE.
	// Always one of: mode.Audit | mode.Permissive | mode.Enforce | mode.Strict.
	// Never mode.Unspecified — Load coerces to mode.Audit instead.
	Mode mode.Mode

	// ModeSource records HOW Mode was set, for traceability in logs and
	// metrics. Useful when diagnosing "why is the operator in audit mode?"
	// against an unexpected PVC_PLUMBER_MODE value.
	ModeSource ModeSource

	// RawModeValue is the verbatim PVC_PLUMBER_MODE env var value, useful
	// for diagnostic logs when ModeSource is SourceMalformed.
	RawModeValue string

	// v4 builder/executor defaults (Patch 6.8a).
	//
	// String fields default to "" when their env var is unset. The
	// builder's coalesce() chain falls through per-PVC label override
	// → PVC spec → these defaults; in permissive mode an empty value
	// would silently emit RS/RD with an empty field, so
	// RequireV4WriteDefaults rejects unset string values for permissive.
	//
	// Integer fields are pointers so an unset env var is distinguishable
	// from an explicit "0". The builder dereferences nil-pointers to
	// the literal zero value (root); permissive validation rejects both
	// nil and *0 because a root-mover security context is incompatible
	// with the talos cluster's PSA profile and the existing inline
	// RS/RD convention of 568:568:568.
	DefaultSnapshotClass string
	DefaultCacheCapacity string
	DefaultStorageClass  string
	DefaultUID           *int64
	DefaultGID           *int64
	DefaultFSGroup       *int64
}

// ModeSource classifies where the effective Mode came from.
type ModeSource int

const (
	// SourceDefault means PVC_PLUMBER_MODE was unset or empty; Mode
	// fell back to audit.
	SourceDefault ModeSource = iota

	// SourceEnv means PVC_PLUMBER_MODE was set to a valid value and Mode
	// reflects it exactly.
	SourceEnv

	// SourceMalformed means PVC_PLUMBER_MODE was set to an unparseable
	// value; Mode was coerced to audit for safety.
	SourceMalformed
)

func (s ModeSource) String() string {
	switch s {
	case SourceEnv:
		return "env"
	case SourceMalformed:
		return "malformed-env-coerced-to-audit"
	default:
		return "default-audit"
	}
}

// Load reads the process environment and returns a resolved Config plus
// any non-fatal warning. The returned Config is always usable; the
// optional warning surfaces a malformed PVC_PLUMBER_MODE value (or an
// invalid integer in one of the v4 default env vars) so the caller can
// log it loudly.
//
// Mode behavior matrix:
//
//	env unset / empty            → Mode=Audit ModeSource=SourceDefault
//	env=audit|permissive|enforce|strict
//	                             → Mode set ModeSource=SourceEnv
//	env=anything else            → Mode=Audit ModeSource=SourceMalformed (warning)
//
// V4 default fields (Patch 6.8a) are populated best-effort:
//
//	unset / empty                → field stays at zero / nil
//	valid string                 → field set verbatim (trimmed)
//	non-numeric int              → returns warning, field stays nil
//	negative int                 → returns warning, field stays nil
//	"0" or positive int          → field set to *int64 of that value
//
// Validation that defaults are present + non-zero for permissive lives
// in RequireV4WriteDefaults, called separately by the operator binary
// after Load (so audit-mode startup never trips on missing defaults).
func Load() (Config, error) {
	raw := os.Getenv(EnvKey)
	cfg := Config{RawModeValue: raw}

	var errs []error

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		cfg.Mode = mode.Audit
		cfg.ModeSource = SourceDefault
	} else {
		parsed, err := mode.ParseMode(trimmed)
		switch {
		case err != nil:
			cfg.Mode = mode.Audit
			cfg.ModeSource = SourceMalformed
			errs = append(errs, fmt.Errorf("invalid %s=%q: %w (coerced to audit for safety)", EnvKey, raw, err))
		case parsed == mode.Unspecified:
			// ParseMode returns (Unspecified, nil) for empty string,
			// already handled above. Defensive: future ParseMode
			// changes that yield Unspecified are treated as default.
			cfg.Mode = mode.Audit
			cfg.ModeSource = SourceDefault
		default:
			cfg.Mode = parsed
			cfg.ModeSource = SourceEnv
		}
	}

	// V4 defaults. Strings are trimmed; integers may set a non-fatal
	// warning that the caller logs. Permissive-required-defaults gating
	// happens in RequireV4WriteDefaults, NOT here, so Load's contract
	// "always returns a usable Config" stays intact.
	cfg.DefaultSnapshotClass = strings.TrimSpace(os.Getenv(EnvDefaultSnapshotClass))
	cfg.DefaultCacheCapacity = strings.TrimSpace(os.Getenv(EnvDefaultCacheCapacity))
	cfg.DefaultStorageClass = strings.TrimSpace(os.Getenv(EnvDefaultStorageClass))

	if v, err := parseNonNegInt64Env(EnvDefaultUID); err != nil {
		errs = append(errs, err)
	} else {
		cfg.DefaultUID = v
	}
	if v, err := parseNonNegInt64Env(EnvDefaultGID); err != nil {
		errs = append(errs, err)
	} else {
		cfg.DefaultGID = v
	}
	if v, err := parseNonNegInt64Env(EnvDefaultFSGroup); err != nil {
		errs = append(errs, err)
	} else {
		cfg.DefaultFSGroup = v
	}

	switch len(errs) {
	case 0:
		return cfg, nil
	case 1:
		return cfg, errs[0]
	default:
		msgs := make([]string, 0, len(errs))
		for _, e := range errs {
			msgs = append(msgs, e.Error())
		}
		return cfg, fmt.Errorf("%s", strings.Join(msgs, "; "))
	}
}

// parseNonNegInt64Env returns (nil, nil) if the env var is unset /
// whitespace-only, (nil, err) for non-numeric or negative values, and
// (&v, nil) for any valid non-negative integer (including 0). The
// "0 is valid here" choice keeps Load lenient; RequireV4WriteDefaults
// is where permissive-mode rejects 0 for security-context fields.
func parseNonNegInt64Env(key string) (*int64, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid %s=%q: not a valid integer: %w", key, raw, err)
	}
	if v < 0 {
		return nil, fmt.Errorf("invalid %s=%q: must be non-negative", key, raw)
	}
	return &v, nil
}

// RequireV4WriteDefaults enforces the Patch 6.8a contract: in
// permissive mode, all six PVC_PLUMBER_DEFAULT_* env vars must be set
// to non-empty / non-zero values before the operator binary will start.
// Returns a single composite error listing every missing or zero field
// so the operator who set PVC_PLUMBER_MODE=permissive without the
// defaults gets a single actionable log line instead of a sequence of
// half-broken startups.
//
// Behavior matrix:
//
//	Mode=Audit                 → nil (defaults optional, executor short-circuits)
//	Mode=Permissive            → nil iff all six are set + UID/GID/FSGroup > 0
//	Mode=Enforce / Strict      → nil (defensive; validateMode in the
//	                             operator binary rejects these at startup
//	                             before reaching this validator)
//	Mode=Unspecified           → nil (defensive; Load coerces to Audit
//	                             before this is reached)
//
// The "> 0" check on integer fields is intentional. The current talos
// cluster runs VolSync mover Pods as 568:568:568 (matching every app's
// runAsUser) and a value of 0 (root) is incompatible with both the
// cluster's PSA profile and the existing inline RS/RD pattern.
func RequireV4WriteDefaults(cfg Config) error {
	if cfg.Mode != mode.Permissive {
		return nil
	}

	var missing []string
	if cfg.DefaultSnapshotClass == "" {
		missing = append(missing, EnvDefaultSnapshotClass+" must be set when "+EnvKey+"=permissive")
	}
	if cfg.DefaultCacheCapacity == "" {
		missing = append(missing, EnvDefaultCacheCapacity+" must be set when "+EnvKey+"=permissive")
	}
	if cfg.DefaultStorageClass == "" {
		missing = append(missing, EnvDefaultStorageClass+" must be set when "+EnvKey+"=permissive")
	}

	switch {
	case cfg.DefaultUID == nil:
		missing = append(missing, EnvDefaultUID+" must be set when "+EnvKey+"=permissive")
	case *cfg.DefaultUID == 0:
		missing = append(missing, EnvDefaultUID+" must be > 0 when "+EnvKey+"=permissive (got 0; root mover security context is not supported)")
	}

	switch {
	case cfg.DefaultGID == nil:
		missing = append(missing, EnvDefaultGID+" must be set when "+EnvKey+"=permissive")
	case *cfg.DefaultGID == 0:
		missing = append(missing, EnvDefaultGID+" must be > 0 when "+EnvKey+"=permissive (got 0; root mover security context is not supported)")
	}

	switch {
	case cfg.DefaultFSGroup == nil:
		missing = append(missing, EnvDefaultFSGroup+" must be set when "+EnvKey+"=permissive")
	case *cfg.DefaultFSGroup == 0:
		missing = append(missing, EnvDefaultFSGroup+" must be > 0 when "+EnvKey+"=permissive (got 0; root mover security context is not supported)")
	}

	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("v4 permissive mode requires explicit defaults:\n  - %s", strings.Join(missing, "\n  - "))
}

// Banner returns the one-line startup message the operator logs immediately
// after loading the runtime configuration. The exact phrasing for audit
// mode is the contract required by Phase 2.5 deliverable §4.
func (c Config) Banner() string {
	switch c.Mode {
	case mode.Audit:
		return fmt.Sprintf("pvc-plumber starting in audit mode: no cluster writes will be performed (%s=%q via %s)",
			EnvKey, c.RawModeValue, c.ModeSource)
	case mode.Permissive:
		return fmt.Sprintf("pvc-plumber starting in permissive mode: will generate resources and warn on unknown backup truth (%s=%q via %s)",
			EnvKey, c.RawModeValue, c.ModeSource)
	case mode.Enforce:
		return fmt.Sprintf("pvc-plumber starting in enforce mode: will deny on unknown backup truth for opted-in PVCs (%s=%q via %s)",
			EnvKey, c.RawModeValue, c.ModeSource)
	case mode.Strict:
		return fmt.Sprintf("pvc-plumber starting in STRICT mode: will deny on stale cache, unknown truth, and duplicate identities (%s=%q via %s)",
			EnvKey, c.RawModeValue, c.ModeSource)
	default:
		return fmt.Sprintf("pvc-plumber starting in unspecified mode (%s=%q) — DEFENSIVE: treating as audit",
			EnvKey, c.RawModeValue)
	}
}

// WritesAllowed is a convenience predicate. False when Mode is Audit.
// Use this at decision points where a verbose mode check would otherwise
// be inline.
func (c Config) WritesAllowed() bool {
	return c.Mode != mode.Audit && c.Mode != mode.Unspecified
}

// WebhookRegistrationAllowed reports whether the main process should
// register the admission webhook handlers. In audit mode the binary must
// be safe to deploy without webhooks; we skip registration even though
// no MutatingWebhookConfiguration / ValidatingWebhookConfiguration is
// expected to exist yet.
//
// Audit mode → false. All other modes → true.
func (c Config) WebhookRegistrationAllowed() bool {
	return c.Mode != mode.Audit && c.Mode != mode.Unspecified
}
