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
	"strings"

	"github.com/mitchross/pvc-plumber/internal/v4/mode"
)

// EnvKey is the environment variable name parsed by Load.
const EnvKey = "PVC_PLUMBER_MODE"

// Config is the resolved runtime configuration. Add fields here as the
// operator gains more env-driven knobs; for Phase 2.5 only Mode is plumbed.
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
// optional warning surfaces a malformed PVC_PLUMBER_MODE value so the
// caller can log it loudly.
//
// Behavior matrix:
//
//	env unset / empty            → Mode=Audit ModeSource=SourceDefault
//	env=audit|permissive|enforce|strict
//	                             → Mode set ModeSource=SourceEnv
//	env=anything else            → Mode=Audit ModeSource=SourceMalformed (warning)
func Load() (Config, error) {
	raw := os.Getenv(EnvKey)
	cfg := Config{RawModeValue: raw}

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		cfg.Mode = mode.Audit
		cfg.ModeSource = SourceDefault
		return cfg, nil
	}

	parsed, err := mode.ParseMode(trimmed)
	if err != nil {
		cfg.Mode = mode.Audit
		cfg.ModeSource = SourceMalformed
		return cfg, fmt.Errorf("invalid %s=%q: %w (coerced to audit for safety)", EnvKey, raw, err)
	}

	if parsed == mode.Unspecified {
		// ParseMode returns (Unspecified, nil) for the empty string but we
		// already handled that. Defensive: any future ParseMode change that
		// returns Unspecified should be treated as a default, not env-set.
		cfg.Mode = mode.Audit
		cfg.ModeSource = SourceDefault
		return cfg, nil
	}

	cfg.Mode = parsed
	cfg.ModeSource = SourceEnv
	return cfg, nil
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
