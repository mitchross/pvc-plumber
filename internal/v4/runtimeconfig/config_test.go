package runtimeconfig

import (
	"strings"
	"testing"

	"github.com/mitchross/pvc-plumber/internal/v4/mode"
)

// Test-scope constants for mode string values (same wire form the
// mode package exposes via ParseMode, kept private there).
const (
	valPermissive = "permissive"
	valEnforce    = "enforce"
	valStrict     = "strict"
)

func TestLoad(t *testing.T) {
	cases := []struct {
		name       string
		env        string
		envSet     bool
		wantMode   mode.Mode
		wantSource ModeSource
		wantErr    bool
	}{
		{
			name:       "unset → default audit",
			envSet:     false,
			wantMode:   mode.Audit,
			wantSource: SourceDefault,
		},
		{
			name:       "empty string → default audit",
			env:        "",
			envSet:     true,
			wantMode:   mode.Audit,
			wantSource: SourceDefault,
		},
		{
			name:       "whitespace-only → default audit",
			env:        "   ",
			envSet:     true,
			wantMode:   mode.Audit,
			wantSource: SourceDefault,
		},
		{
			name:       "audit set explicitly",
			env:        "audit",
			envSet:     true,
			wantMode:   mode.Audit,
			wantSource: SourceEnv,
		},
		{
			name:       valPermissive,
			env:        valPermissive,
			envSet:     true,
			wantMode:   mode.Permissive,
			wantSource: SourceEnv,
		},
		{
			name:       valEnforce,
			env:        valEnforce,
			envSet:     true,
			wantMode:   mode.Enforce,
			wantSource: SourceEnv,
		},
		{
			name:       valStrict,
			env:        valStrict,
			envSet:     true,
			wantMode:   mode.Strict,
			wantSource: SourceEnv,
		},
		{
			name:       "case-insensitive",
			env:        "ENFORCE",
			envSet:     true,
			wantMode:   mode.Enforce,
			wantSource: SourceEnv,
		},
		{
			name:       "whitespace tolerated",
			env:        "  permissive  ",
			envSet:     true,
			wantMode:   mode.Permissive,
			wantSource: SourceEnv,
		},
		{
			name:       "garbage → coerced to audit + warning",
			env:        "yolo",
			envSet:     true,
			wantMode:   mode.Audit,
			wantSource: SourceMalformed,
			wantErr:    true,
		},
		{
			name:       "never (RestoreMode token) is NOT a Mode → coerced",
			env:        "never",
			envSet:     true,
			wantMode:   mode.Audit,
			wantSource: SourceMalformed,
			wantErr:    true,
		},
		{
			name:       "force (RestoreMode token) is NOT a Mode → coerced",
			env:        "force",
			envSet:     true,
			wantMode:   mode.Audit,
			wantSource: SourceMalformed,
			wantErr:    true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.envSet {
				t.Setenv(EnvKey, tc.env)
			} else {
				// t.Setenv unsets when test ends, but we must explicitly
				// unset for "no env" cases since prior tests may have set it.
				t.Setenv(EnvKey, "")
			}
			cfg, err := Load()
			if (err != nil) != tc.wantErr {
				t.Errorf("err: got %v, wantErr=%v", err, tc.wantErr)
			}
			if cfg.Mode != tc.wantMode {
				t.Errorf("Mode: got %v, want %v", cfg.Mode, tc.wantMode)
			}
			if cfg.ModeSource != tc.wantSource {
				t.Errorf("ModeSource: got %v, want %v", cfg.ModeSource, tc.wantSource)
			}
		})
	}
}

func TestBanner_ContractWording(t *testing.T) {
	// Phase 2.5 deliverable §4 requires the exact wording:
	//   "pvc-plumber starting in audit mode: no cluster writes will be performed"
	t.Setenv(EnvKey, "audit")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	b := cfg.Banner()
	required := "pvc-plumber starting in audit mode: no cluster writes will be performed"
	if !strings.HasPrefix(b, required) {
		t.Errorf("audit banner must start with the contract line.\n  got:    %q\n  prefix: %q", b, required)
	}
}

func TestBanner_NonAuditModes(t *testing.T) {
	// Smoke-test: banners for non-audit modes mention the mode and the
	// PVC_PLUMBER_MODE source so an operator can tell at a glance whether
	// the binary is in a write-capable mode.
	for _, m := range []string{"permissive", "enforce", "strict"} {
		t.Run(m, func(t *testing.T) {
			t.Setenv(EnvKey, m)
			cfg, _ := Load()
			b := cfg.Banner()
			if !strings.Contains(b, m) {
				t.Errorf("banner does not mention mode %q: %s", m, b)
			}
			if !strings.Contains(b, EnvKey) {
				t.Errorf("banner does not mention env key %q: %s", EnvKey, b)
			}
		})
	}
}

func TestBanner_MalformedShownInBanner(t *testing.T) {
	t.Setenv(EnvKey, "garbage")
	cfg, _ := Load()
	b := cfg.Banner()
	if !strings.Contains(b, "audit") {
		t.Errorf("malformed env should fall back to audit banner; got %q", b)
	}
	if !strings.Contains(b, "garbage") {
		t.Errorf("malformed env value should be echoed in banner for diagnostics; got %q", b)
	}
	if !strings.Contains(b, "malformed") {
		t.Errorf("banner should indicate malformed source; got %q", b)
	}
}

func TestWritesAllowed(t *testing.T) {
	cases := []struct {
		mode    mode.Mode
		allowed bool
	}{
		{mode.Audit, false},
		{mode.Unspecified, false}, // defensive
		{mode.Permissive, true},
		{mode.Enforce, true},
		{mode.Strict, true},
	}
	for _, tc := range cases {
		t.Run(tc.mode.String(), func(t *testing.T) {
			cfg := Config{Mode: tc.mode}
			if got := cfg.WritesAllowed(); got != tc.allowed {
				t.Errorf("WritesAllowed(%s): got %v, want %v", tc.mode, got, tc.allowed)
			}
		})
	}
}

func TestWebhookRegistrationAllowed(t *testing.T) {
	cases := []struct {
		mode    mode.Mode
		allowed bool
	}{
		{mode.Audit, false},
		{mode.Unspecified, false}, // defensive
		{mode.Permissive, true},
		{mode.Enforce, true},
		{mode.Strict, true},
	}
	for _, tc := range cases {
		t.Run(tc.mode.String(), func(t *testing.T) {
			cfg := Config{Mode: tc.mode}
			if got := cfg.WebhookRegistrationAllowed(); got != tc.allowed {
				t.Errorf("WebhookRegistrationAllowed(%s): got %v, want %v", tc.mode, got, tc.allowed)
			}
		})
	}
}

func TestModeSourceString(t *testing.T) {
	cases := map[ModeSource]string{
		SourceDefault:   "default-audit",
		SourceEnv:       "env",
		SourceMalformed: "malformed-env-coerced-to-audit",
	}
	for src, want := range cases {
		if got := src.String(); got != want {
			t.Errorf("ModeSource(%d).String() = %q, want %q", src, got, want)
		}
	}
}
