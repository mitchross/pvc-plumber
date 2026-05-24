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

// Test-scope constants for the canonical v4 default values used by
// Patch 6.8a's parser + validator tests. Centralized to satisfy
// goconst — the strings repeat across canonicalDefaultsFixture and
// the per-field assertions.
const (
	testSnapshotClass = "longhorn-snapclass"
	testCacheCapacity = "2Gi"
	testStorageClass  = "longhorn"
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

// =============================================================================
// Patch 6.8a: v4 builder/executor defaults — env parsing + validation
// =============================================================================

// canonicalDefaultsFixture sets all six PVC_PLUMBER_DEFAULT_* env vars
// to the reference talos values (568:568:568, longhorn-snapclass, 2Gi,
// longhorn). Tests that need a "valid defaults" baseline call this and
// then mutate one env var (via t.Setenv) to drive the failure path.
func canonicalDefaultsFixture(t *testing.T) {
	t.Helper()
	t.Setenv(EnvDefaultSnapshotClass, testSnapshotClass)
	t.Setenv(EnvDefaultCacheCapacity, testCacheCapacity)
	t.Setenv(EnvDefaultStorageClass, testStorageClass)
	t.Setenv(EnvDefaultUID, "568")
	t.Setenv(EnvDefaultGID, "568")
	t.Setenv(EnvDefaultFSGroup, "568")
}

// unsetDefaultsFixture clears every PVC_PLUMBER_DEFAULT_* env var so
// Load returns nil pointers / empty strings. t.Setenv("") is the
// project-standard way to mark an env var "unset for this test"
// (matches the TestLoad pattern).
func unsetDefaultsFixture(t *testing.T) {
	t.Helper()
	t.Setenv(EnvDefaultSnapshotClass, "")
	t.Setenv(EnvDefaultCacheCapacity, "")
	t.Setenv(EnvDefaultStorageClass, "")
	t.Setenv(EnvDefaultUID, "")
	t.Setenv(EnvDefaultGID, "")
	t.Setenv(EnvDefaultFSGroup, "")
}

// TestLoad_DefaultsAllSet confirms Load reads every PVC_PLUMBER_DEFAULT_*
// var when present and stores them on the returned Config. UID/GID/
// FSGROUP land as *int64 (non-nil) so the validator can distinguish
// "explicitly 0" from "unset."
func TestLoad_DefaultsAllSet(t *testing.T) {
	t.Setenv(EnvKey, valPermissive)
	canonicalDefaultsFixture(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: unexpected error %v", err)
	}
	if cfg.DefaultSnapshotClass != testSnapshotClass {
		t.Errorf("DefaultSnapshotClass: got %q, want longhorn-snapclass", cfg.DefaultSnapshotClass)
	}
	if cfg.DefaultCacheCapacity != testCacheCapacity {
		t.Errorf("DefaultCacheCapacity: got %q, want 2Gi", cfg.DefaultCacheCapacity)
	}
	if cfg.DefaultStorageClass != testStorageClass {
		t.Errorf("DefaultStorageClass: got %q, want longhorn", cfg.DefaultStorageClass)
	}
	if cfg.DefaultUID == nil || *cfg.DefaultUID != 568 {
		t.Errorf("DefaultUID: got %v, want *568", cfg.DefaultUID)
	}
	if cfg.DefaultGID == nil || *cfg.DefaultGID != 568 {
		t.Errorf("DefaultGID: got %v, want *568", cfg.DefaultGID)
	}
	if cfg.DefaultFSGroup == nil || *cfg.DefaultFSGroup != 568 {
		t.Errorf("DefaultFSGroup: got %v, want *568", cfg.DefaultFSGroup)
	}
}

// TestLoad_DefaultsUnset confirms Load returns zero strings and nil
// int pointers when every PVC_PLUMBER_DEFAULT_* env var is unset.
// The audit-mode binary relies on this: missing defaults are allowed
// in audit, so Load must not error.
func TestLoad_DefaultsUnset(t *testing.T) {
	t.Setenv(EnvKey, "") // audit default
	unsetDefaultsFixture(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: unexpected error %v", err)
	}
	if cfg.DefaultSnapshotClass != "" {
		t.Errorf("DefaultSnapshotClass: got %q, want empty", cfg.DefaultSnapshotClass)
	}
	if cfg.DefaultCacheCapacity != "" {
		t.Errorf("DefaultCacheCapacity: got %q, want empty", cfg.DefaultCacheCapacity)
	}
	if cfg.DefaultStorageClass != "" {
		t.Errorf("DefaultStorageClass: got %q, want empty", cfg.DefaultStorageClass)
	}
	if cfg.DefaultUID != nil {
		t.Errorf("DefaultUID: got %v, want nil", *cfg.DefaultUID)
	}
	if cfg.DefaultGID != nil {
		t.Errorf("DefaultGID: got %v, want nil", *cfg.DefaultGID)
	}
	if cfg.DefaultFSGroup != nil {
		t.Errorf("DefaultFSGroup: got %v, want nil", *cfg.DefaultFSGroup)
	}
}

// TestLoad_DefaultsWhitespaceOnly: env vars that are pure whitespace
// behave identically to unset.
func TestLoad_DefaultsWhitespaceOnly(t *testing.T) {
	t.Setenv(EnvKey, "")
	t.Setenv(EnvDefaultSnapshotClass, "   ")
	t.Setenv(EnvDefaultUID, " \t ")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: unexpected error %v", err)
	}
	if cfg.DefaultSnapshotClass != "" {
		t.Errorf("whitespace SnapshotClass: got %q, want empty", cfg.DefaultSnapshotClass)
	}
	if cfg.DefaultUID != nil {
		t.Errorf("whitespace UID: got %v, want nil", *cfg.DefaultUID)
	}
}

// TestLoad_DefaultUIDInvalidString_Error returns a warning for non-
// numeric integer env vars but still returns a usable Config (audit
// fallback). The UID field stays nil so validators can branch on
// "missing" rather than "broken."
func TestLoad_DefaultUIDInvalidString_Error(t *testing.T) {
	t.Setenv(EnvKey, "")
	t.Setenv(EnvDefaultUID, "abc")
	cfg, err := Load()
	if err == nil {
		t.Fatal("Load: want non-nil error for non-numeric UID; got nil")
	}
	if !strings.Contains(err.Error(), EnvDefaultUID) {
		t.Errorf("error must name the offending env var; got %q", err.Error())
	}
	if cfg.DefaultUID != nil {
		t.Errorf("DefaultUID: got %v, want nil (invalid input must not populate)", *cfg.DefaultUID)
	}
}

// TestLoad_DefaultUIDNegative_Error: negative integers are rejected
// before reaching RequireV4WriteDefaults. The field stays nil.
func TestLoad_DefaultUIDNegative_Error(t *testing.T) {
	t.Setenv(EnvKey, "")
	t.Setenv(EnvDefaultUID, "-1")
	cfg, err := Load()
	if err == nil {
		t.Fatal("Load: want non-nil error for negative UID; got nil")
	}
	if !strings.Contains(err.Error(), "non-negative") {
		t.Errorf("error must mention non-negative; got %q", err.Error())
	}
	if cfg.DefaultUID != nil {
		t.Errorf("DefaultUID: got %v, want nil after negative input", *cfg.DefaultUID)
	}
}

// TestLoad_DefaultUIDZero_ParsesNonNil documents the Load-vs-validator
// split: Load accepts "0" (a valid non-negative integer) and stores
// *int64(0). RequireV4WriteDefaults is what rejects 0 in permissive
// mode. This separation lets audit mode tolerate "0" without erroring.
func TestLoad_DefaultUIDZero_ParsesNonNil(t *testing.T) {
	t.Setenv(EnvKey, "audit")
	t.Setenv(EnvDefaultUID, "0")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: unexpected error %v", err)
	}
	if cfg.DefaultUID == nil {
		t.Fatal("DefaultUID: got nil, want *0 (Load distinguishes explicit 0 from unset)")
	}
	if *cfg.DefaultUID != 0 {
		t.Errorf("DefaultUID: got %d, want 0", *cfg.DefaultUID)
	}
}

// TestLoad_AggregateMultipleErrors: when several int env vars are
// malformed simultaneously, Load returns a single composite error
// mentioning all offending env vars (rather than swallowing all but
// the first).
func TestLoad_AggregateMultipleErrors(t *testing.T) {
	t.Setenv(EnvKey, "")
	t.Setenv(EnvDefaultUID, "abc")
	t.Setenv(EnvDefaultGID, "-2")
	_, err := Load()
	if err == nil {
		t.Fatal("Load: want non-nil composite error; got nil")
	}
	if !strings.Contains(err.Error(), EnvDefaultUID) {
		t.Errorf("composite error missing %s; got %q", EnvDefaultUID, err.Error())
	}
	if !strings.Contains(err.Error(), EnvDefaultGID) {
		t.Errorf("composite error missing %s; got %q", EnvDefaultGID, err.Error())
	}
}

// =============================================================================
// RequireV4WriteDefaults validation matrix
// =============================================================================

// TestRequireV4WriteDefaults_AuditAcceptsAnyDefaults: audit mode never
// fails this validator, regardless of which defaults are set. The
// executor short-circuits in audit, so empty defaults are harmless.
func TestRequireV4WriteDefaults_AuditAcceptsAnyDefaults(t *testing.T) {
	cases := []Config{
		{Mode: mode.Audit}, // all unset
		{Mode: mode.Audit, DefaultSnapshotClass: "x"},                            // partially set
		{Mode: mode.Audit, DefaultUID: int64Ptr(0)},                              // explicit zero
		{Mode: mode.Audit, DefaultUID: int64Ptr(568), DefaultGID: int64Ptr(568)}, // valid subset
		{Mode: mode.Audit, DefaultSnapshotClass: "x", DefaultCacheCapacity: "1Gi", // fully set
			DefaultStorageClass: "y", DefaultUID: int64Ptr(568),
			DefaultGID: int64Ptr(568), DefaultFSGroup: int64Ptr(568)},
	}
	for i, cfg := range cases {
		if err := RequireV4WriteDefaults(cfg); err != nil {
			t.Errorf("case %d audit: got error %v, want nil", i, err)
		}
	}
}

// TestRequireV4WriteDefaults_PermissiveAllSet_NoError: the happy path
// for the karakeep canary deployment.
func TestRequireV4WriteDefaults_PermissiveAllSet_NoError(t *testing.T) {
	cfg := Config{
		Mode:                 mode.Permissive,
		DefaultSnapshotClass: testSnapshotClass,
		DefaultCacheCapacity: testCacheCapacity,
		DefaultStorageClass:  testStorageClass,
		DefaultUID:           int64Ptr(568),
		DefaultGID:           int64Ptr(568),
		DefaultFSGroup:       int64Ptr(568),
	}
	if err := RequireV4WriteDefaults(cfg); err != nil {
		t.Errorf("permissive all-set: got error %v, want nil", err)
	}
}

// TestRequireV4WriteDefaults_PermissiveMissing iterates each required
// field, building a config that's complete except for the field under
// test, and asserts the error mentions the corresponding env var name.
// Single test method per field keeps the failure output specific.
func TestRequireV4WriteDefaults_PermissiveMissing(t *testing.T) {
	base := func() Config {
		return Config{
			Mode:                 mode.Permissive,
			DefaultSnapshotClass: testSnapshotClass,
			DefaultCacheCapacity: testCacheCapacity,
			DefaultStorageClass:  testStorageClass,
			DefaultUID:           int64Ptr(568),
			DefaultGID:           int64Ptr(568),
			DefaultFSGroup:       int64Ptr(568),
		}
	}
	cases := []struct {
		name      string
		mutate    func(*Config)
		wantToken string // substring required in the error
	}{
		{"snapshot class empty", func(c *Config) { c.DefaultSnapshotClass = "" }, EnvDefaultSnapshotClass},
		{"cache capacity empty", func(c *Config) { c.DefaultCacheCapacity = "" }, EnvDefaultCacheCapacity},
		{"storage class empty", func(c *Config) { c.DefaultStorageClass = "" }, EnvDefaultStorageClass},
		{"UID nil", func(c *Config) { c.DefaultUID = nil }, EnvDefaultUID},
		{"GID nil", func(c *Config) { c.DefaultGID = nil }, EnvDefaultGID},
		{"FSGroup nil", func(c *Config) { c.DefaultFSGroup = nil }, EnvDefaultFSGroup},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(&cfg)
			err := RequireV4WriteDefaults(cfg)
			if err == nil {
				t.Fatalf("permissive missing %s: got nil, want error", tc.wantToken)
			}
			if !strings.Contains(err.Error(), tc.wantToken) {
				t.Errorf("error must mention %s; got %q", tc.wantToken, err.Error())
			}
		})
	}
}

// TestRequireV4WriteDefaults_PermissiveExplicitZeroUID_Error: an
// explicit "0" parses cleanly through Load but RequireV4WriteDefaults
// rejects it for security-context fields. The error must distinguish
// "missing" (nil) from "set to zero" so the operator's log line gives
// the actual remediation.
func TestRequireV4WriteDefaults_PermissiveExplicitZeroUID_Error(t *testing.T) {
	zero := int64(0)
	cases := []struct {
		name      string
		mutate    func(*Config)
		wantToken string
	}{
		{"UID = 0", func(c *Config) { c.DefaultUID = &zero }, EnvDefaultUID},
		{"GID = 0", func(c *Config) { c.DefaultGID = &zero }, EnvDefaultGID},
		{"FSGroup = 0", func(c *Config) { c.DefaultFSGroup = &zero }, EnvDefaultFSGroup},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				Mode:                 mode.Permissive,
				DefaultSnapshotClass: testSnapshotClass,
				DefaultCacheCapacity: testCacheCapacity,
				DefaultStorageClass:  testStorageClass,
				DefaultUID:           int64Ptr(568),
				DefaultGID:           int64Ptr(568),
				DefaultFSGroup:       int64Ptr(568),
			}
			tc.mutate(&cfg)
			err := RequireV4WriteDefaults(cfg)
			if err == nil {
				t.Fatalf("permissive %s: got nil, want error", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantToken) {
				t.Errorf("error must mention %s; got %q", tc.wantToken, err.Error())
			}
			if !strings.Contains(err.Error(), "> 0") {
				t.Errorf("error must mention '> 0' so operator knows the constraint; got %q", err.Error())
			}
		})
	}
}

// TestRequireV4WriteDefaults_PermissiveAllMissing_AggregateError: when
// nothing is set, the validator produces a single composite error
// listing every missing env var so the operator sees the full
// remediation list in one log line.
func TestRequireV4WriteDefaults_PermissiveAllMissing_AggregateError(t *testing.T) {
	cfg := Config{Mode: mode.Permissive} // every default unset
	err := RequireV4WriteDefaults(cfg)
	if err == nil {
		t.Fatal("permissive nothing set: got nil, want composite error")
	}
	for _, key := range []string{
		EnvDefaultSnapshotClass, EnvDefaultCacheCapacity, EnvDefaultStorageClass,
		EnvDefaultUID, EnvDefaultGID, EnvDefaultFSGroup,
	} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("composite error missing %s; got %q", key, err.Error())
		}
	}
}

// TestRequireV4WriteDefaults_EnforceStrictNoError: defensive sanity
// — the validator does not error for enforce/strict because the
// operator binary's validateMode rejects them at startup before this
// runs. If a future refactor moves the call ordering, this test makes
// sure the validator's defensive behavior stays predictable.
func TestRequireV4WriteDefaults_EnforceStrictNoError(t *testing.T) {
	for _, m := range []mode.Mode{mode.Enforce, mode.Strict, mode.Unspecified} {
		cfg := Config{Mode: m} // every default unset
		if err := RequireV4WriteDefaults(cfg); err != nil {
			t.Errorf("RequireV4WriteDefaults(%s) defensive nil expected; got %v", m.String(), err)
		}
	}
}

// int64Ptr is a small test helper because Go has no &literal for
// numeric types.
func int64Ptr(v int64) *int64 { return &v }
