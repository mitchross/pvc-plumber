package decision

import (
	"errors"
	"strings"
	"testing"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/mode"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// Test-scope constants — repeated PVC names and event-reason strings.
const (
	testPVCData      = "data"
	testPVCDataDst   = "data-dst"
	testIdMyappData  = "myapp/data"
	testPVCLibrary   = "library"
	testPVCLibraryBk = "library-backup"
	testEvBackupUnk  = "BackupStateUnknown"
)

// helper: build the minimal opted-in PVC LabelSpec.
func optedIn(tier labels.Tier) labels.Spec {
	return labels.Spec{
		Origin:  labels.OriginNew,
		Enabled: true,
		Tier:    tier,
	}
}

// helper: build a fully-zero LabelSpec (PVC has no relevant labels).
func notOptedIn() labels.Spec {
	return labels.Spec{}
}

// helper: build a typical operator config.
func defaultConfig() Config {
	return Config{
		NamingStrategy:        naming.StrategyBareDst,
		DefaultRepoSecretName: naming.DefaultRepoSecretName,
		ExcludedNamespaces:    map[string]struct{}{},
	}
}

// helper: a Resolved with explicit Mode and RestoreMode.
func resolved(m mode.Mode, r mode.RestoreMode) mode.Resolved {
	return mode.Resolved{
		Mode:          m,
		ModeSource:    mode.SourceGlobal,
		Restore:       r,
		RestoreSource: mode.SourceGlobal,
	}
}

// helper: a base Input pre-filled with namespace, name, config.
func baseInput(spec labels.Spec, res mode.Resolved, bs BackupState, cf CacheFreshness) Input {
	return Input{
		Namespace:      "myapp",
		PVCName:        testPVCData,
		LabelSpec:      spec,
		Resolved:       res,
		BackupState:    bs,
		CacheFreshness: cf,
		Config:         defaultConfig(),
	}
}

// TestFailureMatrix exercises the 18 cases from docs/pvc-plumber-v4-prd.md §10.
// Each subtest is named with the PRD case number for traceability.
func TestFailureMatrix(t *testing.T) {
	cases := []struct {
		name        string
		in          Input
		wantAdmit   bool
		wantMutate  bool
		wantReason  ReasonCode
		wantDSRName string // expected DataSourceRef.Name; "" means no DSR
	}{
		// === PRD §10 Case 1: fresh app, no backup ===
		{
			name:       "case1: fresh app + no backup + enforce → allow, no DSR",
			in:         baseInput(optedIn(labels.TierDaily), resolved(mode.Enforce, mode.RestoreEnforce), BackupMissing, CacheFresh),
			wantAdmit:  true,
			wantMutate: false,
			wantReason: ReasonAllowedFreshNoBackup,
		},

		// === PRD §10 Case 2: backup exists → inject ===
		{
			name:        "case2: backup exists + enforce → inject DSR",
			in:          baseInput(optedIn(labels.TierDaily), resolved(mode.Enforce, mode.RestoreEnforce), BackupExists, CacheFresh),
			wantAdmit:   true,
			wantMutate:  true,
			wantReason:  ReasonAllowedRestoreInjected,
			wantDSRName: testPVCDataDst,
		},

		// === PRD §10 Case 3: backend unreachable + strict ===
		{
			name:       "case3: backup unknown + strict → DENY",
			in:         baseInput(optedIn(labels.TierDaily), resolved(mode.Strict, mode.RestoreStrict), BackupUnknown, CacheFreshnessUnknown),
			wantAdmit:  false,
			wantMutate: false,
			wantReason: ReasonDeniedBackupUnknownStrict,
		},

		// === PRD §10 Case 4: backend unreachable + audit/permissive ===
		{
			name:       "case4a: backup unknown + audit → allow + warn",
			in:         baseInput(optedIn(labels.TierDaily), resolved(mode.Audit, mode.RestoreAudit), BackupUnknown, CacheFreshnessUnknown),
			wantAdmit:  true,
			wantMutate: false,
			wantReason: ReasonAllowedPermissiveWarn,
		},
		{
			name:       "case4b: backup unknown + permissive → allow + warn",
			in:         baseInput(optedIn(labels.TierDaily), resolved(mode.Permissive, mode.RestorePermissive), BackupUnknown, CacheFreshnessUnknown),
			wantAdmit:  true,
			wantMutate: false,
			wantReason: ReasonAllowedPermissiveWarn,
		},

		// === PRD §10 Case 5: cache stale + strict ===
		{
			name:       "case5: cache stale + BackupExists + strict → DENY",
			in:         baseInput(optedIn(labels.TierDaily), resolved(mode.Strict, mode.RestoreStrict), BackupExists, CacheStale),
			wantAdmit:  false,
			wantMutate: false,
			wantReason: ReasonDeniedCacheStaleStrict,
		},

		// === PRD §10 Case 6: cache stale + audit/permissive ===
		{
			name:        "case6a: cache stale + BackupExists + permissive → allow + inject + warn",
			in:          baseInput(optedIn(labels.TierDaily), resolved(mode.Permissive, mode.RestorePermissive), BackupExists, CacheStale),
			wantAdmit:   true,
			wantMutate:  true,
			wantReason:  ReasonAllowedRestoreInjected,
			wantDSRName: testPVCDataDst,
		},
		{
			name:       "case6b: cache stale + BackupMissing + permissive → allow (fresh) + warn",
			in:         baseInput(optedIn(labels.TierDaily), resolved(mode.Permissive, mode.RestorePermissive), BackupMissing, CacheStale),
			wantAdmit:  true,
			wantMutate: false,
			wantReason: ReasonAllowedFreshNoBackup,
		},

		// === PRD §10 Case 7: duplicate identity + strict ===
		{
			name: "case7: duplicate identity + strict → DENY",
			in: func() Input {
				i := baseInput(optedIn(labels.TierDaily), resolved(mode.Strict, mode.RestoreStrict), BackupExists, CacheFresh)
				i.KnownIdentities = []IdentityRef{
					{Namespace: "other-app", PVCName: "data", Identity: testIdMyappData},
				}
				return i
			}(),
			wantAdmit:  false,
			wantMutate: false,
			wantReason: ReasonDeniedDuplicateIdentityStrict,
		},
		{
			name: "case7b: duplicate identity + permissive → warn + still inject",
			in: func() Input {
				i := baseInput(optedIn(labels.TierDaily), resolved(mode.Permissive, mode.RestorePermissive), BackupExists, CacheFresh)
				i.KnownIdentities = []IdentityRef{
					{Namespace: "other-app", PVCName: "data", Identity: testIdMyappData},
				}
				return i
			}(),
			wantAdmit:   true,
			wantMutate:  true,
			wantReason:  ReasonAllowedRestoreInjected,
			wantDSRName: testPVCDataDst,
		},

		// === PRD §10 Case 8: skip-restore without reason ===
		{
			name: "case8: skip-restore without reason + strict → DENY",
			in: func() Input {
				spec := optedIn(labels.TierDaily)
				spec.SkipRestore = true
				return baseInput(spec, resolved(mode.Strict, mode.RestoreStrict), BackupExists, CacheFresh)
			}(),
			wantAdmit:  false,
			wantMutate: false,
			wantReason: ReasonDeniedSkipRestoreMissingReason,
		},
		{
			name: "case8b: skip-restore without reason + permissive → ALSO DENY (contract violation)",
			in: func() Input {
				spec := optedIn(labels.TierDaily)
				spec.SkipRestore = true
				return baseInput(spec, resolved(mode.Permissive, mode.RestorePermissive), BackupExists, CacheFresh)
			}(),
			wantAdmit:  false,
			wantMutate: false,
			wantReason: ReasonDeniedSkipRestoreMissingReason,
		},

		// === PRD §10 Case 9: skip-restore with reason ===
		{
			name: "case9: skip-restore with reason → allow, no inject",
			in: func() Input {
				spec := optedIn(labels.TierDaily)
				spec.SkipRestore = true
				spec.SkipRestoreReason = "DR drill 2026-06-01"
				return baseInput(spec, resolved(mode.Strict, mode.RestoreStrict), BackupExists, CacheFresh)
			}(),
			wantAdmit:  true,
			wantMutate: false,
			wantReason: ReasonAllowedSkipRestoreWithReason,
		},

		// === PRD §10 Case 12 (re-purposed §11 effectively): RestoreMode = never ===
		{
			name: "case-never: restore-mode=never → allow, no inject (any backup state)",
			in: func() Input {
				return baseInput(optedIn(labels.TierDaily), resolved(mode.Enforce, mode.RestoreNever), BackupExists, CacheFresh)
			}(),
			wantAdmit:  true,
			wantMutate: false,
			wantReason: ReasonAllowedRestoreModeNever,
		},

		// === PRD §10 Case 13: restore-mode=force, backup exists ===
		{
			name:        "case-force-exists: restore-mode=force + BackupExists → inject",
			in:          baseInput(optedIn(labels.TierDaily), resolved(mode.Enforce, mode.RestoreForce), BackupExists, CacheFresh),
			wantAdmit:   true,
			wantMutate:  true,
			wantReason:  ReasonAllowedRestoreInjected,
			wantDSRName: testPVCDataDst,
		},

		// === PRD §10 Case 14: restore-mode=force, no backup ===
		{
			name:       "case-force-missing: restore-mode=force + BackupMissing → DENY",
			in:         baseInput(optedIn(labels.TierDaily), resolved(mode.Enforce, mode.RestoreForce), BackupMissing, CacheFresh),
			wantAdmit:  false,
			wantMutate: false,
			wantReason: ReasonDeniedRestoreForceNoBackup,
		},
		{
			name:       "case-force-unknown: restore-mode=force + BackupUnknown → DENY",
			in:         baseInput(optedIn(labels.TierDaily), resolved(mode.Enforce, mode.RestoreForce), BackupUnknown, CacheFreshnessUnknown),
			wantAdmit:  false,
			wantMutate: false,
			wantReason: ReasonDeniedRestoreForceUnknown,
		},

		// === Invalid tier / parse-error cases ===
		{
			name: "invalid-tier + strict → DENY",
			in: func() Input {
				spec := optedIn(labels.TierUnspecified)
				spec.Errors = []error{errors.New("pvc-plumber.io/tier: invalid tier \"monthly\"")}
				return baseInput(spec, resolved(mode.Strict, mode.RestoreStrict), BackupExists, CacheFresh)
			}(),
			wantAdmit:  false,
			wantMutate: false,
			wantReason: ReasonDeniedInvalidConfig,
		},
		{
			name: "invalid-tier + audit → allow + warn",
			in: func() Input {
				spec := optedIn(labels.TierUnspecified)
				spec.Errors = []error{errors.New("pvc-plumber.io/tier: invalid tier \"monthly\"")}
				return baseInput(spec, resolved(mode.Audit, mode.RestoreAudit), BackupExists, CacheFresh)
			}(),
			wantAdmit:  true,
			wantMutate: false,
			// Note: in audit mode, parse-errors warn and fall through; the
			// backup-exists branch then "would inject" → AuditModeWouldDeny.
			wantReason: ReasonAllowedAuditModeWouldDeny,
		},

		// === Not opted in / unrelated PVC ===
		{
			name:       "unrelated PVC (no labels) → admit, no mutate",
			in:         baseInput(notOptedIn(), resolved(mode.Strict, mode.RestoreStrict), BackupUnknown, CacheFreshnessUnknown),
			wantAdmit:  true,
			wantMutate: false,
			wantReason: ReasonAllowedNotOptedIn,
		},
		{
			name: "PVC with only legacy backup:hourly label → admit, no mutate (not opted in to webhook)",
			in: func() Input {
				spec := labels.Spec{
					Origin:     labels.OriginLegacyOnly,
					Enabled:    false,
					LegacyTier: labels.TierHourly,
					LegacyRaw:  "hourly",
					Tier:       labels.TierHourly,
				}
				return baseInput(spec, resolved(mode.Strict, mode.RestoreStrict), BackupUnknown, CacheFreshnessUnknown)
			}(),
			wantAdmit:  true,
			wantMutate: false,
			wantReason: ReasonAllowedNotOptedIn,
		},

		// === Backup-exempt contract ===
		{
			name: "exempt with FQ reason → admit, no mutate",
			in: func() Input {
				spec := labels.Spec{
					Origin:       labels.OriginNew,
					Enabled:      true,
					ExemptKind:   labels.ExemptValid,
					ExemptReason: "NAS-backed",
				}
				return baseInput(spec, resolved(mode.Strict, mode.RestoreStrict), BackupUnknown, CacheFreshnessUnknown)
			}(),
			wantAdmit:  true,
			wantMutate: false,
			wantReason: ReasonAllowedExempt,
		},
		{
			name: "exempt without FQ reason → DENY (any mode)",
			in: func() Input {
				spec := labels.Spec{
					Origin:     labels.OriginNew,
					Enabled:    true,
					ExemptKind: labels.ExemptMissingReason,
					Errors:     []error{errors.New("backup-exempt=true requires FQ reason annotation")},
				}
				return baseInput(spec, resolved(mode.Enforce, mode.RestoreEnforce), BackupUnknown, CacheFreshnessUnknown)
			}(),
			wantAdmit:  false,
			wantMutate: false,
			wantReason: ReasonDeniedExemptMissingReason,
		},

		// === Excluded namespace short-circuit ===
		{
			name: "excluded namespace (kube-system) → admit, no mutate even in strict",
			in: func() Input {
				i := baseInput(optedIn(labels.TierDaily), resolved(mode.Strict, mode.RestoreStrict), BackupUnknown, CacheFreshnessUnknown)
				i.Namespace = "kube-system"
				i.Config.ExcludedNamespaces = map[string]struct{}{"kube-system": {}}
				return i
			}(),
			wantAdmit:  true,
			wantMutate: false,
			wantReason: ReasonAllowedExcludedNamespace,
		},

		// === Multi-PVC same namespace with different identities remain distinct ===
		{
			name: "two PVCs same ns, different identities → no false duplicate trigger",
			in: func() Input {
				spec := optedIn(labels.TierDaily)
				spec.BackupIdentity = "" // default identity = myapp/data
				i := baseInput(spec, resolved(mode.Strict, mode.RestoreStrict), BackupExists, CacheFresh)
				// Sibling PVC in same ns has a DIFFERENT identity.
				i.KnownIdentities = []IdentityRef{
					{Namespace: "myapp", PVCName: "config", Identity: "myapp/config"},
				}
				return i
			}(),
			wantAdmit:   true,
			wantMutate:  true,
			wantReason:  ReasonAllowedRestoreInjected,
			wantDSRName: testPVCDataDst,
		},

		// === Audit overrides deny verdicts ===
		{
			name:       "audit override: would-deny-on-unknown becomes admit + would-deny event",
			in:         baseInput(optedIn(labels.TierDaily), resolved(mode.Audit, mode.RestoreAudit), BackupUnknown, CacheFreshnessUnknown),
			wantAdmit:  true,
			wantMutate: false,
			wantReason: ReasonAllowedPermissiveWarn,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := Decide(tc.in)
			if out.Admit != tc.wantAdmit {
				t.Errorf("Admit: got %v, want %v (reason=%s msg=%q)", out.Admit, tc.wantAdmit, out.ReasonCode, out.Message)
			}
			if out.Mutate != tc.wantMutate {
				t.Errorf("Mutate: got %v, want %v", out.Mutate, tc.wantMutate)
			}
			if out.ReasonCode != tc.wantReason {
				t.Errorf("ReasonCode: got %s, want %s (msg=%q)", out.ReasonCode, tc.wantReason, out.Message)
			}
			if tc.wantDSRName != "" {
				if out.DataSourceRef == nil {
					t.Errorf("DataSourceRef: got nil, want name=%q", tc.wantDSRName)
				} else if out.DataSourceRef.Name != tc.wantDSRName {
					t.Errorf("DataSourceRef.Name: got %q, want %q", out.DataSourceRef.Name, tc.wantDSRName)
				}
			} else if out.DataSourceRef != nil {
				t.Errorf("DataSourceRef: got non-nil %+v, want nil", out.DataSourceRef)
			}
		})
	}
}

func TestDecide_IdentityResolution(t *testing.T) {
	t.Run("default identity is <ns>/<pvc>", func(t *testing.T) {
		in := baseInput(optedIn(labels.TierDaily), resolved(mode.Enforce, mode.RestoreEnforce), BackupExists, CacheFresh)
		out := Decide(in)
		if out.BackupIdentity != "myapp/data" {
			t.Errorf("BackupIdentity: got %q, want %q", out.BackupIdentity, "myapp/data")
		}
	})
	t.Run("override sets opaque identity", func(t *testing.T) {
		spec := optedIn(labels.TierDaily)
		spec.BackupIdentity = "immich-library"
		in := baseInput(spec, resolved(mode.Enforce, mode.RestoreEnforce), BackupExists, CacheFresh)
		out := Decide(in)
		if out.BackupIdentity != "immich-library" {
			t.Errorf("BackupIdentity: got %q, want %q", out.BackupIdentity, "immich-library")
		}
	})
}

func TestDecide_NamesAndDataSourceRef(t *testing.T) {
	// Bare-dst convention: RS=<pvc>, RD=<pvc>-dst
	in := baseInput(optedIn(labels.TierDaily), resolved(mode.Enforce, mode.RestoreEnforce), BackupExists, CacheFresh)
	in.PVCName = testPVCLibrary
	out := Decide(in)
	if out.Names.RS != "library" {
		t.Errorf("Names.RS: got %q, want %q", out.Names.RS, "library")
	}
	if out.Names.RD != "library-dst" {
		t.Errorf("Names.RD: got %q, want %q", out.Names.RD, "library-dst")
	}
	if out.DataSourceRef == nil || out.DataSourceRef.Name != "library-dst" {
		t.Errorf("DataSourceRef.Name: got %+v, want library-dst", out.DataSourceRef)
	}
	if out.DataSourceRef.APIGroup != VolSyncAPIGroup || out.DataSourceRef.Kind != VolSyncRDKind {
		t.Errorf("DataSourceRef apiGroup/kind: got %s/%s", out.DataSourceRef.APIGroup, out.DataSourceRef.Kind)
	}
}

func TestDecide_LegacyNamingStrategy(t *testing.T) {
	// When the operator is configured with StrategyLegacyBackup (for Phase 6
	// adoption of v3-operator-era orphans), RS and RD both end in "-backup".
	in := baseInput(optedIn(labels.TierDaily), resolved(mode.Enforce, mode.RestoreEnforce), BackupExists, CacheFresh)
	in.PVCName = testPVCLibrary
	in.Config.NamingStrategy = naming.StrategyLegacyBackup
	out := Decide(in)
	if out.Names.RS != testPVCLibraryBk {
		t.Errorf("Names.RS (legacy): got %q, want %q", out.Names.RS, "library-backup")
	}
	if out.Names.RD != testPVCLibraryBk {
		t.Errorf("Names.RD (legacy): got %q, want %q", out.Names.RD, "library-backup")
	}
	if out.DataSourceRef.Name != testPVCLibraryBk {
		t.Errorf("DataSourceRef.Name (legacy): got %q", out.DataSourceRef.Name)
	}
}

func TestDecide_AuditOverridePreservesContext(t *testing.T) {
	// In audit mode, BackupUnknown → admit + WouldDeny event.
	in := baseInput(optedIn(labels.TierDaily), resolved(mode.Audit, mode.RestoreAudit), BackupUnknown, CacheFreshnessUnknown)
	out := Decide(in)
	if !out.Admit {
		t.Fatal("audit override should admit; got Admit=false")
	}
	// Audit mode reaches the BackupUnknown branch which directly emits the
	// permissive warn path (no deny was ever set); ReasonCode is
	// ReasonAllowedPermissiveWarn for audit. Test that an event was emitted.
	foundWarn := false
	for _, ev := range out.Events {
		if ev.Reason == "BackupStateUnknown" {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Errorf("expected BackupStateUnknown event in audit/unknown branch; events: %+v", out.Events)
	}
}

func TestDecide_AuditOverride_OnHardDeny(t *testing.T) {
	// A skip-restore-without-reason WOULD deny; under audit mode it admits
	// but emits a WouldDeny event with the original reason in the message.
	spec := optedIn(labels.TierDaily)
	spec.SkipRestore = true
	in := baseInput(spec, resolved(mode.Audit, mode.RestoreAudit), BackupExists, CacheFresh)
	out := Decide(in)
	if !out.Admit {
		t.Fatal("audit override should admit even on skip-restore-without-reason; got Admit=false")
	}
	if out.Mutate {
		t.Error("audit override should not mutate")
	}
	foundWouldDeny := false
	for _, ev := range out.Events {
		if ev.Reason == "WouldDeny" && strings.Contains(ev.Message, "DeniedSkipRestoreMissingReason") {
			foundWouldDeny = true
		}
	}
	if !foundWouldDeny {
		t.Errorf("expected WouldDeny event referencing DeniedSkipRestoreMissingReason; events: %+v", out.Events)
	}
	if out.ReasonCode != ReasonDeniedSkipRestoreMissingReason {
		t.Errorf("ReasonCode preserved for traceability: got %s, want %s", out.ReasonCode, ReasonDeniedSkipRestoreMissingReason)
	}
}

func TestDecide_ParseErrorsAggregated(t *testing.T) {
	spec := optedIn(labels.TierUnspecified)
	spec.Errors = []error{errors.New("tier: invalid"), errors.New("UID: out of range")}
	in := baseInput(spec, resolved(mode.Permissive, mode.RestorePermissive), BackupExists, CacheFresh)
	out := Decide(in)
	if len(out.ParseErrors) != 2 {
		t.Errorf("ParseErrors: got %d, want 2 (%v)", len(out.ParseErrors), out.ParseErrors)
	}
}

func TestDecide_SelfReferenceNotDuplicate(t *testing.T) {
	in := baseInput(optedIn(labels.TierDaily), resolved(mode.Strict, mode.RestoreStrict), BackupExists, CacheFresh)
	// Same namespace + same PVC name in KnownIdentities → must NOT trigger dup.
	in.KnownIdentities = []IdentityRef{
		{Namespace: in.Namespace, PVCName: in.PVCName, Identity: testIdMyappData},
	}
	out := Decide(in)
	if !out.Admit {
		t.Fatal("self-reference should not deny")
	}
	if out.ReasonCode == ReasonDeniedDuplicateIdentityStrict {
		t.Errorf("self-reference falsely triggered duplicate-identity deny")
	}
}
