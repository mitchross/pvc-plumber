package mode

import (
	"strings"
	"testing"
)

func TestParseMode(t *testing.T) {
	cases := []struct {
		in      string
		want    Mode
		wantErr bool
	}{
		{"", Unspecified, false},
		{"audit", Audit, false},
		{"Audit", Audit, false},
		{"PERMISSIVE", Permissive, false},
		{"  enforce ", Enforce, false},
		{"strict", Strict, false},
		{"never", Unspecified, true}, // never is a RestoreMode, not a Mode
		{"force", Unspecified, true}, // force is a RestoreMode, not a Mode
		{"garbage", Unspecified, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseMode(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err: got %v, wantErr=%v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("Mode: got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseRestoreMode(t *testing.T) {
	cases := []struct {
		in      string
		want    RestoreMode
		wantErr bool
	}{
		{"", RestoreUnspecified, false},
		{"audit", RestoreAudit, false},
		{"permissive", RestorePermissive, false},
		{"enforce", RestoreEnforce, false},
		{"strict", RestoreStrict, false},
		{"never", RestoreNever, false},
		{"force", RestoreForce, false},
		{"Force", RestoreForce, false},
		{"garbage", RestoreUnspecified, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseRestoreMode(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err: got %v, wantErr=%v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("RestoreMode: got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestResolve_Precedence(t *testing.T) {
	cases := []struct {
		name           string
		in             PrecedenceInputs
		wantMode       Mode
		wantModeSrc    Source
		wantRestore    RestoreMode
		wantRestoreSrc Source
		wantErr        bool
	}{
		{
			name: "all-unset → default",
			in: PrecedenceInputs{
				Default:        Audit,
				DefaultRestore: RestoreAudit,
			},
			wantMode:       Audit,
			wantModeSrc:    SourceDefault,
			wantRestore:    RestoreAudit,
			wantRestoreSrc: SourceDefault,
		},
		{
			name: "global overrides default",
			in: PrecedenceInputs{
				GlobalMode:     Permissive,
				GlobalRestore:  RestorePermissive,
				Default:        Audit,
				DefaultRestore: RestoreAudit,
			},
			wantMode:       Permissive,
			wantModeSrc:    SourceGlobal,
			wantRestore:    RestorePermissive,
			wantRestoreSrc: SourceGlobal,
		},
		{
			name: "namespace overrides global",
			in: PrecedenceInputs{
				NamespaceMode:        "enforce",
				NamespaceRestoreMode: "never",
				GlobalMode:           Permissive,
				GlobalRestore:        RestorePermissive,
				Default:              Audit,
				DefaultRestore:       RestoreAudit,
			},
			wantMode:       Enforce,
			wantModeSrc:    SourceNamespace,
			wantRestore:    RestoreNever,
			wantRestoreSrc: SourceNamespace,
		},
		{
			name: "PVC annotation overrides all",
			in: PrecedenceInputs{
				PVCMode:              "strict",
				PVCRestoreMode:       "force",
				NamespaceMode:        "enforce",
				NamespaceRestoreMode: "never",
				GlobalMode:           Permissive,
				GlobalRestore:        RestorePermissive,
				Default:              Audit,
				DefaultRestore:       RestoreAudit,
			},
			wantMode:       Strict,
			wantModeSrc:    SourcePVCAnnotation,
			wantRestore:    RestoreForce,
			wantRestoreSrc: SourcePVCAnnotation,
		},
		{
			name: "PVC overrides Mode but not RestoreMode (independent)",
			in: PrecedenceInputs{
				PVCMode:        "strict",
				GlobalMode:     Permissive,
				GlobalRestore:  RestorePermissive,
				Default:        Audit,
				DefaultRestore: RestoreAudit,
			},
			wantMode:       Strict,
			wantModeSrc:    SourcePVCAnnotation,
			wantRestore:    RestorePermissive,
			wantRestoreSrc: SourceGlobal,
		},
		{
			name: "malformed PVC mode → falls through, error returned, no crash",
			in: PrecedenceInputs{
				PVCMode:    "garbage",
				GlobalMode: Permissive,
				Default:    Audit,
			},
			wantMode:    Permissive, // fell back to global
			wantModeSrc: SourceGlobal,
			wantErr:     true,
		},
		{
			name: "malformed namespace mode + valid PVC → PVC wins, error returned",
			in: PrecedenceInputs{
				PVCMode:       "enforce",
				NamespaceMode: "garbage",
				GlobalMode:    Permissive,
				Default:       Audit,
			},
			wantMode:    Enforce,
			wantModeSrc: SourcePVCAnnotation,
			wantErr:     true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Resolve(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err: got %v, wantErr=%v", err, tc.wantErr)
			}
			if got.Mode != tc.wantMode {
				t.Errorf("Mode: got %v, want %v", got.Mode, tc.wantMode)
			}
			if got.ModeSource != tc.wantModeSrc {
				t.Errorf("ModeSource: got %v, want %v", got.ModeSource, tc.wantModeSrc)
			}
			if got.Restore != tc.wantRestore {
				t.Errorf("Restore: got %v, want %v", got.Restore, tc.wantRestore)
			}
			if got.RestoreSource != tc.wantRestoreSrc {
				t.Errorf("RestoreSource: got %v, want %v", got.RestoreSource, tc.wantRestoreSrc)
			}
		})
	}
}

func TestSafeBootstrapDefaults(t *testing.T) {
	m, r := SafeBootstrapDefaults()
	if m != Audit {
		t.Errorf("bootstrap mode: got %v, want Audit", m)
	}
	if r != RestoreAudit {
		t.Errorf("bootstrap restore: got %v, want RestoreAudit", r)
	}
}

func TestModePredicates(t *testing.T) {
	cases := []struct {
		mode                                        Mode
		deniesUnknown, deniesStale, mutates, writes bool
	}{
		{Audit, false, false, false, false},
		{Permissive, false, false, true, true},
		{Enforce, false, false, true, true},
		{Strict, true, true, true, true},
		{Unspecified, false, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.mode.String(), func(t *testing.T) {
			if got := tc.mode.DeniesUnknown(); got != tc.deniesUnknown {
				t.Errorf("DeniesUnknown: got %v, want %v", got, tc.deniesUnknown)
			}
			if got := tc.mode.DeniesStale(); got != tc.deniesStale {
				t.Errorf("DeniesStale: got %v, want %v", got, tc.deniesStale)
			}
			if got := tc.mode.MutatesOnExists(); got != tc.mutates {
				t.Errorf("MutatesOnExists: got %v, want %v", got, tc.mutates)
			}
			if got := tc.mode.WritesResources(); got != tc.writes {
				t.Errorf("WritesResources: got %v, want %v", got, tc.writes)
			}
		})
	}
}

func TestMultiError(t *testing.T) {
	in := PrecedenceInputs{
		PVCMode:       "garbage",
		NamespaceMode: "also-garbage",
		Default:       Audit,
	}
	_, err := Resolve(in)
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "namespace mode") || !strings.Contains(msg, "pvc mode") {
		t.Errorf("joined error message lost detail: %s", msg)
	}
}
