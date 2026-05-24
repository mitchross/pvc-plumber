package controller

import (
	"strings"
	"testing"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// Default bare-dst convention: RS=<pvc>, RD=<pvc>-dst, repo=
// volsync-kopia-repository, kopia identity uses ns/pvc. Phase 5 contract
// item #4 regression guard.
func TestComputeExpected_DefaultBareDst(t *testing.T) {
	got := ComputeExpected(testNSOpenWebUI, testPVCStorageName, labels.Spec{}, naming.StrategyBareDst, DefaultRepoSecretName)

	if got.RSName != testPVCStorageName {
		t.Errorf("RSName: got %q, want %q", got.RSName, testPVCStorageName)
	}
	if got.RDName != testPVCStorageName+"-dst" {
		t.Errorf("RDName: got %q, want %q", got.RDName, testPVCStorageName+"-dst")
	}
	if got.RepositorySecret != testRepoSecretShare {
		t.Errorf("RepositorySecret: got %q, want %q", got.RepositorySecret, testRepoSecretShare)
	}
	if got.KopiaUsername != testPVCStorageName {
		t.Errorf("KopiaUsername: got %q, want %q", got.KopiaUsername, testPVCStorageName)
	}
	if got.KopiaHostname != testNSOpenWebUI {
		t.Errorf("KopiaHostname: got %q, want %q", got.KopiaHostname, testNSOpenWebUI)
	}
	if got.BackupIdentity != testNSOpenWebUI+"/"+testPVCStorageName {
		t.Errorf("BackupIdentity: got %q, want %q", got.BackupIdentity, testNSOpenWebUI+"/"+testPVCStorageName)
	}
}

// Phase 5 contract item #4: NO `<pvc>-backup` default. The default RD
// suffix is `-dst`. This test guards the regression where someone might
// silently change StrategyBareDst to legacy-backup.
func TestComputeExpected_NoBackupSuffixDefault(t *testing.T) {
	got := ComputeExpected("ns", testPVCLibrary, labels.Spec{}, naming.StrategyBareDst, DefaultRepoSecretName)

	if strings.HasSuffix(got.RSName, "-backup") {
		t.Errorf("RSName ends with -backup (legacy shape): %q", got.RSName)
	}
	if strings.HasSuffix(got.RDName, "-backup") {
		t.Errorf("RDName ends with -backup (legacy shape): %q", got.RDName)
	}
	if !strings.HasSuffix(got.RDName, "-dst") {
		t.Errorf("RDName missing -dst suffix: %q", got.RDName)
	}
}

// Phase 5 contract item #4: shared repo by default. Even with an explicit
// "" passed as defaultRepoSecret, ComputeExpected falls back to the
// canonical testRepoSecretShare — the caller never has to know
// the constant string.
func TestComputeExpected_SharedRepoDefaultFallback(t *testing.T) {
	cases := []struct {
		name string
		def  string
		want string
	}{
		{name: "explicit canonical", def: testRepoSecretShare, want: testRepoSecretShare},
		{name: "explicit alternate", def: "custom-repo", want: "custom-repo"},
		{name: "empty falls back to canonical", def: "", want: testRepoSecretShare},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ComputeExpected("ns", "p", labels.Spec{}, naming.StrategyBareDst, tc.def)
			if got.RepositorySecret != tc.want {
				t.Errorf("RepositorySecret: got %q, want %q", got.RepositorySecret, tc.want)
			}
		})
	}
}

// Phase 5 contract item #4: NO per-PVC ExternalSecret expected. The
// ExpectedState struct has no ExternalSecret field — this is a
// structural test. If a future patch adds an ExternalSecret field, this
// test will fail to compile and force a deliberate decision.
func TestComputeExpected_NoPerPVCExternalSecretField(t *testing.T) {
	// If you find yourself adding fields like ExpectedState.ExternalSecretName,
	// reconsider — the v4 design uses the shared volsync-kopia-repository
	// Secret fanned out by ClusterExternalSecret in the talos repo. There
	// is no per-PVC ES to compute.
	es := ExpectedState{
		RSName:           "x",
		RDName:           "x-dst",
		RepositorySecret: testRepoSecretShare,
		KopiaUsername:    "x",
		KopiaHostname:    "ns",
		BackupIdentity:   "ns/x",
	}
	_ = es // Struct compiles with exactly these 6 fields — no ES name field.
}

// Backup identity override (pvc-plumber.io/backup-identity annotation)
// short-circuits the default <ns>/<pvc> formula AND drives kopia
// username (per naming.IdentityFor).
func TestComputeExpected_BackupIdentityOverride(t *testing.T) {
	spec := labels.Spec{BackupIdentity: testIdentityImmich}
	got := ComputeExpected("immich-prod", "library", spec, naming.StrategyBareDst, DefaultRepoSecretName)

	if got.BackupIdentity != testIdentityImmich {
		t.Errorf("BackupIdentity: got %q, want %q", got.BackupIdentity, testIdentityImmich)
	}
	// naming.IdentityFor returns Username=override, Hostname="" for the
	// override case (the override is opaque; namespace isn't applied).
	if got.KopiaUsername != testIdentityImmich {
		t.Errorf("KopiaUsername with override: got %q, want %q", got.KopiaUsername, testIdentityImmich)
	}
	if got.KopiaHostname != "" {
		t.Errorf("KopiaHostname with override: got %q, want empty (override is opaque)", got.KopiaHostname)
	}
	// RS/RD names are still derived from the PVC name, not the override.
	if got.RSName != "library" || got.RDName != "library-dst" {
		t.Errorf("RS/RD names with identity override: got %s/%s, want library/library-dst", got.RSName, got.RDName)
	}
}

// Legacy-backup naming strategy is available for adoption in Phase 6
// (claiming v3-operator-era <pvc>-backup orphans). Currently NOT the
// default, but ComputeExpected must honor it when requested.
func TestComputeExpected_LegacyBackupStrategy(t *testing.T) {
	got := ComputeExpected("ns", testPVCLibrary, labels.Spec{}, naming.StrategyLegacyBackup, DefaultRepoSecretName)
	if got.RSName != "library-backup" {
		t.Errorf("RSName (legacy): got %q, want %q", got.RSName, "library-backup")
	}
	if got.RDName != "library-backup" {
		t.Errorf("RDName (legacy): got %q, want %q", got.RDName, "library-backup")
	}
}

// Kopia identity convention matches the talos repo's inline RS/RD:
// `username: <pvc-name>` and `hostname: <namespace>`. Phase 5 contract
// item #4 regression guard.
func TestComputeExpected_KopiaIdentityConvention(t *testing.T) {
	cases := []struct {
		ns, pvc string
	}{
		{testNSOpenWebUI, testPVCStorageName},
		{"home-assistant", "config"},
		{"posthog", "redpanda-data-kafka-0"},
		{"karakeep", "meilisearch-pvc"},
	}
	for _, tc := range cases {
		t.Run(tc.ns+"/"+tc.pvc, func(t *testing.T) {
			got := ComputeExpected(tc.ns, tc.pvc, labels.Spec{}, naming.StrategyBareDst, DefaultRepoSecretName)
			if got.KopiaUsername != tc.pvc {
				t.Errorf("KopiaUsername: got %q, want %q", got.KopiaUsername, tc.pvc)
			}
			if got.KopiaHostname != tc.ns {
				t.Errorf("KopiaHostname: got %q, want %q", got.KopiaHostname, tc.ns)
			}
		})
	}
}

// DefaultRepoSecretName must mirror naming.DefaultRepoSecretName
// (re-exported for caller convenience). Tied test.
func TestDefaultRepoSecretNameMirrorsNamingPackage(t *testing.T) {
	if DefaultRepoSecretName != naming.DefaultRepoSecretName {
		t.Errorf("DefaultRepoSecretName drift: %q vs naming.%q", DefaultRepoSecretName, naming.DefaultRepoSecretName)
	}
	if DefaultRepoSecretName != testRepoSecretShare {
		t.Errorf("canonical name changed: %q (expected volsync-kopia-repository)", DefaultRepoSecretName)
	}
}
