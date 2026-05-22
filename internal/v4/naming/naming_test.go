package naming

import (
	"strings"
	"testing"
)

func TestParseStrategy(t *testing.T) {
	cases := []struct {
		in      string
		want    Strategy
		wantErr bool
	}{
		{"", StrategyBareDst, false},
		{"bare-dst", StrategyBareDst, false},
		{"BARE-DST", StrategyBareDst, false},
		{"  legacy-backup  ", StrategyLegacyBackup, false},
		{"dst-src", StrategyDstSrc, false},
		{"made-up", StrategyBareDst, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseStrategy(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err: got %v, wantErr=%v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("Strategy: got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCompute(t *testing.T) {
	cases := []struct {
		name     string
		strategy Strategy
		pvc      string
		repo     string
		wantRS   string
		wantRD   string
	}{
		{
			name:     "bare-dst short pvc (default)",
			strategy: StrategyBareDst,
			pvc:      "storage",
			repo:     "volsync-kopia-repository",
			wantRS:   "storage",
			wantRD:   "storage-dst",
		},
		{
			name:     "bare-dst matches talos inline convention (jellyfin/config)",
			strategy: StrategyBareDst,
			pvc:      "config",
			repo:     DefaultRepoSecretName,
			wantRS:   "config",
			wantRD:   "config-dst",
		},
		{
			name:     "legacy-backup yields <pvc>-backup for both",
			strategy: StrategyLegacyBackup,
			pvc:      "library",
			repo:     DefaultRepoSecretName,
			wantRS:   "library-backup",
			wantRD:   "library-backup",
		},
		{
			name:     "dst-src (today equivalent to bare-dst)",
			strategy: StrategyDstSrc,
			pvc:      "data",
			repo:     DefaultRepoSecretName,
			wantRS:   "data",
			wantRD:   "data-dst",
		},
		{
			name:     "empty pvc returns zero value",
			strategy: StrategyBareDst,
			pvc:      "",
		},
		{
			name:     "kebab and underscore names preserved verbatim",
			strategy: StrategyBareDst,
			pvc:      "redpanda-data-kafka-0",
			repo:     DefaultRepoSecretName,
			wantRS:   "redpanda-data-kafka-0",
			wantRD:   "redpanda-data-kafka-0-dst",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Compute(tc.strategy, tc.pvc, tc.repo)
			if got.RS != tc.wantRS {
				t.Errorf("RS: got %q, want %q", got.RS, tc.wantRS)
			}
			if got.RD != tc.wantRD {
				t.Errorf("RD: got %q, want %q", got.RD, tc.wantRD)
			}
			if tc.pvc != "" && got.PVC != tc.pvc {
				t.Errorf("PVC: got %q, want %q", got.PVC, tc.pvc)
			}
			if tc.pvc != "" && got.RepoSecret != tc.repo {
				t.Errorf("RepoSecret: got %q, want %q", got.RepoSecret, tc.repo)
			}
			// Label selector value: short names should pass through; long names hash.
			if tc.pvc != "" {
				if len(tc.pvc) <= MaxLabelValueLen && got.LabelSelectorValue != tc.pvc {
					t.Errorf("LabelSelectorValue (short): got %q, want %q", got.LabelSelectorValue, tc.pvc)
				}
			}
		})
	}
}

func TestLabelSafeRef(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"short pvc passes through", "storage", "storage"},
		{"exactly 63 chars passes through", strings.Repeat("x", 63), strings.Repeat("x", 63)},
		{"long pvc hashed", strings.Repeat("a-very-long-pvc-name-", 5), ""}, // checked below
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := LabelSafeRef(tc.in)
			if len(tc.in) <= MaxLabelValueLen {
				if got != tc.in {
					t.Errorf("short input: got %q, want %q (passthrough)", got, tc.in)
				}
				return
			}
			// Long input: must be exactly "pvc-" + 24 hex.
			if len(got) != 28 {
				t.Errorf("hashed length: got %d (%q), want 28", len(got), got)
			}
			if !strings.HasPrefix(got, "pvc-") {
				t.Errorf("hashed prefix: got %q, want 'pvc-…'", got)
			}
			if len(got) > MaxLabelValueLen {
				t.Errorf("hashed exceeds label limit %d: %q", MaxLabelValueLen, got)
			}
			// Deterministic: same input → same output.
			got2 := LabelSafeRef(tc.in)
			if got != got2 {
				t.Errorf("non-deterministic: %q vs %q", got, got2)
			}
		})
	}
}

func TestIdentityFor(t *testing.T) {
	cases := []struct {
		name     string
		ns, pvc  string
		override string
		wantUser string
		wantHost string
	}{
		{
			name:     "default identity (ns/pvc convention)",
			ns:       "open-webui",
			pvc:      "storage",
			wantUser: "storage",
			wantHost: "open-webui",
		},
		{
			name:     "override pins to opaque identity",
			ns:       "immich-prod",
			pvc:      "library",
			override: "immich-library",
			wantUser: "immich-library",
			wantHost: "",
		},
		{
			name:     "empty override is treated as unset",
			ns:       "jellyfin",
			pvc:      "config",
			override: "",
			wantUser: "config",
			wantHost: "jellyfin",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := IdentityFor(tc.ns, tc.pvc, tc.override)
			if id.Username != tc.wantUser {
				t.Errorf("Username: got %q, want %q", id.Username, tc.wantUser)
			}
			if id.Hostname != tc.wantHost {
				t.Errorf("Hostname: got %q, want %q", id.Hostname, tc.wantHost)
			}
		})
	}
}
