package main

import (
	"sort"
	"strings"
	"testing"
)

// TestParseSystemNamespaces_AlwaysSeedsDefaults locks in the load-bearing
// invariant from S2: the 9-entry defaultSystemNamespaces list must always be
// present in the result, regardless of what (if anything) the SYSTEM_NAMESPACES
// env var contributed. The previous implementation was replace-style and a
// `SYSTEM_NAMESPACES=staging-infra` deployment would have silently dropped
// kube-system / cert-manager / etc. from the exclusion set, allowing the
// reconciler to start managing PVCs in those namespaces — exactly the
// admission-deadlock recovery path failure mode we exclude them for.
func TestParseSystemNamespaces_AlwaysSeedsDefaults(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		wantExtra []string // entries that should be present beyond the defaults
	}{
		{
			name: "empty env keeps defaults",
			raw:  "",
		},
		{
			name: "whitespace-only env keeps defaults",
			raw:  "   \t  ",
		},
		{
			name:      "single extra ns adds without losing defaults",
			raw:       "staging-infra",
			wantExtra: []string{"staging-infra"},
		},
		{
			name:      "multiple extras add to defaults",
			raw:       "staging-infra,prod-tools,monitoring",
			wantExtra: []string{"staging-infra", "prod-tools", "monitoring"},
		},
		{
			name:      "extras with whitespace are trimmed",
			raw:       " staging-infra , prod-tools ,  ,monitoring",
			wantExtra: []string{"staging-infra", "prod-tools", "monitoring"},
		},
		{
			name: "env entry duplicating a default is harmless (set semantics)",
			raw:  "kube-system,cert-manager",
			// no extras — kube-system and cert-manager are already defaults
		},
		{
			name:      "duplicate env entries collapse",
			raw:       "extra,extra,extra",
			wantExtra: []string{"extra"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseSystemNamespaces(tc.raw)

			// Every default must always be present.
			for _, ns := range defaultSystemNamespaces {
				if _, ok := got[ns]; !ok {
					t.Errorf("default %q missing from result; raw=%q", ns, tc.raw)
				}
			}

			// Every requested extra must be present.
			for _, ns := range tc.wantExtra {
				if _, ok := got[ns]; !ok {
					t.Errorf("extra %q missing from result; raw=%q", ns, tc.raw)
				}
			}

			// Result size: 9 defaults + len(unique extras that aren't already defaults).
			expectedSize := len(defaultSystemNamespaces)
			defaultSet := map[string]struct{}{}
			for _, ns := range defaultSystemNamespaces {
				defaultSet[ns] = struct{}{}
			}
			seenExtra := map[string]struct{}{}
			for _, ns := range tc.wantExtra {
				if _, isDefault := defaultSet[ns]; isDefault {
					continue
				}
				if _, dup := seenExtra[ns]; dup {
					continue
				}
				seenExtra[ns] = struct{}{}
				expectedSize++
			}
			if len(got) != expectedSize {
				keys := make([]string, 0, len(got))
				for k := range got {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				t.Errorf("size mismatch: got %d (%s) want %d; raw=%q",
					len(got), strings.Join(keys, ","), expectedSize, tc.raw)
			}
		})
	}
}

// TestParseSystemNamespaces_DefaultsAreCanonicalNine locks the canonical list
// at exactly 9 entries. If a future change adds or removes an entry from
// defaultSystemNamespaces without updating webhooks.yaml's namespaceSelector
// in the GitOps repo, this test fails loudly and forces the author to
// reconcile both sides of the contract.
func TestParseSystemNamespaces_DefaultsAreCanonicalNine(t *testing.T) {
	want := map[string]struct{}{
		"kube-system":         {},
		"volsync-system":      {},
		"kyverno":             {},
		"argocd":              {},
		"longhorn-system":     {},
		"snapshot-controller": {},
		"cert-manager":        {},
		"external-secrets":    {},
		"1passwordconnect":    {},
	}
	if len(defaultSystemNamespaces) != len(want) {
		t.Fatalf("defaultSystemNamespaces length: got %d want %d — if you intentionally changed this, also update infrastructure/controllers/pvc-plumber/webhooks.yaml namespaceSelector",
			len(defaultSystemNamespaces), len(want))
	}
	for _, ns := range defaultSystemNamespaces {
		if _, ok := want[ns]; !ok {
			t.Errorf("unexpected default namespace %q; if intentional, update webhooks.yaml AND this test", ns)
		}
	}
}
