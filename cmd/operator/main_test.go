package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/mitchross/pvc-plumber/internal/config"
	"github.com/mitchross/pvc-plumber/internal/controller"
	"github.com/mitchross/pvc-plumber/internal/v4/mode"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// testStagingNS is the example site-local namespace used across the additive-
// behavior subtests. Realistic shape (lowercase, dashed) is enough to catch
// the trim/split cases without needing variety per case.
const testStagingNS = "staging-infra"

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
			raw:       testStagingNS,
			wantExtra: []string{testStagingNS},
		},
		{
			name:      "multiple extras add to defaults",
			raw:       testStagingNS + ",prod-tools,monitoring",
			wantExtra: []string{testStagingNS, "prod-tools", "monitoring"},
		},
		{
			name:      "extras with whitespace are trimmed",
			raw:       " " + testStagingNS + " , prod-tools ,  ,monitoring",
			wantExtra: []string{testStagingNS, "prod-tools", "monitoring"},
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

// TestReconcilerKindFor pins the reconciler selection contract for
// every mode. Audit mode runs the v4 audit reconciler exclusively; all
// other modes keep the existing v3 path. A future mode addition (e.g.
// "dry-run-strict") will fail this test by routing to "v3" and force
// an explicit decision about which reconciler should run there.
func TestReconcilerKindFor(t *testing.T) {
	cases := []struct {
		name string
		m    mode.Mode
		want string
	}{
		{"audit → v4-audit", mode.Audit, reconcilerKindV4Audit},
		{"permissive → v3", mode.Permissive, reconcilerKindV3},
		{"enforce → v3", mode.Enforce, reconcilerKindV3},
		{"strict → v3", mode.Strict, reconcilerKindV3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := reconcilerKindFor(tc.m); got != tc.want {
				t.Errorf("reconcilerKindFor(%s) = %q, want %q", tc.m.String(), got, tc.want)
			}
		})
	}
}

// TestReconcilerKindFor_OnlyAuditRoutesToV4 is the inverse guard: the
// v4 reconciler MUST NOT be selected for any mode other than audit. If
// a future refactor accidentally routes permissive/enforce/strict into
// the v4 path, this test catches it.
func TestReconcilerKindFor_OnlyAuditRoutesToV4(t *testing.T) {
	for _, m := range []mode.Mode{mode.Permissive, mode.Enforce, mode.Strict} {
		if got := reconcilerKindFor(m); got == reconcilerKindV4Audit {
			t.Errorf("mode %q must NOT route to %s (only audit may)", m.String(), reconcilerKindV4Audit)
		}
	}
}

// =============================================================================
// newAuditHTTPServer route surface
// =============================================================================

// emptyAuditStore is a real *controller.Store with the same config the
// production audit path uses. Empty, so /audit returns a valid report
// with zero entries — sufficient to exercise the mux.
func emptyAuditStore() *controller.Store {
	return controller.NewStore(
		mode.Audit.String(),
		naming.StrategyBareDst.String(),
		naming.DefaultRepoSecretName,
	)
}

func testCfgPort() *config.Config {
	return &config.Config{Port: "8080"}
}

func TestNewAuditHTTPServer_RoutesAuditEndpoint(t *testing.T) {
	srv := newAuditHTTPServer(testCfgPort(), emptyAuditStore(), slog.New(slog.DiscardHandler))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("/audit status: got %d, want 200", rr.Code)
	}
	if got := rr.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("/audit Content-Type: got %q, want application/json", got)
	}
	var report controller.ParityReport
	if err := json.Unmarshal(rr.Body.Bytes(), &report); err != nil {
		t.Fatalf("/audit body did not decode as ParityReport: %v\nbody: %s", err, rr.Body.String())
	}
	if report.OperatorMode != mode.Audit.String() {
		t.Errorf("/audit report.OperatorMode: got %q, want %q", report.OperatorMode, mode.Audit.String())
	}
	if report.NamingStrategy != naming.StrategyBareDst.String() {
		t.Errorf("/audit report.NamingStrategy: got %q, want %q", report.NamingStrategy, naming.StrategyBareDst.String())
	}
	if report.DefaultRepoSecret != naming.DefaultRepoSecretName {
		t.Errorf("/audit report.DefaultRepoSecret: got %q, want %q", report.DefaultRepoSecret, naming.DefaultRepoSecretName)
	}
}

func TestNewAuditHTTPServer_RoutesHealthz(t *testing.T) {
	srv := newAuditHTTPServer(testCfgPort(), emptyAuditStore(), slog.New(slog.DiscardHandler))

	for _, path := range []string{"/healthz", "/readyz"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
			rr := httptest.NewRecorder()
			srv.Handler.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Errorf("%s status: got %d, want 200", path, rr.Code)
			}
			if got := rr.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("%s Content-Type: got %q, want application/json", path, got)
			}
			var body map[string]string
			if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
				t.Fatalf("%s body did not decode: %v", path, err)
			}
			if body["status"] != "ok" {
				t.Errorf("%s body status: got %q, want %q", path, body["status"], "ok")
			}
		})
	}
}

// /exists must NOT be mounted on the audit-mode HTTP server. The legacy
// endpoint requires a backend, which audit mode does not initialize;
// surfacing /exists would either crash or return misleading 503s.
func TestNewAuditHTTPServer_DoesNotMountLegacyExists(t *testing.T) {
	srv := newAuditHTTPServer(testCfgPort(), emptyAuditStore(), slog.New(slog.DiscardHandler))

	// http.ServeMux returns 404 for any unmounted path. /exists/ is the
	// legacy prefix; /exists/<ns>/<pvc> would route through it if
	// mounted.
	for _, path := range []string{"/exists", "/exists/", "/exists/myapp/data"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
			rr := httptest.NewRecorder()
			srv.Handler.ServeHTTP(rr, req)
			if rr.Code != http.StatusNotFound {
				t.Errorf("%s status: got %d, want 404 (legacy /exists must not be mounted)", path, rr.Code)
			}
		})
	}
}

// /metrics is intentionally NOT mounted on the audit-mode HTTP server.
// The controller-runtime manager exposes its own /metrics on
// metricsAddr. Mounting a second /metrics here would risk Prometheus
// scrape duplication.
func TestNewAuditHTTPServer_DoesNotMountMetrics(t *testing.T) {
	srv := newAuditHTTPServer(testCfgPort(), emptyAuditStore(), slog.New(slog.DiscardHandler))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("/metrics status: got %d, want 404 (manager owns /metrics in audit mode)", rr.Code)
	}
}

// /audit must reject non-GET/HEAD methods even when mounted via this
// mux. The handler-level test covers this directly; this is a sanity
// check that the mux registration didn't accidentally restrict methods.
func TestNewAuditHTTPServer_AuditEndpointRejectsPost(t *testing.T) {
	srv := newAuditHTTPServer(testCfgPort(), emptyAuditStore(), slog.New(slog.DiscardHandler))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/audit", nil)
	rr := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /audit status: got %d, want 405", rr.Code)
	}
	if got := rr.Header().Get("Allow"); got != "GET, HEAD" {
		t.Errorf("POST /audit Allow header: got %q, want %q", got, "GET, HEAD")
	}
}

// The audit HTTP server binds to cfg.Port — same as the legacy HTTP
// server in non-audit modes — so kubelet probes don't have to know
// which mode the pod is running in.
func TestNewAuditHTTPServer_BindsCfgPort(t *testing.T) {
	cfg := &config.Config{Port: "12345"}
	srv := newAuditHTTPServer(cfg, emptyAuditStore(), slog.New(slog.DiscardHandler))

	if srv.Addr != ":12345" {
		t.Errorf("audit server Addr: got %q, want :12345 (must follow cfg.Port)", srv.Addr)
	}
}

// =============================================================================
// Module-graph guard: audit-mode wiring stays backend-independent.
// =============================================================================

// TestNewAuditHTTPServer_NoBackendNeeded constructs the audit HTTP
// server with a nil cfg-derived backend config and a brand-new empty
// store. If the audit path ever silently grows a dependency on the
// backend bundle, the construction will start requiring it (or
// panicking on a nil deref), and this test breaks.
//
// In production main() never calls buildBackend in audit mode, so
// passing a backend-free cfg is faithful to the running shape.
func TestNewAuditHTTPServer_NoBackendNeeded(t *testing.T) {
	// Explicitly zero out backend-related fields. If newAuditHTTPServer
	// or anything downstream reaches for them, it'll panic or misbehave.
	cfg := &config.Config{
		Port: "8080",
		// All other fields intentionally zero.
	}
	srv := newAuditHTTPServer(cfg, emptyAuditStore(), slog.New(slog.DiscardHandler))

	if srv == nil {
		t.Fatal("newAuditHTTPServer returned nil with backend-free config")
	}
	if srv.Handler == nil {
		t.Fatal("audit server has nil Handler with backend-free config")
	}
	// Exercise the audit handler too to ensure the store->handler->
	// JSON path doesn't reach for a backend.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("backend-free /audit status: got %d, want 200", rr.Code)
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
