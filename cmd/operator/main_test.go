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
	"github.com/mitchross/pvc-plumber/internal/v4/runtimeconfig"
)

// testStagingNS is the example site-local namespace used across the additive-
// behavior subtests. Realistic shape (lowercase, dashed) is enough to catch
// the trim/split cases without needing variety per case.
const testStagingNS = "staging-infra"

// Test-scope constants for Patch 6.8a permissive-defaults assertions.
// Mirrors the runtimeconfig package's test constants so the operator
// test can use the same canonical values without exporting them.
const (
	testMainSnapshotClass = "longhorn-snapclass"
	testMainCacheCapacity = "2Gi"
	testMainStorageClass  = "longhorn"
	testMainKubeSystemNS  = "kube-system"
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
// every mode. As of Patch 6.7-wire, audit AND permissive both route
// to the v4 reconciler; enforce/strict are rejected at startup by
// validateMode and never reach this predicate, but their defensive
// fallback maps to v3. A future mode addition (e.g. "dry-run-strict")
// will fail this test and force an explicit decision about which
// reconciler should run there.
func TestReconcilerKindFor(t *testing.T) {
	cases := []struct {
		name string
		m    mode.Mode
		want string
	}{
		{"audit → v4", mode.Audit, reconcilerKindV4},
		{"permissive → v4", mode.Permissive, reconcilerKindV4},
		// Enforce/strict are rejected by validateMode before reaching
		// reconcilerKindFor in production. The defensive fallback maps
		// them to v3 so a future Phase 8 author has a clear hook —
		// these cases lock that fallback contract.
		{"enforce → v3 (rejected at startup)", mode.Enforce, reconcilerKindV3},
		{"strict → v3 (rejected at startup)", mode.Strict, reconcilerKindV3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := reconcilerKindFor(tc.m); got != tc.want {
				t.Errorf("reconcilerKindFor(%s) = %q, want %q", tc.m.String(), got, tc.want)
			}
		})
	}
}

// TestReconcilerKindFor_V4RoutingContainsAuditAndPermissive is the
// positive inverse of the old "only audit routes to v4" test. After
// Patch 6.7-wire, permissive joined audit on the v4 path; this test
// locks that membership. If a future refactor drops permissive from
// the v4 set (e.g. accidentally falls back to v3), the assertion
// fails loudly.
func TestReconcilerKindFor_V4RoutingContainsAuditAndPermissive(t *testing.T) {
	for _, m := range []mode.Mode{mode.Audit, mode.Permissive} {
		if got := reconcilerKindFor(m); got != reconcilerKindV4 {
			t.Errorf("mode %q must route to %s; got %s", m.String(), reconcilerKindV4, got)
		}
	}
}

// TestReconcilerSelection_V3NeverRunsForPermissive is the contract
// the user explicitly requested: under no circumstance may permissive
// fall through to the v3 chart-era PVCReconciler. A future refactor
// that drops the runsV4Reconciler() == true branch for permissive
// would trip this test before any cluster surface area is affected.
func TestReconcilerSelection_V3NeverRunsForPermissive(t *testing.T) {
	if got := reconcilerKindFor(mode.Permissive); got == reconcilerKindV3 {
		t.Errorf("permissive routed to v3; got %s, want %s (the entire point of Patch 6.7-wire)", got, reconcilerKindV4)
	}
}

// TestRunsV4Reconciler locks the runs-v4 predicate's contract: only
// audit + permissive return true. Used by main()'s gates for backend
// init, /audit HTTP server, and webhook skip.
func TestRunsV4Reconciler(t *testing.T) {
	cases := []struct {
		m    mode.Mode
		want bool
	}{
		{mode.Audit, true},
		{mode.Permissive, true},
		{mode.Enforce, false},
		{mode.Strict, false},
	}
	for _, tc := range cases {
		t.Run(tc.m.String(), func(t *testing.T) {
			if got := runsV4Reconciler(tc.m); got != tc.want {
				t.Errorf("runsV4Reconciler(%s) = %v, want %v", tc.m.String(), got, tc.want)
			}
		})
	}
}

// TestNeedsBackend locks the needs-backend predicate: returns true
// only for modes that would run the v3 chart-era reconciler. Today
// this is the inverse of runsV4Reconciler; the two predicates are
// kept separate so a future v4 component that needs a backend hook
// can decouple them without rewriting the routing logic.
func TestNeedsBackend(t *testing.T) {
	cases := []struct {
		m    mode.Mode
		want bool
	}{
		{mode.Audit, false},
		{mode.Permissive, false},
		{mode.Enforce, true},
		{mode.Strict, true},
	}
	for _, tc := range cases {
		t.Run(tc.m.String(), func(t *testing.T) {
			if got := needsBackend(tc.m); got != tc.want {
				t.Errorf("needsBackend(%s) = %v, want %v", tc.m.String(), got, tc.want)
			}
		})
	}
}

// TestPredicateInvariant_V4AndBackendAreOpposite is a paranoia check.
// Today runsV4Reconciler == !needsBackend by construction. If a future
// change accidentally double-negates one without the other, the
// invariant breaks and this test catches it before the operator
// binary ships with a torn routing contract.
func TestPredicateInvariant_V4AndBackendAreOpposite(t *testing.T) {
	for _, m := range []mode.Mode{mode.Audit, mode.Permissive, mode.Enforce, mode.Strict} {
		if runsV4Reconciler(m) == needsBackend(m) {
			t.Errorf("runsV4Reconciler(%s) and needsBackend(%s) must be opposite; both returned %v",
				m.String(), m.String(), runsV4Reconciler(m))
		}
	}
}

// TestValidateMode locks the fail-fast contract for enforce/strict.
// Audit + permissive must pass; enforce + strict must return an
// error mentioning Phase 8 (so the operator who set the env var
// gets an actionable signal in the pod log). The exact wording is
// part of the contract — log scrapers can match on "Phase 8" to
// alert on misconfigured deployments.
func TestValidateMode(t *testing.T) {
	cases := []struct {
		name           string
		m              mode.Mode
		wantErr        bool
		wantContainStr string
	}{
		{"audit accepted", mode.Audit, false, ""},
		{"permissive accepted", mode.Permissive, false, ""},
		{"enforce rejected with Phase 8 message", mode.Enforce, true, "Phase 8"},
		{"strict rejected with Phase 8 message", mode.Strict, true, "Phase 8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMode(tc.m)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("validateMode(%s) = nil; want error mentioning %q", tc.m.String(), tc.wantContainStr)
				}
				if !strings.Contains(err.Error(), tc.wantContainStr) {
					t.Errorf("validateMode(%s) error = %q; want substring %q", tc.m.String(), err.Error(), tc.wantContainStr)
				}
				// The error MUST also mention the mode string so an
				// operator reading the pod log can tell which mode
				// was rejected.
				if !strings.Contains(err.Error(), tc.m.String()) {
					t.Errorf("validateMode(%s) error = %q; want substring %q (operator must see which mode was rejected)", tc.m.String(), err.Error(), tc.m.String())
				}
			} else if err != nil {
				t.Errorf("validateMode(%s) = %v; want nil", tc.m.String(), err)
			}
		})
	}
}

// =============================================================================
// newAuditHTTPServer route surface
// =============================================================================

// emptyAuditStore is a real *controller.Store with the same config the
// production audit path uses. Empty, so /audit returns a valid report
// with zero entries — sufficient to exercise the mux.
func emptyAuditStore() *controller.Store {
	return emptyV4Store(mode.Audit)
}

// emptyV4Store is the mode-parametric variant. Tests that need to
// exercise the permissive-routed /audit endpoint use this directly so
// the report's operator_mode field reflects the running mode.
func emptyV4Store(m mode.Mode) *controller.Store {
	return controller.NewStore(
		m.String(),
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

// =============================================================================
// Patch 6.7-wire: /audit HTTP server under permissive
// =============================================================================
//
// Under permissive mode the /audit endpoint must be mounted just like
// under audit mode, with the report's operator_mode field correctly
// reflecting "permissive" so dashboards / log scrapers can tell which
// mode the binary is running in. Legacy /exists and /metrics endpoints
// must remain unmounted; the audit-mode tests above lock that for
// audit, the cases below lock it for permissive.

// TestNewV4HTTPServer_PermissiveReportsPermissiveOperatorMode confirms
// the /audit JSON's operator_mode field reads "permissive" when the
// Store is constructed from runtimeCfg.Mode=permissive (the wiring
// main() does in Patch 6.7-wire).
func TestNewV4HTTPServer_PermissiveReportsPermissiveOperatorMode(t *testing.T) {
	srv := newAuditHTTPServer(testCfgPort(), emptyV4Store(mode.Permissive), slog.New(slog.DiscardHandler))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("/audit status: got %d, want 200", rr.Code)
	}
	var report controller.ParityReport
	if err := json.Unmarshal(rr.Body.Bytes(), &report); err != nil {
		t.Fatalf("/audit body did not decode as ParityReport: %v\nbody: %s", err, rr.Body.String())
	}
	if report.OperatorMode != mode.Permissive.String() {
		t.Errorf("operator_mode: got %q, want %q (the entire point of permissive routing to v4)",
			report.OperatorMode, mode.Permissive.String())
	}
	// NamingStrategy + DefaultRepoSecret are the same defaults whether
	// audit or permissive — main() does not vary these by mode.
	if report.NamingStrategy != naming.StrategyBareDst.String() {
		t.Errorf("naming_strategy: got %q, want %q", report.NamingStrategy, naming.StrategyBareDst.String())
	}
	if report.DefaultRepoSecret != naming.DefaultRepoSecretName {
		t.Errorf("default_repo_secret: got %q, want %q", report.DefaultRepoSecret, naming.DefaultRepoSecretName)
	}
}

// TestNewV4HTTPServer_PermissiveDoesNotMountLegacyExists confirms the
// /exists endpoint stays off under permissive. v4 modes have no backend
// initialized so the legacy handler cannot work; mounting it would
// surface 503s or panics depending on how the handler is constructed.
func TestNewV4HTTPServer_PermissiveDoesNotMountLegacyExists(t *testing.T) {
	srv := newAuditHTTPServer(testCfgPort(), emptyV4Store(mode.Permissive), slog.New(slog.DiscardHandler))

	for _, path := range []string{"/exists", "/exists/", "/exists/myapp/data"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
			rr := httptest.NewRecorder()
			srv.Handler.ServeHTTP(rr, req)
			if rr.Code != http.StatusNotFound {
				t.Errorf("%s status: got %d, want 404 (legacy /exists must not be mounted in v4 modes)", path, rr.Code)
			}
		})
	}
}

// TestNewV4HTTPServer_PermissiveDoesNotMountMetrics confirms /metrics
// is not double-mounted under permissive (controller-runtime exposes
// its own /metrics on metricsAddr).
func TestNewV4HTTPServer_PermissiveDoesNotMountMetrics(t *testing.T) {
	srv := newAuditHTTPServer(testCfgPort(), emptyV4Store(mode.Permissive), slog.New(slog.DiscardHandler))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("/metrics status: got %d, want 404 (manager owns /metrics)", rr.Code)
	}
}

// TestNewV4HTTPServer_PermissiveBackendIndependent extends the
// backend-free constructor test to permissive. If main() ever wires
// a backend dependency into the v4 HTTP server for permissive (e.g.
// by passing a non-nil bundle into newAuditHTTPServer), this test
// catches the regression: the audit server takes only a Store, no
// backend.
func TestNewV4HTTPServer_PermissiveBackendIndependent(t *testing.T) {
	cfg := &config.Config{Port: "8080"} // all backend fields zero
	srv := newAuditHTTPServer(cfg, emptyV4Store(mode.Permissive), slog.New(slog.DiscardHandler))

	if srv == nil {
		t.Fatal("newAuditHTTPServer returned nil with backend-free permissive config")
	}
	if srv.Handler == nil {
		t.Fatal("permissive v4 server has nil Handler")
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("backend-free permissive /audit status: got %d, want 200", rr.Code)
	}
}

// =============================================================================
// Patch 6.8a: V4 builder defaults flow from runtimeconfig into the reconciler
// =============================================================================

// int64TestPtr is the test-local equivalent of runtimeconfig.int64Ptr
// (the runtimeconfig package's helper is private). Used to build
// runtimeconfig.Config fixtures inline.
func int64TestPtr(v int64) *int64 { return &v }

// TestInt64OrZero locks the nil-deref behavior of the small helper
// that bridges runtimeconfig.Config (*int64) into the reconciler's
// flat int64 fields. Trivial logic, but explicit so a future refactor
// that changes the helper to "return -1 for nil" trips a clear test.
func TestInt64OrZero(t *testing.T) {
	if got := int64OrZero(nil); got != 0 {
		t.Errorf("int64OrZero(nil) = %d, want 0", got)
	}
	v := int64(568)
	if got := int64OrZero(&v); got != 568 {
		t.Errorf("int64OrZero(&568) = %d, want 568", got)
	}
	zero := int64(0)
	if got := int64OrZero(&zero); got != 0 {
		t.Errorf("int64OrZero(&0) = %d, want 0", got)
	}
	neg := int64(-1)
	if got := int64OrZero(&neg); got != -1 {
		// Validator rejects negative at parse; helper itself passes
		// through. Locking the pass-through behavior.
		t.Errorf("int64OrZero(&-1) = %d, want -1 (validation belongs elsewhere)", got)
	}
}

// TestNewV4Reconciler_PassesPermissiveDefaults is the core
// contract-locking test for Patch 6.8a: given a runtimeconfig.Config
// with permissive-mode defaults set, the constructed reconciler's
// Default* fields match verbatim. Without this test, a typo in the
// factory's field assignments would silently render karakeep RS/RD
// with the wrong UID/storage class/snapshot class.
func TestNewV4Reconciler_PassesPermissiveDefaults(t *testing.T) {
	cfg := runtimeconfig.Config{
		Mode:                 mode.Permissive,
		DefaultSnapshotClass: testMainSnapshotClass,
		DefaultCacheCapacity: testMainCacheCapacity,
		DefaultStorageClass:  testMainStorageClass,
		DefaultUID:           int64TestPtr(568),
		DefaultGID:           int64TestPtr(568),
		DefaultFSGroup:       int64TestPtr(568),
	}
	sysNs := map[string]struct{}{testMainKubeSystemNS: {}}
	store := emptyV4Store(mode.Permissive)

	r := newV4Reconciler(nil, store, sysNs, cfg)
	if r == nil {
		t.Fatal("newV4Reconciler returned nil")
	}
	if r.Mode != mode.Permissive {
		t.Errorf("Mode: got %v, want %v", r.Mode, mode.Permissive)
	}
	if r.OperatorMode != mode.Permissive.String() {
		t.Errorf("OperatorMode: got %q, want %q", r.OperatorMode, mode.Permissive.String())
	}
	if r.NamingStrategy != naming.StrategyBareDst {
		t.Errorf("NamingStrategy: got %v, want %v", r.NamingStrategy, naming.StrategyBareDst)
	}
	if r.DefaultRepoSecret != naming.DefaultRepoSecretName {
		t.Errorf("DefaultRepoSecret: got %q, want %q", r.DefaultRepoSecret, naming.DefaultRepoSecretName)
	}
	if r.Store != store {
		t.Error("Store reference: factory must pass through the caller's store, not allocate a new one")
	}
	if r.DefaultSnapshotClass != testMainSnapshotClass {
		t.Errorf("DefaultSnapshotClass: got %q, want %s", r.DefaultSnapshotClass, testMainSnapshotClass)
	}
	if r.DefaultCacheCapacity != testMainCacheCapacity {
		t.Errorf("DefaultCacheCapacity: got %q, want %s", r.DefaultCacheCapacity, testMainCacheCapacity)
	}
	if r.DefaultStorageClass != testMainStorageClass {
		t.Errorf("DefaultStorageClass: got %q, want %s", r.DefaultStorageClass, testMainStorageClass)
	}
	if r.DefaultUID != 568 {
		t.Errorf("DefaultUID: got %d, want 568", r.DefaultUID)
	}
	if r.DefaultGID != 568 {
		t.Errorf("DefaultGID: got %d, want 568", r.DefaultGID)
	}
	if r.DefaultFSGroup != 568 {
		t.Errorf("DefaultFSGroup: got %d, want 568", r.DefaultFSGroup)
	}
	if _, ok := r.SystemNamespaces[testMainKubeSystemNS]; !ok {
		t.Error("SystemNamespaces: kube-system missing from factory output")
	}
}

// TestNewV4Reconciler_AuditWithUnsetDefaults_RendersZeros mirrors the
// audit-mode invariant: nil pointers in runtimeconfig.Config flatten to
// 0 on the reconciler. The executor short-circuits in audit so these
// zeros never leave the process, but the contract that nil pointer →
// 0 int64 is locked here for the audit path.
func TestNewV4Reconciler_AuditWithUnsetDefaults_RendersZeros(t *testing.T) {
	cfg := runtimeconfig.Config{Mode: mode.Audit} // all default fields nil/empty
	r := newV4Reconciler(nil, emptyV4Store(mode.Audit), nil, cfg)

	if r.Mode != mode.Audit {
		t.Errorf("Mode: got %v, want audit", r.Mode)
	}
	if r.DefaultSnapshotClass != "" {
		t.Errorf("DefaultSnapshotClass: got %q, want empty", r.DefaultSnapshotClass)
	}
	if r.DefaultUID != 0 {
		t.Errorf("DefaultUID: got %d, want 0 (nil pointer derefs to 0)", r.DefaultUID)
	}
	if r.DefaultGID != 0 {
		t.Errorf("DefaultGID: got %d, want 0", r.DefaultGID)
	}
	if r.DefaultFSGroup != 0 {
		t.Errorf("DefaultFSGroup: got %d, want 0", r.DefaultFSGroup)
	}
}

// TestNewV4Reconciler_PartialDefaults_AuditAllowed: audit-mode
// reconcilers can be constructed with any combination of set/unset
// defaults. (Permissive's hard-validation is upstream in
// RequireV4WriteDefaults, not in the factory.) This test pins that
// the factory does not double-validate.
func TestNewV4Reconciler_PartialDefaults_AuditAllowed(t *testing.T) {
	cfg := runtimeconfig.Config{
		Mode:                 mode.Audit,
		DefaultSnapshotClass: testMainSnapshotClass, // only one field set
	}
	r := newV4Reconciler(nil, emptyV4Store(mode.Audit), nil, cfg)
	if r == nil {
		t.Fatal("factory rejected partial-defaults audit config; must accept")
	}
	if r.DefaultSnapshotClass != testMainSnapshotClass {
		t.Errorf("DefaultSnapshotClass: got %q, want %s", r.DefaultSnapshotClass, testMainSnapshotClass)
	}
	if r.DefaultStorageClass != "" {
		t.Errorf("DefaultStorageClass: got %q, want empty (audit tolerates unset)", r.DefaultStorageClass)
	}
}

// =============================================================================
// Patch 6.8a: RequireV4WriteDefaults integration smoke test
// =============================================================================
//
// runtimeconfig.RequireV4WriteDefaults has its own exhaustive unit
// tests in internal/v4/runtimeconfig/config_test.go. The integration
// test below proves only that main()'s contract — "permissive
// requires the six env vars; audit doesn't" — is wired correctly at
// the boundary. If a future refactor inverts the wiring (e.g. calls
// the validator in audit mode), this test fails before any cluster
// deployment.

// TestMainIntegration_RequireV4WriteDefaults_AuditNeverErrors mirrors
// the runtimeconfig audit case but called as the binary would: import
// the package, run the validator on an audit Config, assert no error.
func TestMainIntegration_RequireV4WriteDefaults_AuditNeverErrors(t *testing.T) {
	cfg := runtimeconfig.Config{Mode: mode.Audit} // every default unset
	if err := runtimeconfig.RequireV4WriteDefaults(cfg); err != nil {
		t.Errorf("RequireV4WriteDefaults(audit) = %v, want nil (audit tolerates missing defaults)", err)
	}
}

// TestMainIntegration_RequireV4WriteDefaults_PermissiveAllSetOK locks
// the happy path the karakeep canary deployment will take.
func TestMainIntegration_RequireV4WriteDefaults_PermissiveAllSetOK(t *testing.T) {
	cfg := runtimeconfig.Config{
		Mode:                 mode.Permissive,
		DefaultSnapshotClass: testMainSnapshotClass,
		DefaultCacheCapacity: testMainCacheCapacity,
		DefaultStorageClass:  testMainStorageClass,
		DefaultUID:           int64TestPtr(568),
		DefaultGID:           int64TestPtr(568),
		DefaultFSGroup:       int64TestPtr(568),
	}
	if err := runtimeconfig.RequireV4WriteDefaults(cfg); err != nil {
		t.Errorf("RequireV4WriteDefaults(permissive, all set) = %v, want nil", err)
	}
}

// TestMainIntegration_RequireV4WriteDefaults_PermissiveMissingAny is
// the load-bearing case for the canary: missing any default must
// produce an error referencing the specific env var so the operator's
// log line is actionable.
func TestMainIntegration_RequireV4WriteDefaults_PermissiveMissingAny(t *testing.T) {
	cfg := runtimeconfig.Config{Mode: mode.Permissive} // every default unset
	err := runtimeconfig.RequireV4WriteDefaults(cfg)
	if err == nil {
		t.Fatal("permissive with no defaults: got nil, want error")
	}
	// All six env var names must appear in the composite error.
	for _, key := range []string{
		runtimeconfig.EnvDefaultSnapshotClass,
		runtimeconfig.EnvDefaultCacheCapacity,
		runtimeconfig.EnvDefaultStorageClass,
		runtimeconfig.EnvDefaultUID,
		runtimeconfig.EnvDefaultGID,
		runtimeconfig.EnvDefaultFSGroup,
	} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("composite error missing %s; got %q", key, err.Error())
		}
	}
}
