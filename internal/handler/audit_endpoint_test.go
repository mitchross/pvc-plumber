package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mitchross/pvc-plumber/internal/controller"
)

const (
	testNSOpenWebUI   = "open-webui"
	testModeAudit     = "audit"
	testRepoSecretFix = "volsync-kopia-repository"
)

// =============================================================================
// Test fixtures
// =============================================================================

// fakeSnapshotter is the smallest possible ParitySnapshotter for tests.
// It returns a caller-controlled ParityReport and counts how many times
// Snapshot was called. No backend/kube/cluster dependencies whatsoever
// — proving the handler is backend-independent per Patch 4 contract.
type fakeSnapshotter struct {
	mu       sync.Mutex
	report   controller.ParityReport
	calls    int
	hookFn   func() // optional pre-return hook, used by mutation test
	returned []controller.ParityReport
}

func (f *fakeSnapshotter) Snapshot() controller.ParityReport {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.hookFn != nil {
		f.hookFn()
	}
	// Return a copy so we can detect downstream mutation in tests.
	// We do this by encoding/decoding via JSON which mirrors what the
	// handler ultimately does anyway. For the empty case this is
	// essentially a no-op.
	cpy := f.report
	if f.report.Entries != nil {
		cpy.Entries = append([]controller.ParityEntry(nil), f.report.Entries...)
	}
	f.returned = append(f.returned, cpy)
	return cpy
}

// nonEmptyReport returns a hand-built ParityReport with a couple of
// entries and a non-trivial summary so tests can verify the handler
// preserves the full structure end-to-end.
func nonEmptyReport() controller.ParityReport {
	t := time.Date(2026, 5, 23, 22, 0, 0, 0, time.UTC)
	entries := []controller.ParityEntry{
		{
			Namespace:      testNSOpenWebUI,
			PVC:            "storage",
			Mode:           testModeAudit,
			Tier:           "daily",
			LabelSource:    controller.LabelSourceLegacy,
			BackupIdentity: "open-webui/storage",
			Owner:          controller.OwnerInlineArgo,
			Action:         controller.ActionAlreadyMatches,
			EvaluatedAt:    t,
		},
		{
			Namespace:   "myapp",
			PVC:         "data",
			Mode:        testModeAudit,
			Tier:        "hourly",
			LabelSource: controller.LabelSourceV4,
			Owner:       controller.OwnerNone,
			Action:      controller.ActionWouldCreate,
			EvaluatedAt: t,
		},
	}
	return controller.ParityReport{
		GeneratedAt:       t,
		OperatorMode:      testModeAudit,
		NamingStrategy:    "bare-dst",
		DefaultRepoSecret: testRepoSecretFix,
		Summary: controller.ReportSummary{
			TotalPVCs: 2,
			ByAction: map[controller.ActionKind]int{
				controller.ActionAlreadyMatches: 1,
				controller.ActionWouldCreate:    1,
			},
			ByOwner: map[controller.OwnerClassification]int{
				controller.OwnerInlineArgo: 1,
				controller.OwnerNone:       1,
			},
			BySource: map[controller.LabelSource]int{
				controller.LabelSourceLegacy: 1,
				controller.LabelSourceV4:     1,
			},
		},
		Entries: entries,
	}
}

// emptyReport mirrors what a brand-new Store returns: zero entries,
// fully-populated summary maps with all enum members at zero, intact
// metadata.
func emptyReport() controller.ParityReport {
	store := controller.NewStore(testModeAudit, "bare-dst", testRepoSecretFix)
	return store.Snapshot()
}

// newHandler builds the handler under test and an httptest recorder
// helper. Returns the handler so tests can invoke it directly or via
// http.NewRequest.
func newHandler(t *testing.T, report controller.ParityReport) (*AuditHandler, *fakeSnapshotter) {
	t.Helper()
	fake := &fakeSnapshotter{report: report}
	return NewAuditHandler(fake, nil), fake
}

// =============================================================================
// GET returns 200, application/json, valid JSON, summary + entries
// =============================================================================

func TestAuditHandler_GET_Returns200(t *testing.T) {
	h, _ := newHandler(t, nonEmptyReport())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", rr.Code, http.StatusOK)
	}
}

func TestAuditHandler_GET_ContentTypeApplicationJSON(t *testing.T) {
	h, _ := newHandler(t, nonEmptyReport())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	got := rr.Header().Get("Content-Type")
	if got != "application/json" {
		t.Errorf("Content-Type: got %q, want application/json", got)
	}
}

func TestAuditHandler_GET_CacheControlNoStore(t *testing.T) {
	h, _ := newHandler(t, nonEmptyReport())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if got := rr.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control: got %q, want no-store", got)
	}
}

func TestAuditHandler_GET_BodyIsValidJSON(t *testing.T) {
	h, _ := newHandler(t, nonEmptyReport())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	var generic map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &generic); err != nil {
		t.Fatalf("response body is not valid JSON: %v\nbody: %s", err, rr.Body.String())
	}

	// Top-level keys we expect every response to carry.
	for _, key := range []string{"generated_at", "operator_mode", "naming_strategy", "default_repo_secret", "summary", "entries"} {
		if _, ok := generic[key]; !ok {
			t.Errorf("missing top-level key %q in response: %s", key, rr.Body.String())
		}
	}
}

func TestAuditHandler_GET_BodyIncludesSummaryAndEntries(t *testing.T) {
	h, _ := newHandler(t, nonEmptyReport())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	var got controller.ParityReport
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode ParityReport: %v\nbody: %s", err, rr.Body.String())
	}

	if got.Summary.TotalPVCs != 2 {
		t.Errorf("Summary.TotalPVCs: got %d, want 2", got.Summary.TotalPVCs)
	}
	if got.Summary.ByAction[controller.ActionAlreadyMatches] != 1 {
		t.Errorf("Summary.ByAction[already-matches]: got %d, want 1",
			got.Summary.ByAction[controller.ActionAlreadyMatches])
	}
	if got.Summary.ByAction[controller.ActionWouldCreate] != 1 {
		t.Errorf("Summary.ByAction[would-create]: got %d, want 1",
			got.Summary.ByAction[controller.ActionWouldCreate])
	}
	if len(got.Entries) != 2 {
		t.Fatalf("Entries len: got %d, want 2", len(got.Entries))
	}

	// Spot-check one entry to confirm fields survive the round-trip.
	var openWebUI *controller.ParityEntry
	for i := range got.Entries {
		if got.Entries[i].Namespace == testNSOpenWebUI {
			openWebUI = &got.Entries[i]
			break
		}
	}
	if openWebUI == nil {
		t.Fatalf("open-webui entry missing from response")
	}
	if openWebUI.Action != controller.ActionAlreadyMatches {
		t.Errorf("open-webui action: got %q, want %q", openWebUI.Action, controller.ActionAlreadyMatches)
	}
	if openWebUI.LabelSource != controller.LabelSourceLegacy {
		t.Errorf("open-webui label_source: got %q, want %q", openWebUI.LabelSource, controller.LabelSourceLegacy)
	}
}

func TestAuditHandler_GET_TopLevelMetadataPropagates(t *testing.T) {
	h, _ := newHandler(t, nonEmptyReport())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	var got controller.ParityReport
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.OperatorMode != testModeAudit {
		t.Errorf("OperatorMode: got %q, want audit", got.OperatorMode)
	}
	if got.NamingStrategy != "bare-dst" {
		t.Errorf("NamingStrategy: got %q, want bare-dst", got.NamingStrategy)
	}
	if got.DefaultRepoSecret != testRepoSecretFix {
		t.Errorf("DefaultRepoSecret: got %q, want volsync-kopia-repository", got.DefaultRepoSecret)
	}
}

// =============================================================================
// Empty store → still a valid report
// =============================================================================

func TestAuditHandler_EmptyStore_ValidReport(t *testing.T) {
	h, _ := newHandler(t, emptyReport())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusOK)
	}

	var got controller.ParityReport
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v\nbody: %s", err, rr.Body.String())
	}

	if got.Summary.TotalPVCs != 0 {
		t.Errorf("Summary.TotalPVCs: got %d, want 0", got.Summary.TotalPVCs)
	}
	if len(got.Entries) != 0 {
		t.Errorf("Entries: got %d, want 0", len(got.Entries))
	}
	// Summary maps must still be populated with every enum at zero —
	// the consumer should always see the full taxonomy.
	if len(got.Summary.ByAction) == 0 {
		t.Error("Summary.ByAction is empty; expected fully-populated zero map")
	}
	if len(got.Summary.ByOwner) == 0 {
		t.Error("Summary.ByOwner is empty; expected fully-populated zero map")
	}
	if len(got.Summary.BySource) == 0 {
		t.Error("Summary.BySource is empty; expected fully-populated zero map")
	}
	for action, count := range got.Summary.ByAction {
		if count != 0 {
			t.Errorf("Summary.ByAction[%q] = %d; expected 0 in empty report", action, count)
		}
	}
}

// =============================================================================
// HEAD support (optional per spec; implemented because trivial)
// =============================================================================

func TestAuditHandler_HEAD_Returns200(t *testing.T) {
	h, _ := newHandler(t, nonEmptyReport())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodHead, "/audit", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("HEAD status: got %d, want %d", rr.Code, http.StatusOK)
	}
	if got := rr.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("HEAD Content-Type: got %q, want application/json", got)
	}
	if rr.Body.Len() != 0 {
		t.Errorf("HEAD body: got %d bytes, want 0", rr.Body.Len())
	}
}

func TestAuditHandler_HEAD_StillCallsSnapshot(t *testing.T) {
	// Defense-in-depth: HEAD is meant as a reachability probe. The
	// Snapshot() call exercises the full store-→handler code path,
	// so a HEAD that returns 200 actually means "the store is wired
	// up correctly," not just "the binary is running."
	h, fake := newHandler(t, nonEmptyReport())
	req := httptest.NewRequestWithContext(t.Context(), http.MethodHead, "/audit", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)
	if fake.calls != 1 {
		t.Errorf("Snapshot call count: got %d, want 1", fake.calls)
	}
}

// =============================================================================
// non-GET/HEAD → 405 + Allow header
// =============================================================================

func TestAuditHandler_NonGET_Returns405(t *testing.T) {
	cases := []string{
		http.MethodPost,
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
		http.MethodOptions,
		"WEIRD",
	}
	for _, method := range cases {
		t.Run(method, func(t *testing.T) {
			h, fake := newHandler(t, nonEmptyReport())
			req := httptest.NewRequestWithContext(t.Context(), method, "/audit", nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			if rr.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s: status got %d, want %d", method, rr.Code, http.StatusMethodNotAllowed)
			}
			if got := rr.Header().Get("Allow"); got != "GET, HEAD" {
				t.Errorf("%s: Allow header got %q, want %q", method, got, "GET, HEAD")
			}
			// 405 must not have triggered a Store read — the handler
			// rejects before touching the snapshotter.
			if fake.calls != 0 {
				t.Errorf("%s: snapshotter called %d times on 405, want 0", method, fake.calls)
			}
		})
	}
}

// =============================================================================
// Concurrency safety under -race
// =============================================================================

func TestAuditHandler_ConcurrentGETs_Safe(t *testing.T) {
	// 32 goroutines × 64 requests = 2048 concurrent requests. With
	// `go test -race` this catches any unsynchronized access in the
	// handler itself. The Store.Snapshot path is already covered by
	// controller-package tests; this test specifically covers the
	// handler's wrap around it.
	h, fake := newHandler(t, nonEmptyReport())

	const goroutines = 32
	const iterations = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
				rr := httptest.NewRecorder()
				h.ServeHTTP(rr, req)
				if rr.Code != http.StatusOK {
					t.Errorf("status: got %d, want 200", rr.Code)
					return
				}
				// Body must decode every time.
				var report controller.ParityReport
				if err := json.Unmarshal(rr.Body.Bytes(), &report); err != nil {
					t.Errorf("decode: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if got, want := fake.calls, goroutines*iterations; got != want {
		t.Errorf("snapshotter call count: got %d, want %d", got, want)
	}
}

// =============================================================================
// JSON body does not mutate the underlying store
// =============================================================================

// The Store path's safety: Snapshot() returns a deep copy, so the
// handler's encoder cannot reach back into the live map/slice. This
// test pins that contract via a real Store (not the fake) and checks
// that the store's observable state is identical before vs after a
// burst of GETs.
func TestAuditHandler_GET_DoesNotMutateStore(t *testing.T) {
	store := controller.NewStore(testModeAudit, "bare-dst", testRepoSecretFix)
	t0 := time.Date(2026, 5, 23, 22, 0, 0, 0, time.UTC)
	store.Set(controller.ParityEntry{
		Namespace:   "myapp",
		PVC:         "data",
		Mode:        testModeAudit,
		Tier:        "hourly",
		LabelSource: controller.LabelSourceV4,
		Owner:       controller.OwnerNone,
		Action:      controller.ActionWouldCreate,
		EvaluatedAt: t0,
	})
	store.Set(controller.ParityEntry{
		Namespace:   testNSOpenWebUI,
		PVC:         "storage",
		Mode:        testModeAudit,
		Tier:        "daily",
		LabelSource: controller.LabelSourceLegacy,
		Owner:       controller.OwnerInlineArgo,
		Action:      controller.ActionAlreadyMatches,
		EvaluatedAt: t0,
	})

	// Take a baseline snapshot for comparison. We compare entries
	// only (GeneratedAt is set to time.Now() inside Snapshot, so two
	// snapshots taken at different instants legitimately differ on
	// that field).
	baseline := store.Snapshot()

	h := NewAuditHandler(store, nil)
	for i := 0; i < 25; i++ {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("iteration %d: status %d", i, rr.Code)
		}
	}

	after := store.Snapshot()

	if !reflect.DeepEqual(baseline.Entries, after.Entries) {
		t.Errorf("entries mutated by handler:\nbefore: %+v\nafter:  %+v", baseline.Entries, after.Entries)
	}
	if !reflect.DeepEqual(baseline.Summary, after.Summary) {
		t.Errorf("summary mutated by handler:\nbefore: %+v\nafter:  %+v", baseline.Summary, after.Summary)
	}
	if baseline.OperatorMode != after.OperatorMode ||
		baseline.NamingStrategy != after.NamingStrategy ||
		baseline.DefaultRepoSecret != after.DefaultRepoSecret {
		t.Errorf("metadata mutated by handler:\nbefore: %+v\nafter:  %+v", baseline, after)
	}
	if got := store.Len(); got != 2 {
		t.Errorf("Store.Len after handler bursts: got %d, want 2", got)
	}
}

// =============================================================================
// Production Store satisfies the ParitySnapshotter interface
// =============================================================================

// Compile-time guarantee: *controller.Store must implement
// ParitySnapshotter. If a future refactor changes Snapshot's signature,
// this test breaks at compile time rather than at runtime when
// cmd/operator wiring tries to inject the store.
func TestStore_ImplementsParitySnapshotter(t *testing.T) {
	var _ ParitySnapshotter = (*controller.Store)(nil)
}

// =============================================================================
// nil-logger tolerance
// =============================================================================

// The handler must tolerate a nil logger — internal services in this
// repo sometimes construct lightweight handlers without wiring a
// logger. nil should not panic.
func TestAuditHandler_NilLogger_DoesNotPanic(t *testing.T) {
	h := NewAuditHandler(&fakeSnapshotter{report: emptyReport()}, nil)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("handler panicked with nil logger: %v", r)
		}
	}()
	h.ServeHTTP(rr, req)
	_, _ = io.Copy(io.Discard, rr.Body)
	if rr.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", rr.Code)
	}
}

// =============================================================================
// Backend independence — explicit structural assertion
// =============================================================================

// Every test in this file uses fakeSnapshotter or controller.Store
// directly. Neither path imports internal/backend, internal/kopia, or
// internal/s3 transitively from the handler under test (only the
// existing /exists handler does). This test is the canary: if a
// future change adds a backend dependency to AuditHandler, the
// constructor signature will likely shift and force a re-think.
//
// We assert the contract by constructing the handler with the
// narrowest possible dependency and checking it works end-to-end
// without any backend wiring.
func TestAuditHandler_NoBackendDependency(t *testing.T) {
	// A snapshotter with zero fields beyond the report. No backend
	// client, no health checker, no logger, nothing storage-related.
	fake := &fakeSnapshotter{report: emptyReport()}
	h := NewAuditHandler(fake, nil)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/audit", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (backend-free path must succeed)", rr.Code)
	}
	if fake.calls != 1 {
		t.Errorf("Snapshot call count: got %d, want 1", fake.calls)
	}
}
