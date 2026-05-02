package cache

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// fakeBackend is a hand-rolled stub used so the cache tests don't have
// to import handler or set up a real backend. It records call counts
// and returns a configured CheckResult.
type fakeBackend struct {
	calls  atomic.Int64
	result backend.CheckResult
}

func (f *fakeBackend) CheckBackupExists(_ context.Context, namespace, pvc string) backend.CheckResult {
	f.calls.Add(1)
	r := f.result
	r.Namespace = namespace
	r.Pvc = pvc
	return r
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPreWarm_AddsEntriesWithoutEvicting(t *testing.T) {
	c := New(&fakeBackend{}, time.Minute, discardLogger())

	c.PreWarm(map[string]bool{
		"app-a/data": true,
		"app-b/data": false,
	})

	if got := len(c.items); got != 2 {
		t.Fatalf("after PreWarm: items=%d, want 2", got)
	}

	// Second PreWarm with a different set — entries from the first
	// call must STAY (PreWarm does not evict).
	c.PreWarm(map[string]bool{
		"app-c/data": true,
	})

	if got := len(c.items); got != 3 {
		t.Fatalf("after second PreWarm: items=%d, want 3 (no eviction)", got)
	}
}

func TestRefresh_ReplacesItemsAndEvictsMissing(t *testing.T) {
	c := New(&fakeBackend{}, time.Minute, discardLogger())

	c.PreWarm(map[string]bool{
		"app-a/data": true,
		"app-b/data": true,
	})
	if got := len(c.items); got != 2 {
		t.Fatalf("setup: items=%d, want 2", got)
	}

	// Refresh with a new set — app-a kept, app-b dropped, app-c added.
	c.Refresh(map[string]bool{
		"app-a/data": true,
		"app-c/data": false,
	})

	if got := len(c.items); got != 2 {
		t.Fatalf("after Refresh: items=%d, want 2", got)
	}
	if _, ok := c.items["app-b/data"]; ok {
		t.Errorf("app-b/data should have been evicted, still present")
	}
	if _, ok := c.items["app-c/data"]; !ok {
		t.Errorf("app-c/data should have been added, missing")
	}
	if e, ok := c.items["app-a/data"]; !ok {
		t.Errorf("app-a/data should still be present, missing")
	} else if !e.result.Authoritative {
		t.Errorf("kept entry lost authoritative bit")
	}
}

func TestRefresh_ExtendsExpiryOfKeptEntries(t *testing.T) {
	c := New(&fakeBackend{}, time.Minute, discardLogger())

	c.PreWarm(map[string]bool{"app-a/data": true})
	originalExpiry := c.items["app-a/data"].expiresAt

	// Sleep long enough that the new expiry time is observably later.
	time.Sleep(2 * time.Millisecond)

	c.Refresh(map[string]bool{"app-a/data": true})
	newExpiry := c.items["app-a/data"].expiresAt

	if !newExpiry.After(originalExpiry) {
		t.Errorf("Refresh should have extended expiry: original=%s new=%s", originalExpiry, newExpiry)
	}
}

func TestRefresh_SkipsMalformedKeys(t *testing.T) {
	c := New(&fakeBackend{}, time.Minute, discardLogger())

	c.Refresh(map[string]bool{
		"valid/key":     true,
		"missing-slash": true,
		"/empty-ns":     true,
		"empty-pvc/":    true,
	})

	if got := len(c.items); got != 1 {
		t.Fatalf("malformed keys should be skipped: items=%d, want 1", got)
	}
	if _, ok := c.items["valid/key"]; !ok {
		t.Errorf("valid/key should be present, missing")
	}
}

func TestCheckBackupExists_ServesFromRefreshedCache(t *testing.T) {
	bk := &fakeBackend{result: backend.CheckResult{Decision: backend.DecisionFresh, Authoritative: true}}
	c := New(bk, time.Minute, discardLogger())

	c.Refresh(map[string]bool{"app-a/data": true})

	// Cache hit — backend should NOT be called.
	res := c.CheckBackupExists(context.Background(), "app-a", "data")
	if !res.Exists {
		t.Errorf("Refresh wrote exists=true, got false")
	}
	if got := bk.calls.Load(); got != 0 {
		t.Errorf("backend was called %d times, want 0 (cache hit expected)", got)
	}
}

// blockingBackend gates CheckBackupExists on a `release` channel so the test
// can guarantee multiple goroutines have entered the cache-miss path
// concurrently before any of them complete. Used to verify singleflight
// dedups identical concurrent lookups.
type blockingBackend struct {
	calls   atomic.Int64
	release chan struct{}
	result  backend.CheckResult
}

func (b *blockingBackend) CheckBackupExists(_ context.Context, namespace, pvc string) backend.CheckResult {
	b.calls.Add(1)
	<-b.release
	r := b.result
	r.Namespace = namespace
	r.Pvc = pvc
	return r
}

func TestCheckBackupExists_SingleflightDedupsConcurrentLookups(t *testing.T) {
	bk := &blockingBackend{
		release: make(chan struct{}),
		result: backend.CheckResult{
			Exists:        true,
			Decision:      backend.DecisionRestore,
			Authoritative: true,
			Backend:       "kopia-fs",
		},
	}
	c := New(bk, time.Minute, discardLogger())

	const callers = 8
	var wg sync.WaitGroup
	wg.Add(callers)

	// Fan out N concurrent callers for the SAME key. They all enter the
	// cache-miss path and contend on singleflight; only one should reach
	// the backend.
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			res := c.CheckBackupExists(context.Background(), "ns", "data")
			if !res.Exists {
				t.Errorf("expected exists=true from shared result, got false")
			}
		}()
	}

	// Give goroutines time to enter sf.Do and queue up behind the leader.
	// 50ms is generous; the test still works at 5ms but is flakier under
	// load. We then unblock the leader's backend call.
	time.Sleep(50 * time.Millisecond)
	close(bk.release)
	wg.Wait()

	if got := bk.calls.Load(); got != 1 {
		t.Errorf("backend called %d times for %d concurrent callers, want 1 (singleflight dedup)", got, callers)
	}
	if got, want := c.DedupedCalls(), int64(callers-1); got != want {
		t.Errorf("DedupedCalls=%d, want %d", got, want)
	}
}

func TestCheckBackupExists_FallsThroughOnEvictedEntry(t *testing.T) {
	bk := &fakeBackend{result: backend.CheckResult{
		Decision:      backend.DecisionFresh,
		Authoritative: true,
	}}
	c := New(bk, time.Minute, discardLogger())

	c.Refresh(map[string]bool{"app-a/data": true})
	c.Refresh(map[string]bool{}) // evict everything

	// Cache miss after eviction — backend MUST be called.
	c.CheckBackupExists(context.Background(), "app-a", "data")
	if got := bk.calls.Load(); got != 1 {
		t.Errorf("backend was called %d times after eviction, want 1", got)
	}
}
