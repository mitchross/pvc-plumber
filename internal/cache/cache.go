package cache

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// BackendClient matches handler.BackendClient for wrapping.
type BackendClient interface {
	CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult
}

type entry struct {
	result    backend.CheckResult
	expiresAt time.Time
}

// CachedClient wraps a backend with an in-memory TTL cache.
type CachedClient struct {
	inner  BackendClient
	ttl    time.Duration
	logger *slog.Logger
	mu     sync.RWMutex
	items  map[string]entry
}

// New creates a cached wrapper around a backend client.
func New(inner BackendClient, ttl time.Duration, logger *slog.Logger) *CachedClient {
	return &CachedClient{
		inner:  inner,
		ttl:    ttl,
		logger: logger,
		items:  make(map[string]entry),
	}
}

// PreWarm populates the cache with known backup sources.
// Keys in the map are "namespace/pvc", values are whether a backup exists.
// Entries already in the cache are overwritten; entries not present in
// `sources` are left untouched and continue to age out via TTL. Use this
// for one-shot warming at startup; use Refresh for the periodic loop.
func (c *CachedClient) PreWarm(sources map[string]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiry := time.Now().Add(c.ttl)
	for key, exists := range sources {
		if e, ok := buildEntry(key, exists, expiry); ok {
			c.items[key] = e
		}
	}
	c.logger.Info("cache pre-warmed", "entries", len(c.items))
}

// Refresh replaces the entire cache with entries built from `sources`.
// Unlike PreWarm, entries that are absent from `sources` are evicted
// immediately rather than left to age out. This is the periodic
// re-warm path: call it on a ticker so deleted backups stop returning
// stale exists=true after their TTL would have hidden the change.
func (c *CachedClient) Refresh(sources map[string]bool) {
	expiry := time.Now().Add(c.ttl)
	newItems := make(map[string]entry, len(sources))
	for key, exists := range sources {
		if e, ok := buildEntry(key, exists, expiry); ok {
			newItems[key] = e
		}
	}

	c.mu.Lock()
	c.items = newItems
	c.mu.Unlock()

	c.logger.Info("cache refreshed", "entries", len(newItems))
}

// buildEntry parses a "namespace/pvc" key and constructs a cache entry.
// Returns (entry, true) on success, or (zero, false) when the key is
// malformed (missing slash, empty namespace, or empty pvc).
func buildEntry(key string, exists bool, expiry time.Time) (entry, bool) {
	var namespace, pvc string
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			namespace = key[:i]
			pvc = key[i+1:]
			break
		}
	}
	if namespace == "" || pvc == "" {
		return entry{}, false
	}
	return entry{
		result: backend.CheckResult{
			Exists:        exists,
			Decision:      decisionForExists(exists),
			Authoritative: true,
			Namespace:     namespace,
			Pvc:           pvc,
			Backend:       "kopia-fs",
			Source:        pvc + "-backup@" + namespace + ":/data",
		},
		expiresAt: expiry,
	}, true
}

func (c *CachedClient) CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult {
	key := namespace + "/" + pvc

	// Check cache
	c.mu.RLock()
	if e, ok := c.items[key]; ok && time.Now().Before(e.expiresAt) {
		c.mu.RUnlock()
		c.logger.Debug("cache hit", "namespace", namespace, "pvc", pvc, "exists", e.result.Exists)
		return e.result
	}
	c.mu.RUnlock()

	// Cache miss — call backend
	result := c.inner.CheckBackupExists(ctx, namespace, pvc)

	// Only cache successful checks (no errors)
	if result.Error == "" && result.Authoritative {
		c.mu.Lock()
		c.items[key] = entry{result: result, expiresAt: time.Now().Add(c.ttl)}
		c.mu.Unlock()
	}

	return result
}

func decisionForExists(exists bool) string {
	if exists {
		return backend.DecisionRestore
	}
	return backend.DecisionFresh
}
