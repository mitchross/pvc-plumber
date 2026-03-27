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
func (c *CachedClient) PreWarm(sources map[string]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiry := time.Now().Add(c.ttl)
	for key, exists := range sources {
		// Parse namespace/pvc from key
		var namespace, pvc string
		for i := 0; i < len(key); i++ {
			if key[i] == '/' {
				namespace = key[:i]
				pvc = key[i+1:]
				break
			}
		}
		if namespace == "" || pvc == "" {
			continue
		}
		c.items[key] = entry{
			result: backend.CheckResult{
				Exists:    exists,
				Namespace: namespace,
				Pvc:       pvc,
				Backend:   "kopia-fs",
			},
			expiresAt: expiry,
		}
	}
	c.logger.Info("cache pre-warmed", "entries", len(c.items))
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
	if result.Error == "" {
		c.mu.Lock()
		c.items[key] = entry{result: result, expiresAt: time.Now().Add(c.ttl)}
		c.mu.Unlock()
	}

	return result
}
