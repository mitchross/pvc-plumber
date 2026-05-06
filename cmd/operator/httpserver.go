// HTTP server construction for the operator binary.
//
// This mirrors cmd/pvc-plumber/main.go's setup so the operator can host the
// existing read-only `/exists/`, `/healthz`, `/readyz`, `/metrics` surface
// alongside the new controller-runtime manager. The two share a single
// backend + cache instance, so webhook handlers and HTTP callers benefit
// from one connection to Kopia and one cached decision per (ns, pvc).
//
// The function below is intentionally a near-copy of the equivalent block
// in cmd/pvc-plumber/main.go — keeping them duplicate for now (rather than
// extracting to a shared internal package) preserves the legacy binary
// untouched until the operator has soaked through Phase 4 cutover.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/mitchross/pvc-plumber/internal/cache"
	"github.com/mitchross/pvc-plumber/internal/config"
	"github.com/mitchross/pvc-plumber/internal/handler"
	"github.com/mitchross/pvc-plumber/internal/kopia"
	"github.com/mitchross/pvc-plumber/internal/s3"
)

// backendBundle groups the constructed backend, the cache wrapper that
// fronts it, and (when applicable) the typed *kopia.Client. The kopia
// pointer is only set for BACKEND_TYPE=kopia-fs; callers must nil-check
// before using it (e.g. for the periodic cache re-warm loop).
type backendBundle struct {
	backend handler.BackendClient
	cached  *cache.CachedClient
	kopia   *kopia.Client
}

// buildBackend constructs the backend client + cache layer the same way
// cmd/pvc-plumber/main.go does. Returns the cached client (which the
// operator passes to webhook handlers as their `kopiaClient`) plus the
// raw kopia client (used for cache pre-warm and re-warm).
//
// On BACKEND_TYPE=s3 the kopia field is nil; the cache layer wraps the S3
// client directly. The webhook layer doesn't care which backend it's
// talking to as long as the BackendClient.CheckBackupExists contract holds.
func buildBackend(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*backendBundle, error) {
	var backendClient handler.BackendClient
	var kopiaClient *kopia.Client

	switch cfg.BackendType {
	case "s3":
		logger.Info("initializing s3 backend",
			"endpoint", cfg.S3Endpoint,
			"bucket", cfg.S3Bucket,
			"secure", cfg.S3Secure)
		s3Client, err := s3.NewClient(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Secure)
		if err != nil {
			return nil, fmt.Errorf("create s3 client: %w", err)
		}
		backendClient = s3Client

	case "kopia-fs":
		logger.Info("initializing kopia-fs backend", "path", cfg.KopiaRepositoryPath)
		kc := kopia.NewClient(cfg.KopiaRepositoryPath, cfg.KopiaPassword, logger)
		if err := kc.Connect(ctx); err != nil {
			return nil, fmt.Errorf("connect to kopia repository: %w", err)
		}
		kopiaClient = kc
		backendClient = kc

	default:
		return nil, fmt.Errorf("invalid BACKEND_TYPE: %s", cfg.BackendType)
	}

	cachedBackend := cache.New(backendClient, cfg.CacheTTL, logger)

	// Pre-warm only on kopia (S3 has its own listing semantics). Failure is
	// non-fatal — the cache populates on demand.
	if kopiaClient != nil {
		sources, err := kopiaClient.ListAllSources(ctx)
		if err != nil {
			logger.Warn("cache pre-warm failed, will populate on demand", "error", err)
		} else {
			cachedBackend.PreWarm(sources)
		}
	}

	return &backendBundle{
		backend: backendClient,
		cached:  cachedBackend,
		kopia:   kopiaClient,
	}, nil
}

// newHTTPServer wires the existing /exists, /healthz, /readyz, /metrics
// routes onto a *http.Server bound to cfg.Port. The caller is responsible
// for ListenAndServe + Shutdown.
func newHTTPServer(cfg *config.Config, b *backendBundle, logger *slog.Logger) *http.Server {
	var healthChecker handler.HealthChecker
	if hc, ok := b.backend.(handler.HealthChecker); ok {
		healthChecker = hc
	}
	h := handler.NewWithHealthChecker(b.cached, healthChecker, logger)
	h.SetRequestTimeout(cfg.HTTPTimeout)

	mux := http.NewServeMux()
	mux.HandleFunc("/exists/", h.HandleExists)
	mux.HandleFunc("/healthz", h.HandleHealthz)
	mux.HandleFunc("/readyz", h.HandleReadyz)
	mux.HandleFunc("/metrics", h.HandleMetrics)

	return &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
}

// runCacheReWarmLoop is a verbatim port of the same function in
// cmd/pvc-plumber/main.go. Periodically re-runs `kopia snapshot list --all`
// and refreshes the cache so deleted backups stop returning stale
// exists=true within one re-warm cycle. Returns when ctx is canceled.
func runCacheReWarmLoop(
	ctx context.Context,
	kopiaClient *kopia.Client,
	cachedBackend *cache.CachedClient,
	interval time.Duration,
	logger *slog.Logger,
) {
	logger.Info("cache re-warm loop starting", "interval", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	callTimeout := interval
	if callTimeout > 60*time.Second {
		callTimeout = 60 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("cache re-warm loop stopping")
			return
		case <-ticker.C:
			callCtx, cancel := context.WithTimeout(ctx, callTimeout)
			sources, err := kopiaClient.ListAllSources(callCtx)
			cancel()
			if err != nil {
				logger.Warn("cache re-warm failed; keeping previous entries", "error", err)
				continue
			}
			cachedBackend.Refresh(sources)
		}
	}
}
