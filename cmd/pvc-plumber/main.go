package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mitchross/pvc-plumber/internal/cache"
	"github.com/mitchross/pvc-plumber/internal/config"
	"github.com/mitchross/pvc-plumber/internal/handler"
	"github.com/mitchross/pvc-plumber/internal/kopia"
	"github.com/mitchross/pvc-plumber/internal/s3"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	var logLevel slog.Level
	switch cfg.LogLevel {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	logger.Info("starting pvc-plumber",
		"backend", cfg.BackendType,
		"port", cfg.Port,
		"log_level", cfg.LogLevel,
		"cache_ttl", cfg.CacheTTL,
		"re_warm_interval", cfg.ReWarmInterval)

	// Create backend based on configuration
	var backendClient handler.BackendClient
	switch cfg.BackendType {
	case "s3":
		logger.Info("initializing s3 backend",
			"endpoint", cfg.S3Endpoint,
			"bucket", cfg.S3Bucket,
			"secure", cfg.S3Secure)
		s3Client, err := s3.NewClient(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Secure)
		if err != nil {
			logger.Error("failed to create S3 client", "error", err)
			os.Exit(1)
		}
		backendClient = s3Client

	case "kopia-fs":
		logger.Info("initializing kopia-fs backend", "path", cfg.KopiaRepositoryPath)
		kopiaClient := kopia.NewClient(cfg.KopiaRepositoryPath, cfg.KopiaPassword, logger)
		if err := kopiaClient.Connect(context.Background()); err != nil {
			logger.Error("failed to connect to kopia repository", "error", err)
			os.Exit(1)
		}
		backendClient = kopiaClient
	}

	// Wrap backend with cache
	cachedBackend := cache.New(backendClient, cfg.CacheTTL, logger)

	// Pre-warm cache for kopia backend
	var kopiaClient *kopia.Client
	if cfg.BackendType == "kopia-fs" {
		if kc, ok := backendClient.(*kopia.Client); ok {
			kopiaClient = kc
			sources, err := kc.ListAllSources(context.Background())
			if err != nil {
				logger.Warn("cache pre-warm failed, will populate on demand", "error", err)
			} else {
				cachedBackend.PreWarm(sources)
			}
		}
	}

	// Create handlers — pass the raw backend as health checker (cache layer doesn't need health checks)
	var healthChecker handler.HealthChecker
	if hc, ok := backendClient.(handler.HealthChecker); ok {
		healthChecker = hc
	}
	h := handler.NewWithHealthChecker(cachedBackend, healthChecker, logger)
	h.SetRequestTimeout(cfg.HTTPTimeout)

	// Setup HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/exists/", h.HandleExists)
	mux.HandleFunc("/healthz", h.HandleHealthz)
	mux.HandleFunc("/readyz", h.HandleReadyz)
	mux.HandleFunc("/metrics", h.HandleMetrics)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		logger.Info("server starting", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Periodic cache re-warm (kopia backend only). Each tick re-runs
	// `kopia snapshot list --all` and rebuilds the cache so deleted
	// backups stop returning stale exists=true entries within one
	// re-warm cycle instead of waiting for each entry's TTL to expire.
	rwCtx, rwCancel := context.WithCancel(context.Background())
	defer rwCancel()
	if kopiaClient != nil && cfg.ReWarmInterval > 0 {
		go runCacheReWarmLoop(rwCtx, kopiaClient, cachedBackend, cfg.ReWarmInterval, logger)
	}

	// Wait for interrupt signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down server")
	rwCancel()

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("server forced to shutdown", "error", err)
		os.Exit(1)
	}

	logger.Info("server stopped")
}

// runCacheReWarmLoop periodically re-runs kopia ListAllSources and
// refreshes the cache. Returns when ctx is cancelled (shutdown). Each
// tick is bounded by a per-call timeout so a hung kopia subprocess
// can't pin the goroutine across multiple intervals.
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

	// Bound each call to roughly one interval so a stuck `kopia snapshot
	// list --all` can't keep the loop from progressing.
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
