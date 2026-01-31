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
		"log_level", cfg.LogLevel)

	// Create backend based on configuration
	var backend handler.BackendClient
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
		backend = s3Client

	case "kopia-fs":
		logger.Info("initializing kopia-fs backend", "path", cfg.KopiaRepositoryPath)
		kopiaClient := kopia.NewClient(cfg.KopiaRepositoryPath, cfg.KopiaPassword, logger)
		if err := kopiaClient.Connect(context.Background()); err != nil {
			logger.Error("failed to connect to kopia repository", "error", err)
			os.Exit(1)
		}
		backend = kopiaClient
	}

	// Create handlers
	h := handler.New(backend, logger)

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

	// Wait for interrupt signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down server")

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("server forced to shutdown", "error", err)
		os.Exit(1)
	}

	logger.Info("server stopped")
}
