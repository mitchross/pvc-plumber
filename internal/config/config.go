package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	// Common settings
	BackendType string
	HTTPTimeout time.Duration
	Port        string
	LogLevel    string

	// S3 backend settings
	S3Endpoint  string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3Secure    bool

	// Kopia backend settings
	KopiaRepositoryPath string
	KopiaPassword       string
}

func Load() (*Config, error) {
	// Common settings
	backendType := os.Getenv("BACKEND_TYPE")
	if backendType == "" {
		backendType = "s3"
	}
	if backendType != "s3" && backendType != "kopia-fs" {
		return nil, fmt.Errorf("invalid BACKEND_TYPE: %s (must be 's3' or 'kopia-fs')", backendType)
	}

	httpTimeout := 3 * time.Second
	if timeoutStr := os.Getenv("HTTP_TIMEOUT"); timeoutStr != "" {
		duration, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return nil, fmt.Errorf("invalid HTTP_TIMEOUT: %w", err)
		}
		httpTimeout = duration
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	cfg := &Config{
		BackendType: backendType,
		HTTPTimeout: httpTimeout,
		Port:        port,
		LogLevel:    logLevel,
	}

	// Backend-specific validation
	switch backendType {
	case "s3":
		if err := loadS3Config(cfg); err != nil {
			return nil, err
		}
	case "kopia-fs":
		if err := loadKopiaConfig(cfg); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

func loadS3Config(cfg *Config) error {
	cfg.S3Endpoint = os.Getenv("S3_ENDPOINT")
	if cfg.S3Endpoint == "" {
		return fmt.Errorf("S3_ENDPOINT is required")
	}

	cfg.S3Bucket = os.Getenv("S3_BUCKET")
	if cfg.S3Bucket == "" {
		return fmt.Errorf("S3_BUCKET is required")
	}

	cfg.S3AccessKey = os.Getenv("S3_ACCESS_KEY")
	if cfg.S3AccessKey == "" {
		return fmt.Errorf("S3_ACCESS_KEY is required")
	}

	cfg.S3SecretKey = os.Getenv("S3_SECRET_KEY")
	if cfg.S3SecretKey == "" {
		return fmt.Errorf("S3_SECRET_KEY is required")
	}

	cfg.S3Secure = false
	if secureStr := os.Getenv("S3_SECURE"); secureStr != "" {
		var err error
		cfg.S3Secure, err = strconv.ParseBool(secureStr)
		if err != nil {
			return fmt.Errorf("invalid S3_SECURE: %w", err)
		}
	}

	return nil
}

func loadKopiaConfig(cfg *Config) error {
	cfg.KopiaRepositoryPath = os.Getenv("KOPIA_REPOSITORY_PATH")
	if cfg.KopiaRepositoryPath == "" {
		cfg.KopiaRepositoryPath = "/repository"
	}

	// Verify path exists
	if _, err := os.Stat(cfg.KopiaRepositoryPath); os.IsNotExist(err) {
		return fmt.Errorf("KOPIA_REPOSITORY_PATH %s does not exist", cfg.KopiaRepositoryPath)
	}

	cfg.KopiaPassword = os.Getenv("KOPIA_PASSWORD")
	if cfg.KopiaPassword == "" {
		return fmt.Errorf("KOPIA_PASSWORD is required for kopia-fs backend")
	}

	return nil
}
