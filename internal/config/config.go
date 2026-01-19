package config

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	S3Endpoint  string
	S3Bucket    string
	HTTPTimeout time.Duration
	Port        string
	LogLevel    string
}

func Load() (*Config, error) {
	s3Endpoint := os.Getenv("S3_ENDPOINT")
	if s3Endpoint == "" {
		return nil, fmt.Errorf("S3_ENDPOINT is required")
	}

	s3Bucket := os.Getenv("S3_BUCKET")
	if s3Bucket == "" {
		return nil, fmt.Errorf("S3_BUCKET is required")
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

	return &Config{
		S3Endpoint:  s3Endpoint,
		S3Bucket:    s3Bucket,
		HTTPTimeout: httpTimeout,
		Port:        port,
		LogLevel:    logLevel,
	}, nil
}
