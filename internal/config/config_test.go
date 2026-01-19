package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	// Save original env vars
	origS3Endpoint := os.Getenv("S3_ENDPOINT")
	origS3Bucket := os.Getenv("S3_BUCKET")
	origHTTPTimeout := os.Getenv("HTTP_TIMEOUT")
	origPort := os.Getenv("PORT")
	origLogLevel := os.Getenv("LOG_LEVEL")

	// Restore env vars after test
	defer func() {
		_ = os.Setenv("S3_ENDPOINT", origS3Endpoint)
		_ = os.Setenv("S3_BUCKET", origS3Bucket)
		_ = os.Setenv("HTTP_TIMEOUT", origHTTPTimeout)
		_ = os.Setenv("PORT", origPort)
		_ = os.Setenv("LOG_LEVEL", origLogLevel)
	}()

	tests := []struct {
		name         string
		envVars      map[string]string
		wantErr      bool
		wantEndpoint string
		wantBucket   string
		wantTimeout  time.Duration
		wantPort     string
		wantLogLevel string
	}{
		{
			name: "valid config with all env vars",
			envVars: map[string]string{
				"S3_ENDPOINT":  "http://localhost:9000",
				"S3_BUCKET":    "test-bucket",
				"HTTP_TIMEOUT": "5s",
				"PORT":         "9090",
				"LOG_LEVEL":    "debug",
			},
			wantErr:      false,
			wantEndpoint: "http://localhost:9000",
			wantBucket:   "test-bucket",
			wantTimeout:  5 * time.Second,
			wantPort:     "9090",
			wantLogLevel: "debug",
		},
		{
			name: "valid config with defaults",
			envVars: map[string]string{
				"S3_ENDPOINT": "http://minio:9000",
				"S3_BUCKET":   "volsync-backup",
			},
			wantErr:      false,
			wantEndpoint: "http://minio:9000",
			wantBucket:   "volsync-backup",
			wantTimeout:  3 * time.Second,
			wantPort:     "8080",
			wantLogLevel: "info",
		},
		{
			name: "missing S3_ENDPOINT",
			envVars: map[string]string{
				"S3_BUCKET": "test-bucket",
			},
			wantErr: true,
		},
		{
			name: "missing S3_BUCKET",
			envVars: map[string]string{
				"S3_ENDPOINT": "http://localhost:9000",
			},
			wantErr: true,
		},
		{
			name: "invalid HTTP_TIMEOUT",
			envVars: map[string]string{
				"S3_ENDPOINT":  "http://localhost:9000",
				"S3_BUCKET":    "test-bucket",
				"HTTP_TIMEOUT": "invalid",
			},
			wantErr: true,
		},
		{
			name: "timeout with milliseconds",
			envVars: map[string]string{
				"S3_ENDPOINT":  "http://localhost:9000",
				"S3_BUCKET":    "test-bucket",
				"HTTP_TIMEOUT": "500ms",
			},
			wantErr:      false,
			wantEndpoint: "http://localhost:9000",
			wantBucket:   "test-bucket",
			wantTimeout:  500 * time.Millisecond,
			wantPort:     "8080",
			wantLogLevel: "info",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all env vars
			_ = os.Unsetenv("S3_ENDPOINT")
			_ = os.Unsetenv("S3_BUCKET")
			_ = os.Unsetenv("HTTP_TIMEOUT")
			_ = os.Unsetenv("PORT")
			_ = os.Unsetenv("LOG_LEVEL")

			// Set test env vars
			for k, v := range tt.envVars {
				_ = os.Setenv(k, v)
			}

			cfg, err := Load()

			if tt.wantErr {
				if err == nil {
					t.Errorf("Load() error = nil, wantErr = true")
				}
				return
			}

			if err != nil {
				t.Errorf("Load() unexpected error = %v", err)
				return
			}

			if cfg.S3Endpoint != tt.wantEndpoint {
				t.Errorf("S3Endpoint = %v, want %v", cfg.S3Endpoint, tt.wantEndpoint)
			}

			if cfg.S3Bucket != tt.wantBucket {
				t.Errorf("S3Bucket = %v, want %v", cfg.S3Bucket, tt.wantBucket)
			}

			if cfg.HTTPTimeout != tt.wantTimeout {
				t.Errorf("HTTPTimeout = %v, want %v", cfg.HTTPTimeout, tt.wantTimeout)
			}

			if cfg.Port != tt.wantPort {
				t.Errorf("Port = %v, want %v", cfg.Port, tt.wantPort)
			}

			if cfg.LogLevel != tt.wantLogLevel {
				t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, tt.wantLogLevel)
			}
		})
	}
}
