package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad_S3Backend(t *testing.T) {
	// Save original env vars
	origVars := map[string]string{
		"BACKEND_TYPE":  os.Getenv("BACKEND_TYPE"),
		"S3_ENDPOINT":   os.Getenv("S3_ENDPOINT"),
		"S3_BUCKET":     os.Getenv("S3_BUCKET"),
		"S3_ACCESS_KEY": os.Getenv("S3_ACCESS_KEY"),
		"S3_SECRET_KEY": os.Getenv("S3_SECRET_KEY"),
		"S3_SECURE":     os.Getenv("S3_SECURE"),
		"HTTP_TIMEOUT":  os.Getenv("HTTP_TIMEOUT"),
		"PORT":          os.Getenv("PORT"),
		"LOG_LEVEL":     os.Getenv("LOG_LEVEL"),
	}

	// Restore env vars after test
	defer func() {
		for k, v := range origVars {
			if v == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, v)
			}
		}
	}()

	tests := []struct {
		name         string
		envVars      map[string]string
		wantErr      bool
		wantBackend  string
		wantEndpoint string
		wantBucket   string
		wantAccess   string
		wantSecret   string
		wantSecure   bool
		wantTimeout  time.Duration
		wantPort     string
		wantLogLevel string
	}{
		{
			name: "valid s3 config with all env vars",
			envVars: map[string]string{
				"BACKEND_TYPE":  "s3",
				"S3_ENDPOINT":   "localhost:9000",
				"S3_BUCKET":     "test-bucket",
				"S3_ACCESS_KEY": "myaccesskey",
				"S3_SECRET_KEY": "mysecretkey",
				"S3_SECURE":     "true",
				"HTTP_TIMEOUT":  "5s",
				"PORT":          "9090",
				"LOG_LEVEL":     "debug",
			},
			wantErr:      false,
			wantBackend:  "s3",
			wantEndpoint: "localhost:9000",
			wantBucket:   "test-bucket",
			wantAccess:   "myaccesskey",
			wantSecret:   "mysecretkey",
			wantSecure:   true,
			wantTimeout:  5 * time.Second,
			wantPort:     "9090",
			wantLogLevel: "debug",
		},
		{
			name: "valid s3 config with defaults (no BACKEND_TYPE)",
			envVars: map[string]string{
				"S3_ENDPOINT":   "minio:9000",
				"S3_BUCKET":     "volsync-backup",
				"S3_ACCESS_KEY": "accesskey",
				"S3_SECRET_KEY": "secretkey",
			},
			wantErr:      false,
			wantBackend:  "s3",
			wantEndpoint: "minio:9000",
			wantBucket:   "volsync-backup",
			wantAccess:   "accesskey",
			wantSecret:   "secretkey",
			wantSecure:   false,
			wantTimeout:  3 * time.Second,
			wantPort:     "8080",
			wantLogLevel: "info",
		},
		{
			name: "missing S3_ENDPOINT",
			envVars: map[string]string{
				"BACKEND_TYPE":  "s3",
				"S3_BUCKET":     "test-bucket",
				"S3_ACCESS_KEY": "accesskey",
				"S3_SECRET_KEY": "secretkey",
			},
			wantErr: true,
		},
		{
			name: "missing S3_BUCKET",
			envVars: map[string]string{
				"BACKEND_TYPE":  "s3",
				"S3_ENDPOINT":   "localhost:9000",
				"S3_ACCESS_KEY": "accesskey",
				"S3_SECRET_KEY": "secretkey",
			},
			wantErr: true,
		},
		{
			name: "missing S3_ACCESS_KEY",
			envVars: map[string]string{
				"BACKEND_TYPE":  "s3",
				"S3_ENDPOINT":   "localhost:9000",
				"S3_BUCKET":     "test-bucket",
				"S3_SECRET_KEY": "secretkey",
			},
			wantErr: true,
		},
		{
			name: "missing S3_SECRET_KEY",
			envVars: map[string]string{
				"BACKEND_TYPE":  "s3",
				"S3_ENDPOINT":   "localhost:9000",
				"S3_BUCKET":     "test-bucket",
				"S3_ACCESS_KEY": "accesskey",
			},
			wantErr: true,
		},
		{
			name: "invalid HTTP_TIMEOUT",
			envVars: map[string]string{
				"BACKEND_TYPE":  "s3",
				"S3_ENDPOINT":   "localhost:9000",
				"S3_BUCKET":     "test-bucket",
				"S3_ACCESS_KEY": "accesskey",
				"S3_SECRET_KEY": "secretkey",
				"HTTP_TIMEOUT":  "invalid",
			},
			wantErr: true,
		},
		{
			name: "invalid S3_SECURE",
			envVars: map[string]string{
				"BACKEND_TYPE":  "s3",
				"S3_ENDPOINT":   "localhost:9000",
				"S3_BUCKET":     "test-bucket",
				"S3_ACCESS_KEY": "accesskey",
				"S3_SECRET_KEY": "secretkey",
				"S3_SECURE":     "notabool",
			},
			wantErr: true,
		},
		{
			name: "timeout with milliseconds",
			envVars: map[string]string{
				"S3_ENDPOINT":   "localhost:9000",
				"S3_BUCKET":     "test-bucket",
				"S3_ACCESS_KEY": "accesskey",
				"S3_SECRET_KEY": "secretkey",
				"HTTP_TIMEOUT":  "500ms",
			},
			wantErr:      false,
			wantBackend:  "s3",
			wantEndpoint: "localhost:9000",
			wantBucket:   "test-bucket",
			wantAccess:   "accesskey",
			wantSecret:   "secretkey",
			wantSecure:   false,
			wantTimeout:  500 * time.Millisecond,
			wantPort:     "8080",
			wantLogLevel: "info",
		},
		{
			name: "invalid backend type",
			envVars: map[string]string{
				"BACKEND_TYPE": "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all env vars
			for k := range origVars {
				_ = os.Unsetenv(k)
			}

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

			if cfg.BackendType != tt.wantBackend {
				t.Errorf("BackendType = %v, want %v", cfg.BackendType, tt.wantBackend)
			}

			if cfg.S3Endpoint != tt.wantEndpoint {
				t.Errorf("S3Endpoint = %v, want %v", cfg.S3Endpoint, tt.wantEndpoint)
			}

			if cfg.S3Bucket != tt.wantBucket {
				t.Errorf("S3Bucket = %v, want %v", cfg.S3Bucket, tt.wantBucket)
			}

			if cfg.S3AccessKey != tt.wantAccess {
				t.Errorf("S3AccessKey = %v, want %v", cfg.S3AccessKey, tt.wantAccess)
			}

			if cfg.S3SecretKey != tt.wantSecret {
				t.Errorf("S3SecretKey = %v, want %v", cfg.S3SecretKey, tt.wantSecret)
			}

			if cfg.S3Secure != tt.wantSecure {
				t.Errorf("S3Secure = %v, want %v", cfg.S3Secure, tt.wantSecure)
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

func TestLoad_KopiaBackend(t *testing.T) {
	// Save original env vars
	origVars := map[string]string{
		"BACKEND_TYPE":          os.Getenv("BACKEND_TYPE"),
		"KOPIA_REPOSITORY_PATH": os.Getenv("KOPIA_REPOSITORY_PATH"),
		"KOPIA_PASSWORD":        os.Getenv("KOPIA_PASSWORD"),
		"HTTP_TIMEOUT":          os.Getenv("HTTP_TIMEOUT"),
		"PORT":                  os.Getenv("PORT"),
		"LOG_LEVEL":             os.Getenv("LOG_LEVEL"),
	}

	// Restore env vars after test
	defer func() {
		for k, v := range origVars {
			if v == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, v)
			}
		}
	}()

	// Create a temp directory for testing
	tmpDir := t.TempDir()

	tests := []struct {
		name          string
		envVars       map[string]string
		wantErr       bool
		wantBackend   string
		wantKopiaPath string
		wantTimeout   time.Duration
		wantPort      string
		wantLogLevel  string
	}{
		{
			name: "valid kopia-fs config",
			envVars: map[string]string{
				"BACKEND_TYPE":          "kopia-fs",
				"KOPIA_REPOSITORY_PATH": tmpDir,
				"KOPIA_PASSWORD":        "testpassword",
				"HTTP_TIMEOUT":          "5s",
				"PORT":                  "9090",
				"LOG_LEVEL":             "debug",
			},
			wantErr:       false,
			wantBackend:   "kopia-fs",
			wantKopiaPath: tmpDir,
			wantTimeout:   5 * time.Second,
			wantPort:      "9090",
			wantLogLevel:  "debug",
		},
		{
			name: "kopia-fs with default path that doesn't exist",
			envVars: map[string]string{
				"BACKEND_TYPE": "kopia-fs",
				// KOPIA_REPOSITORY_PATH not set, defaults to /repository which likely doesn't exist
			},
			wantErr: true,
		},
		{
			name: "kopia-fs with non-existent path",
			envVars: map[string]string{
				"BACKEND_TYPE":          "kopia-fs",
				"KOPIA_REPOSITORY_PATH": "/nonexistent/path/to/repo",
				"KOPIA_PASSWORD":        "testpassword",
			},
			wantErr: true,
		},
		{
			name: "kopia-fs missing password",
			envVars: map[string]string{
				"BACKEND_TYPE":          "kopia-fs",
				"KOPIA_REPOSITORY_PATH": tmpDir,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all env vars
			for k := range origVars {
				_ = os.Unsetenv(k)
			}

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

			if cfg.BackendType != tt.wantBackend {
				t.Errorf("BackendType = %v, want %v", cfg.BackendType, tt.wantBackend)
			}

			if cfg.KopiaRepositoryPath != tt.wantKopiaPath {
				t.Errorf("KopiaRepositoryPath = %v, want %v", cfg.KopiaRepositoryPath, tt.wantKopiaPath)
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
