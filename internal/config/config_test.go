package config

import (
	"os"
	"testing"
	"time"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// Test fixture constants. Centralizing these stops goconst from complaining
// about repeated literals across the table-driven tests, and makes it a
// one-line change to rename a fixture string.
const (
	// Env var names exercised by Load().
	envBackendType   = "BACKEND_TYPE"
	envS3Endpoint    = "S3_ENDPOINT"
	envS3Bucket      = "S3_BUCKET"
	envS3AccessKey   = "S3_ACCESS_KEY"
	envS3SecretKey   = "S3_SECRET_KEY"
	envS3Secure      = "S3_SECURE"
	envHTTPTimeout   = "HTTP_TIMEOUT"
	envPort          = "PORT"
	envLogLevel      = "LOG_LEVEL"
	envKopiaRepoPath = "KOPIA_REPOSITORY_PATH"
	envKopiaPassword = "KOPIA_PASSWORD"

	// Repeated test values across multiple table rows.
	testEndpoint = "localhost:9000"
	testBucket   = "test-bucket"
	testAccess   = "accesskey"
	testSecret   = "secretkey"
	testLogDebug = "debug"
	testLogInfo  = "info"
)

func TestLoad_S3Backend(t *testing.T) {
	// Save original env vars
	origVars := map[string]string{
		envBackendType: os.Getenv(envBackendType),
		envS3Endpoint:  os.Getenv(envS3Endpoint),
		envS3Bucket:    os.Getenv(envS3Bucket),
		envS3AccessKey: os.Getenv(envS3AccessKey),
		envS3SecretKey: os.Getenv(envS3SecretKey),
		envS3Secure:    os.Getenv(envS3Secure),
		envHTTPTimeout: os.Getenv(envHTTPTimeout),
		envPort:        os.Getenv(envPort),
		envLogLevel:    os.Getenv(envLogLevel),
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
				envBackendType: "s3",
				envS3Endpoint:  testEndpoint,
				envS3Bucket:    testBucket,
				envS3AccessKey: "myaccesskey",
				envS3SecretKey: "mysecretkey",
				envS3Secure:    "true",
				envHTTPTimeout: "5s",
				envPort:        "9090",
				envLogLevel:    testLogDebug,
			},
			wantErr:      false,
			wantBackend:  "s3",
			wantEndpoint: testEndpoint,
			wantBucket:   testBucket,
			wantAccess:   "myaccesskey",
			wantSecret:   "mysecretkey",
			wantSecure:   true,
			wantTimeout:  5 * time.Second,
			wantPort:     "9090",
			wantLogLevel: testLogDebug,
		},
		{
			name: "valid s3 config with defaults (no BACKEND_TYPE)",
			envVars: map[string]string{
				envS3Endpoint:  "minio:9000",
				envS3Bucket:    "volsync-backup",
				envS3AccessKey: testAccess,
				envS3SecretKey: testSecret,
			},
			wantErr:      false,
			wantBackend:  "s3",
			wantEndpoint: "minio:9000",
			wantBucket:   "volsync-backup",
			wantAccess:   testAccess,
			wantSecret:   testSecret,
			wantSecure:   false,
			wantTimeout:  3 * time.Second,
			wantPort:     "8080",
			wantLogLevel: testLogInfo,
		},
		{
			name: "missing S3_ENDPOINT",
			envVars: map[string]string{
				envBackendType: "s3",
				envS3Bucket:    testBucket,
				envS3AccessKey: testAccess,
				envS3SecretKey: testSecret,
			},
			wantErr: true,
		},
		{
			name: "missing S3_BUCKET",
			envVars: map[string]string{
				envBackendType: "s3",
				envS3Endpoint:  testEndpoint,
				envS3AccessKey: testAccess,
				envS3SecretKey: testSecret,
			},
			wantErr: true,
		},
		{
			name: "missing S3_ACCESS_KEY",
			envVars: map[string]string{
				envBackendType: "s3",
				envS3Endpoint:  testEndpoint,
				envS3Bucket:    testBucket,
				envS3SecretKey: testSecret,
			},
			wantErr: true,
		},
		{
			name: "missing S3_SECRET_KEY",
			envVars: map[string]string{
				envBackendType: "s3",
				envS3Endpoint:  testEndpoint,
				envS3Bucket:    testBucket,
				envS3AccessKey: testAccess,
			},
			wantErr: true,
		},
		{
			name: "invalid HTTP_TIMEOUT",
			envVars: map[string]string{
				envBackendType: "s3",
				envS3Endpoint:  testEndpoint,
				envS3Bucket:    testBucket,
				envS3AccessKey: testAccess,
				envS3SecretKey: testSecret,
				envHTTPTimeout: "invalid",
			},
			wantErr: true,
		},
		{
			name: "invalid S3_SECURE",
			envVars: map[string]string{
				envBackendType: "s3",
				envS3Endpoint:  testEndpoint,
				envS3Bucket:    testBucket,
				envS3AccessKey: testAccess,
				envS3SecretKey: testSecret,
				envS3Secure:    "notabool",
			},
			wantErr: true,
		},
		{
			name: "timeout with milliseconds",
			envVars: map[string]string{
				envS3Endpoint:  testEndpoint,
				envS3Bucket:    testBucket,
				envS3AccessKey: testAccess,
				envS3SecretKey: testSecret,
				envHTTPTimeout: "500ms",
			},
			wantErr:      false,
			wantBackend:  "s3",
			wantEndpoint: testEndpoint,
			wantBucket:   testBucket,
			wantAccess:   testAccess,
			wantSecret:   testSecret,
			wantSecure:   false,
			wantTimeout:  500 * time.Millisecond,
			wantPort:     "8080",
			wantLogLevel: testLogInfo,
		},
		{
			name: "invalid backend type",
			envVars: map[string]string{
				envBackendType: "invalid",
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

func TestLoad_ReWarmInterval(t *testing.T) {
	origVars := map[string]string{
		envBackendType:     os.Getenv(envBackendType),
		envS3Endpoint:      os.Getenv(envS3Endpoint),
		envS3Bucket:        os.Getenv(envS3Bucket),
		envS3AccessKey:     os.Getenv(envS3AccessKey),
		envS3SecretKey:     os.Getenv(envS3SecretKey),
		"RE_WARM_INTERVAL": os.Getenv("RE_WARM_INTERVAL"),
	}
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
		name    string
		envVar  string
		wantErr bool
		wantInt time.Duration
	}{
		{"default when unset", "", false, 90 * time.Second},
		{"explicit 60s", "60s", false, 60 * time.Second},
		{"explicit 2m", "2m", false, 2 * time.Minute},
		{"zero disables", "0s", false, 0},
		{"negative rejected", "-30s", true, 0},
		{"unparseable rejected", "garbage", true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k := range origVars {
				_ = os.Unsetenv(k)
			}
			_ = os.Setenv(envBackendType, "s3")
			_ = os.Setenv(envS3Endpoint, testEndpoint)
			_ = os.Setenv(envS3Bucket, "bucket")
			_ = os.Setenv(envS3AccessKey, "k")
			_ = os.Setenv(envS3SecretKey, "s")
			if tt.envVar != "" {
				_ = os.Setenv("RE_WARM_INTERVAL", tt.envVar)
			}

			cfg, err := Load()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (cfg=%+v)", cfg)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.ReWarmInterval != tt.wantInt {
				t.Errorf("ReWarmInterval = %v, want %v", cfg.ReWarmInterval, tt.wantInt)
			}
		})
	}
}

func TestLoad_KopiaBackend(t *testing.T) {
	// Save original env vars
	origVars := map[string]string{
		envBackendType:   os.Getenv(envBackendType),
		envKopiaRepoPath: os.Getenv(envKopiaRepoPath),
		envKopiaPassword: os.Getenv(envKopiaPassword),
		envHTTPTimeout:   os.Getenv(envHTTPTimeout),
		envPort:          os.Getenv(envPort),
		envLogLevel:      os.Getenv(envLogLevel),
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
				envBackendType:   backend.TypeKopiaFS,
				envKopiaRepoPath: tmpDir,
				envKopiaPassword: "testpassword",
				envHTTPTimeout:   "5s",
				envPort:          "9090",
				envLogLevel:      testLogDebug,
			},
			wantErr:       false,
			wantBackend:   backend.TypeKopiaFS,
			wantKopiaPath: tmpDir,
			wantTimeout:   5 * time.Second,
			wantPort:      "9090",
			wantLogLevel:  testLogDebug,
		},
		{
			name: "kopia-fs with default path that doesn't exist",
			envVars: map[string]string{
				envBackendType: backend.TypeKopiaFS,
				// KOPIA_REPOSITORY_PATH not set, defaults to /repository which likely doesn't exist
			},
			wantErr: true,
		},
		{
			name: "kopia-fs with non-existent path",
			envVars: map[string]string{
				envBackendType:   backend.TypeKopiaFS,
				envKopiaRepoPath: "/nonexistent/path/to/repo",
				envKopiaPassword: "testpassword",
			},
			wantErr: true,
		},
		{
			name: "kopia-fs missing password",
			envVars: map[string]string{
				envBackendType:   backend.TypeKopiaFS,
				envKopiaRepoPath: tmpDir,
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
