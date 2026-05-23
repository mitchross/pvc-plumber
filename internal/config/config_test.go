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
	envBackendType        = "BACKEND_TYPE"
	envS3Endpoint         = "S3_ENDPOINT"
	envS3Bucket           = "S3_BUCKET"
	envS3AccessKey        = "S3_ACCESS_KEY"
	envS3SecretKey        = "S3_SECRET_KEY"
	envS3Secure           = "S3_SECURE"
	envHTTPTimeout        = "HTTP_TIMEOUT"
	envPort               = "PORT"
	envLogLevel           = "LOG_LEVEL"
	envKopiaPassword      = "KOPIA_PASSWORD"
	envKopiaS3Endpoint    = "KOPIA_S3_ENDPOINT"
	envKopiaS3Bucket      = "KOPIA_S3_BUCKET"
	envKopiaS3DisableTLS  = "KOPIA_S3_DISABLE_TLS"
	envAWSAccessKeyID     = "AWS_ACCESS_KEY_ID"
	envAWSSecretAccessKey = "AWS_SECRET_ACCESS_KEY"

	// ExternalSecret-rendering env vars (loaded unconditionally; defaults
	// pin to the reference cluster's 1Password Connect layout).
	envESStoreName             = "EXTERNAL_SECRETS_STORE_NAME"
	envESVaultKey              = "EXTERNAL_SECRETS_VAULT_KEY"
	envESKopiaPasswordProperty = "EXTERNAL_SECRETS_KOPIA_PASSWORD_PROPERTY"
	envESS3AccessKeyProperty   = "EXTERNAL_SECRETS_S3_ACCESS_KEY_PROPERTY"
	envESS3SecretKeyProperty   = "EXTERNAL_SECRETS_S3_SECRET_KEY_PROPERTY"

	// Repeated test values across multiple table rows.
	testEndpoint = "localhost:9000"
	testBucket   = "test-bucket"
	testAccess   = "accesskey"
	testSecret   = "secretkey"
	testLogDebug = "debug"
	testLogInfo  = "info"

	// Kopia-S3 fixture values used across the kopia-s3 backend table.
	testKopiaBucket   = "kopia"
	testKopiaEndpoint = "http://example.com"
	testKopiaPassword = "kp"
)

// allEnvVars is every env var Load() may read. Save / clear / restore in
// every test case so the table runs are hermetic regardless of ambient env.
var allEnvVars = []string{
	envBackendType, envS3Endpoint, envS3Bucket, envS3AccessKey, envS3SecretKey,
	envS3Secure, envHTTPTimeout, envPort, envLogLevel,
	envKopiaPassword, envKopiaS3Endpoint, envKopiaS3Bucket, envKopiaS3DisableTLS,
	envAWSAccessKeyID, envAWSSecretAccessKey,
	envESStoreName, envESVaultKey, envESKopiaPasswordProperty,
	envESS3AccessKeyProperty, envESS3SecretKeyProperty,
	"RE_WARM_INTERVAL",
	// v3.1.0 lazy-credentials env vars
	envKopiaCredentialsPath, envKopiaConnectTimeout,
}

// v3.1.0 env-var names. Promoted to constants because the test file
// references them in multiple table-row literals (goconst flagged
// `KOPIA_CREDENTIALS_PATH` at 5 occurrences).
const (
	envKopiaCredentialsPath = "KOPIA_CREDENTIALS_PATH"
	envKopiaConnectTimeout  = "KOPIA_CONNECT_TIMEOUT"
)

// snapshotEnv saves the current values of allEnvVars; restoreEnv puts them
// back. Used as t.Cleanup so even a failing test doesn't leak env into the
// next case.
func snapshotEnv() map[string]string {
	out := make(map[string]string, len(allEnvVars))
	for _, k := range allEnvVars {
		out[k] = os.Getenv(k)
	}
	return out
}

func restoreEnv(saved map[string]string) {
	for k, v := range saved {
		if v == "" {
			_ = os.Unsetenv(k)
		} else {
			_ = os.Setenv(k, v)
		}
	}
}

func clearAllEnv() {
	for _, k := range allEnvVars {
		_ = os.Unsetenv(k)
	}
}

func TestLoad_S3Backend(t *testing.T) {
	saved := snapshotEnv()
	t.Cleanup(func() { restoreEnv(saved) })

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
			clearAllEnv()
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
	saved := snapshotEnv()
	t.Cleanup(func() { restoreEnv(saved) })

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
			clearAllEnv()
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

// TestLoad_KopiaS3Backend exercises the v3.0.0 kopia-s3 backend path.
// Replaces the v2 TestLoad_KopiaBackend (which validated KOPIA_REPOSITORY_PATH
// stat-on-disk semantics that no longer exist).
func TestLoad_KopiaS3Backend(t *testing.T) {
	saved := snapshotEnv()
	t.Cleanup(func() { restoreEnv(saved) })

	tests := []struct {
		name           string
		envVars        map[string]string
		wantErr        bool
		wantEndpoint   string
		wantBucket     string
		wantAccess     string
		wantSecret     string
		wantPassword   string
		wantDisableTLS bool
	}{
		{
			name: "valid kopia-s3 with --disable-tls",
			envVars: map[string]string{
				envBackendType:        backend.TypeKopiaS3,
				envKopiaS3Endpoint:    "http://192.168.10.133:30293",
				envKopiaS3Bucket:      "volsync-kopia",
				envAWSAccessKeyID:     testAccess,
				envAWSSecretAccessKey: testSecret,
				envKopiaS3DisableTLS:  "true",
				envKopiaPassword:      testKopiaPassword,
			},
			wantEndpoint:   "http://192.168.10.133:30293",
			wantBucket:     "volsync-kopia",
			wantAccess:     testAccess,
			wantSecret:     testSecret,
			wantPassword:   testKopiaPassword,
			wantDisableTLS: true,
		},
		{
			name: "valid kopia-s3 without --disable-tls (default false)",
			envVars: map[string]string{
				envBackendType:        backend.TypeKopiaS3,
				envKopiaS3Endpoint:    "https://s3.example.com",
				envKopiaS3Bucket:      testKopiaBucket,
				envAWSAccessKeyID:     testAccess,
				envAWSSecretAccessKey: testSecret,
				envKopiaPassword:      testKopiaPassword,
			},
			wantEndpoint:   "https://s3.example.com",
			wantBucket:     testKopiaBucket,
			wantAccess:     testAccess,
			wantSecret:     testSecret,
			wantPassword:   testKopiaPassword,
			wantDisableTLS: false,
		},
		{
			name: "kopia-s3 missing endpoint",
			envVars: map[string]string{
				envBackendType:        backend.TypeKopiaS3,
				envKopiaS3Bucket:      testKopiaBucket,
				envAWSAccessKeyID:     testAccess,
				envAWSSecretAccessKey: testSecret,
				envKopiaPassword:      testKopiaPassword,
			},
			wantErr: true,
		},
		{
			name: "kopia-s3 missing bucket",
			envVars: map[string]string{
				envBackendType:        backend.TypeKopiaS3,
				envKopiaS3Endpoint:    testKopiaEndpoint,
				envAWSAccessKeyID:     testAccess,
				envAWSSecretAccessKey: testSecret,
				envKopiaPassword:      testKopiaPassword,
			},
			wantErr: true,
		},
		{
			// v3.1.0: env-var creds are OPTIONAL when KOPIA_CREDENTIALS_PATH
			// is set (default). The operator deployment shape mounts a
			// Secret directory and reads creds from disk at call time. Env-
			// var creds remain valid (legacy HTTP-only deployment shape) but
			// are no longer required. This test row pins that an absent
			// env-var trio is not an error in the default config — see
			// TestLoad_KopiaS3Backend_PathlessRequiresEnvVarCreds for the
			// inverse, where KOPIA_CREDENTIALS_PATH is explicitly empty.
			name: "kopia-s3 missing all env-var creds (path-based default)",
			envVars: map[string]string{
				envBackendType:     backend.TypeKopiaS3,
				envKopiaS3Endpoint: testKopiaEndpoint,
				envKopiaS3Bucket:   testKopiaBucket,
			},
			wantErr:      false,
			wantEndpoint: testKopiaEndpoint,
			wantBucket:   testKopiaBucket,
		},
		{
			name: "kopia-s3 invalid disable-tls",
			envVars: map[string]string{
				envBackendType:        backend.TypeKopiaS3,
				envKopiaS3Endpoint:    testKopiaEndpoint,
				envKopiaS3Bucket:      testKopiaBucket,
				envAWSAccessKeyID:     testAccess,
				envAWSSecretAccessKey: testSecret,
				envKopiaS3DisableTLS:  "definitely-not-a-bool",
				envKopiaPassword:      testKopiaPassword,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearAllEnv()
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

			if cfg.BackendType != backend.TypeKopiaS3 {
				t.Errorf("BackendType = %v, want %v", cfg.BackendType, backend.TypeKopiaS3)
			}
			if cfg.KopiaS3Endpoint != tt.wantEndpoint {
				t.Errorf("KopiaS3Endpoint = %v, want %v", cfg.KopiaS3Endpoint, tt.wantEndpoint)
			}
			if cfg.KopiaS3Bucket != tt.wantBucket {
				t.Errorf("KopiaS3Bucket = %v, want %v", cfg.KopiaS3Bucket, tt.wantBucket)
			}
			if cfg.KopiaS3AccessKey != tt.wantAccess {
				t.Errorf("KopiaS3AccessKey = %v, want %v", cfg.KopiaS3AccessKey, tt.wantAccess)
			}
			if cfg.KopiaS3SecretKey != tt.wantSecret {
				t.Errorf("KopiaS3SecretKey = %v, want %v", cfg.KopiaS3SecretKey, tt.wantSecret)
			}
			if cfg.KopiaPassword != tt.wantPassword {
				t.Errorf("KopiaPassword = %v, want %v", cfg.KopiaPassword, tt.wantPassword)
			}
			if cfg.KopiaS3DisableTLS != tt.wantDisableTLS {
				t.Errorf("KopiaS3DisableTLS = %v, want %v", cfg.KopiaS3DisableTLS, tt.wantDisableTLS)
			}
		})
	}
}

// TestLoad_KopiaS3Backend_DefaultsCredentialsPath pins the v3.1.0 default:
// KOPIA_CREDENTIALS_PATH defaults to /var/secret/pvc-plumber-kopia (matches
// the operator deployment.yaml volumeMount in the consuming GitOps repo) so
// a fresh deployment with the new manifest shape gets path-based creds out
// of the box. KOPIA_CONNECT_TIMEOUT defaults to 60s.
func TestLoad_KopiaS3Backend_DefaultsCredentialsPath(t *testing.T) {
	saved := snapshotEnv()
	t.Cleanup(func() { restoreEnv(saved) })
	clearAllEnv()

	_ = os.Setenv(envBackendType, backend.TypeKopiaS3)
	_ = os.Setenv(envKopiaS3Endpoint, testKopiaEndpoint)
	_ = os.Setenv(envKopiaS3Bucket, testKopiaBucket)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.KopiaCredentialsPath != "/var/secret/pvc-plumber-kopia" {
		t.Errorf("KopiaCredentialsPath = %q, want %q", cfg.KopiaCredentialsPath, "/var/secret/pvc-plumber-kopia")
	}
	if cfg.KopiaConnectTimeout != 60*time.Second {
		t.Errorf("KopiaConnectTimeout = %v, want 60s", cfg.KopiaConnectTimeout)
	}
}

// TestLoad_KopiaS3Backend_PathlessRequiresEnvVarCreds pins the inverse: when
// KOPIA_CREDENTIALS_PATH is explicitly emptied (legacy HTTP-only deployment
// shape), the three env-var creds become required again. This keeps v1.x
// callers compiling and running unchanged.
func TestLoad_KopiaS3Backend_PathlessRequiresEnvVarCreds(t *testing.T) {
	saved := snapshotEnv()
	t.Cleanup(func() { restoreEnv(saved) })

	cases := []struct {
		name       string
		envVars    map[string]string
		wantErrSub string
	}{
		{
			name: "missing password",
			envVars: map[string]string{
				envBackendType:          backend.TypeKopiaS3,
				envKopiaS3Endpoint:      testKopiaEndpoint,
				envKopiaS3Bucket:        testKopiaBucket,
				envAWSAccessKeyID:       testAccess,
				envAWSSecretAccessKey:   testSecret,
				envKopiaCredentialsPath: "",
			},
			wantErrSub: "KOPIA_PASSWORD",
		},
		{
			name: "missing access key",
			envVars: map[string]string{
				envBackendType:          backend.TypeKopiaS3,
				envKopiaS3Endpoint:      testKopiaEndpoint,
				envKopiaS3Bucket:        testKopiaBucket,
				envKopiaPassword:        testKopiaPassword,
				envAWSSecretAccessKey:   testSecret,
				envKopiaCredentialsPath: "",
			},
			wantErrSub: "AWS_ACCESS_KEY_ID",
		},
		{
			name: "missing secret key",
			envVars: map[string]string{
				envBackendType:          backend.TypeKopiaS3,
				envKopiaS3Endpoint:      testKopiaEndpoint,
				envKopiaS3Bucket:        testKopiaBucket,
				envKopiaPassword:        testKopiaPassword,
				envAWSAccessKeyID:       testAccess,
				envKopiaCredentialsPath: "",
			},
			wantErrSub: "AWS_SECRET_ACCESS_KEY",
		},
		{
			name: "all three present (env-var legacy shape)",
			envVars: map[string]string{
				envBackendType:          backend.TypeKopiaS3,
				envKopiaS3Endpoint:      testKopiaEndpoint,
				envKopiaS3Bucket:        testKopiaBucket,
				envKopiaPassword:        testKopiaPassword,
				envAWSAccessKeyID:       testAccess,
				envAWSSecretAccessKey:   testSecret,
				envKopiaCredentialsPath: "",
			},
			wantErrSub: "", // happy path
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearAllEnv()
			for k, v := range tc.envVars {
				_ = os.Setenv(k, v)
			}
			_, err := Load()
			if tc.wantErrSub == "" {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErrSub)
			}
			if !contains(err.Error(), tc.wantErrSub) {
				t.Errorf("error = %v, want substring %q", err, tc.wantErrSub)
			}
		})
	}
}

// TestLoad_KopiaS3Backend_ConnectTimeoutOverride pins the env-var override
// path for KOPIA_CONNECT_TIMEOUT.
func TestLoad_KopiaS3Backend_ConnectTimeoutOverride(t *testing.T) {
	saved := snapshotEnv()
	t.Cleanup(func() { restoreEnv(saved) })

	cases := []struct {
		name    string
		raw     string
		wantErr bool
		want    time.Duration
	}{
		{"explicit 30s", "30s", false, 30 * time.Second},
		{"explicit 2m", "2m", false, 2 * time.Minute},
		{"unparseable rejected", "garbage", true, 0},
		{"zero rejected", "0s", true, 0},
		{"negative rejected", "-5s", true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearAllEnv()
			_ = os.Setenv(envBackendType, backend.TypeKopiaS3)
			_ = os.Setenv(envKopiaS3Endpoint, testKopiaEndpoint)
			_ = os.Setenv(envKopiaS3Bucket, testKopiaBucket)
			_ = os.Setenv(envKopiaConnectTimeout, tc.raw)

			cfg, err := Load()
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for %q, got nil", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.KopiaConnectTimeout != tc.want {
				t.Errorf("KopiaConnectTimeout = %v, want %v", cfg.KopiaConnectTimeout, tc.want)
			}
		})
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// TestLoad_ExternalSecretsConfigDefaults pins the per-PVC ExternalSecret
// rendering knobs' defaults — they MUST match the reference cluster's
// 1Password Connect layout (vault item `rustfs`, kopia_password +
// k8s-admin-{access,secret}-key properties). If a defaults change is
// intentional, update both the loader and this test in the same commit.
func TestLoad_ExternalSecretsConfigDefaults(t *testing.T) {
	saved := snapshotEnv()
	t.Cleanup(func() { restoreEnv(saved) })

	clearAllEnv()
	_ = os.Setenv(envBackendType, "s3")
	_ = os.Setenv(envS3Endpoint, testEndpoint)
	_ = os.Setenv(envS3Bucket, "bucket")
	_ = os.Setenv(envS3AccessKey, "k")
	_ = os.Setenv(envS3SecretKey, "s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.ExternalSecretsStoreName != "1password" {
		t.Errorf("default ExternalSecretsStoreName = %q, want %q", cfg.ExternalSecretsStoreName, "1password")
	}
	if cfg.ExternalSecretsVaultKey != "rustfs" {
		t.Errorf("default ExternalSecretsVaultKey = %q, want %q", cfg.ExternalSecretsVaultKey, "rustfs")
	}
	if cfg.ExternalSecretsKopiaPasswordProperty != "kopia_password" {
		t.Errorf("default ExternalSecretsKopiaPasswordProperty = %q", cfg.ExternalSecretsKopiaPasswordProperty)
	}
	if cfg.ExternalSecretsS3AccessKeyProperty != "k8s-admin-access-key" {
		t.Errorf("default ExternalSecretsS3AccessKeyProperty = %q", cfg.ExternalSecretsS3AccessKeyProperty)
	}
	if cfg.ExternalSecretsS3SecretKeyProperty != "k8s-admin-secret-key" {
		t.Errorf("default ExternalSecretsS3SecretKeyProperty = %q", cfg.ExternalSecretsS3SecretKeyProperty)
	}
}

// TestLoad_ExternalSecretsConfigOverrides exercises the env-var override
// surface for the per-PVC ES rendering. v2 quirks #1/#2/#3 from
// MIGRATION-v1-to-v2.md were "store name / vault item / property are
// hardcoded"; this test pins that v3.0.0 made them configurable.
func TestLoad_ExternalSecretsConfigOverrides(t *testing.T) {
	saved := snapshotEnv()
	t.Cleanup(func() { restoreEnv(saved) })

	clearAllEnv()
	_ = os.Setenv(envBackendType, "s3")
	_ = os.Setenv(envS3Endpoint, testEndpoint)
	_ = os.Setenv(envS3Bucket, "bucket")
	_ = os.Setenv(envS3AccessKey, "k")
	_ = os.Setenv(envS3SecretKey, "s")
	_ = os.Setenv(envESStoreName, "vault-prod")
	_ = os.Setenv(envESVaultKey, "kopia-creds")
	_ = os.Setenv(envESKopiaPasswordProperty, "repo_password")
	_ = os.Setenv(envESS3AccessKeyProperty, "access_key")
	_ = os.Setenv(envESS3SecretKeyProperty, "secret_key")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.ExternalSecretsStoreName != "vault-prod" {
		t.Errorf("ExternalSecretsStoreName = %q, want %q", cfg.ExternalSecretsStoreName, "vault-prod")
	}
	if cfg.ExternalSecretsVaultKey != "kopia-creds" {
		t.Errorf("ExternalSecretsVaultKey = %q, want %q", cfg.ExternalSecretsVaultKey, "kopia-creds")
	}
	if cfg.ExternalSecretsKopiaPasswordProperty != "repo_password" {
		t.Errorf("ExternalSecretsKopiaPasswordProperty = %q", cfg.ExternalSecretsKopiaPasswordProperty)
	}
	if cfg.ExternalSecretsS3AccessKeyProperty != "access_key" {
		t.Errorf("ExternalSecretsS3AccessKeyProperty = %q", cfg.ExternalSecretsS3AccessKeyProperty)
	}
	if cfg.ExternalSecretsS3SecretKeyProperty != "secret_key" {
		t.Errorf("ExternalSecretsS3SecretKeyProperty = %q", cfg.ExternalSecretsS3SecretKeyProperty)
	}
}

// TestLoadWithOptions_SkipBackend covers the Phase 2.5d audit-mode startup
// contract: when SkipBackend is true, the config loader must succeed without
// any backend env vars set, and must NOT default BACKEND_TYPE to "s3".
//
// This is the regression guard for the Phase 3 deployment.yaml crash where
// the audit-mode pod CrashLoopBackOff'd on "S3_ENDPOINT is required" because
// the (defaulted) BACKEND_TYPE=s3 triggered loadS3Config validation that
// requires backend credentials the audit-mode binary doesn't actually use.
func TestLoadWithOptions_SkipBackend_NoEnv(t *testing.T) {
	clearAllEnv()

	cfg, err := LoadWithOptions(LoadOptions{SkipBackend: true})
	if err != nil {
		t.Fatalf("LoadWithOptions(SkipBackend=true) without env returned error: %v", err)
	}
	if cfg == nil {
		t.Fatal("LoadWithOptions returned nil config without error")
	}
	if cfg.BackendType != "" {
		t.Errorf("BackendType: got %q, want empty (no defaulting in skip mode)", cfg.BackendType)
	}
	if cfg.S3Endpoint != "" {
		t.Errorf("S3Endpoint: got %q, want empty", cfg.S3Endpoint)
	}
	if cfg.KopiaS3Endpoint != "" {
		t.Errorf("KopiaS3Endpoint: got %q, want empty", cfg.KopiaS3Endpoint)
	}
	if cfg.KopiaS3AccessKey != "" {
		t.Errorf("KopiaS3AccessKey: got %q, want empty", cfg.KopiaS3AccessKey)
	}
	if cfg.KopiaS3SecretKey != "" {
		t.Errorf("KopiaS3SecretKey: got %q, want empty", cfg.KopiaS3SecretKey)
	}
	if cfg.KopiaPassword != "" {
		t.Errorf("KopiaPassword: got %q, want empty", cfg.KopiaPassword)
	}
	if cfg.LogLevel == "" {
		t.Errorf("LogLevel: should have a default even in skip mode, got empty")
	}
	if cfg.Port == "" {
		t.Errorf("Port: should have a default even in skip mode, got empty")
	}
}

// TestLoadWithOptions_SkipBackend_IgnoresPartialEnv confirms that even if
// some backend env vars happen to be set, SkipBackend doesn't read them
// — audit mode must be hermetic.
func TestLoadWithOptions_SkipBackend_IgnoresPartialEnv(t *testing.T) {
	clearAllEnv()
	t.Setenv(envBackendType, "s3")
	// Deliberately NOT setting S3_ENDPOINT/etc. With the legacy Load() this
	// would fail. With SkipBackend it must succeed.

	cfg, err := LoadWithOptions(LoadOptions{SkipBackend: true})
	if err != nil {
		t.Fatalf("SkipBackend should tolerate BACKEND_TYPE=s3 without S3_ENDPOINT: %v", err)
	}
	// BACKEND_TYPE env value passes through verbatim, but no backend-specific
	// validation runs.
	if cfg.BackendType != "s3" {
		t.Errorf("BackendType (env passthrough): got %q, want %q", cfg.BackendType, "s3")
	}
	if cfg.S3Endpoint != "" {
		t.Errorf("S3Endpoint must not be loaded in skip mode: got %q", cfg.S3Endpoint)
	}
}

// TestLoadWithOptions_LegacyMode_StillRequiresBackend protects against
// accidentally weakening the non-audit codepath. The default (skipBackend=false)
// must reject missing backend env vars exactly as the legacy Load() did.
func TestLoadWithOptions_LegacyMode_StillRequiresBackend(t *testing.T) {
	clearAllEnv()
	// No env set — legacy code defaults BACKEND_TYPE=s3 and then errors on
	// missing S3_ENDPOINT.
	_, err := LoadWithOptions(LoadOptions{SkipBackend: false})
	if err == nil {
		t.Fatal("LoadWithOptions(SkipBackend=false) must require backend env; got nil error")
	}
	// And the original Load() wrapper must produce the same error.
	_, err = Load()
	if err == nil {
		t.Fatal("Load() must require backend env; got nil error")
	}
}

// TestLoadWithOptions_LegacyMode_StillRequiresKopiaS3Backend covers the
// non-default backend path (BACKEND_TYPE=kopia-s3): missing KOPIA_S3_ENDPOINT
// must still fail in non-audit mode.
func TestLoadWithOptions_LegacyMode_StillRequiresKopiaS3Backend(t *testing.T) {
	clearAllEnv()
	t.Setenv(envBackendType, "kopia-s3")

	_, err := LoadWithOptions(LoadOptions{SkipBackend: false})
	if err == nil {
		t.Fatal("LoadWithOptions(SkipBackend=false) with BACKEND_TYPE=kopia-s3 must require KOPIA_S3_ENDPOINT")
	}
}
