package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// Default values for log level and HTTP port. Centralized so the strings
// don't drift between Load() and tests.
const (
	defaultLogLevel = "info"
	defaultPort     = "8080"
)

type Config struct {
	// Common settings
	BackendType    string
	HTTPTimeout    time.Duration
	CacheTTL       time.Duration
	ReWarmInterval time.Duration // 0 disables the periodic re-warm loop
	Port           string
	LogLevel       string

	// S3 backend settings
	S3Endpoint  string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3Secure    bool

	// Kopia (S3) backend settings. v3.0.0 removed the filesystem-backed
	// kopia repo (KOPIA_REPOSITORY_PATH); the operator now connects to an
	// S3-compatible backend (e.g. RustFS, MinIO) the same way VolSync mover
	// Jobs do, so there is no shared volume between the operator pod and
	// mover pods. KopiaS3DisableTLS=true is the in-cluster RustFS shape.
	KopiaPassword     string
	KopiaS3Endpoint   string
	KopiaS3Bucket     string
	KopiaS3AccessKey  string
	KopiaS3SecretKey  string
	KopiaS3DisableTLS bool

	// ExternalSecret rendering knobs used by the PVC reconciler when it
	// templates the per-PVC `volsync-<pvc>` ExternalSecret. Defaults pin to
	// the reference cluster's 1Password Connect setup (vault item
	// `rustfs`, properties `kopia_password`/`k8s-admin-access-key`/
	// `k8s-admin-secret-key`). Override these for any deployment that uses
	// a different secret store layout.
	ExternalSecretsStoreName             string
	ExternalSecretsVaultKey              string
	ExternalSecretsKopiaPasswordProperty string
	ExternalSecretsS3AccessKeyProperty   string
	ExternalSecretsS3SecretKeyProperty   string
}

func Load() (*Config, error) {
	// Common settings
	backendType := os.Getenv("BACKEND_TYPE")
	if backendType == "" {
		backendType = backend.TypeS3
	}
	if backendType != backend.TypeS3 && backendType != backend.TypeKopiaS3 {
		return nil, fmt.Errorf("invalid BACKEND_TYPE: %s (must be %q or %q)",
			backendType, backend.TypeS3, backend.TypeKopiaS3)
	}

	httpTimeout := 3 * time.Second
	if timeoutStr := os.Getenv("HTTP_TIMEOUT"); timeoutStr != "" {
		duration, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return nil, fmt.Errorf("invalid HTTP_TIMEOUT: %w", err)
		}
		httpTimeout = duration
	}

	cacheTTL := 60 * time.Second
	if ttlStr := os.Getenv("CACHE_TTL"); ttlStr != "" {
		duration, err := time.ParseDuration(ttlStr)
		if err != nil {
			return nil, fmt.Errorf("invalid CACHE_TTL: %w", err)
		}
		cacheTTL = duration
	}

	reWarmInterval := 90 * time.Second
	if intervalStr := os.Getenv("RE_WARM_INTERVAL"); intervalStr != "" {
		duration, err := time.ParseDuration(intervalStr)
		if err != nil {
			return nil, fmt.Errorf("invalid RE_WARM_INTERVAL: %w", err)
		}
		if duration < 0 {
			return nil, fmt.Errorf("RE_WARM_INTERVAL must be >= 0, got %s", intervalStr)
		}
		reWarmInterval = duration
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = defaultLogLevel
	}

	cfg := &Config{
		BackendType:    backendType,
		HTTPTimeout:    httpTimeout,
		CacheTTL:       cacheTTL,
		ReWarmInterval: reWarmInterval,
		Port:           port,
		LogLevel:       logLevel,
	}

	// Backend-specific validation
	switch backendType {
	case backend.TypeS3:
		if err := loadS3Config(cfg); err != nil {
			return nil, err
		}
	case backend.TypeKopiaS3:
		if err := loadKopiaS3Config(cfg); err != nil {
			return nil, err
		}
	}

	// ExternalSecret rendering knobs are independent of the existence
	// backend choice (they configure the per-PVC ES the reconciler renders),
	// so load them unconditionally.
	loadExternalSecretsConfig(cfg)

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

// loadKopiaS3Config loads the env vars the operator's own kopia client uses
// to connect to the shared Kopia repository over S3. Mirrors the env-var
// surface VolSync mover Jobs see (KOPIA_S3_ENDPOINT, KOPIA_S3_BUCKET,
// KOPIA_S3_DISABLE_TLS, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
// KOPIA_PASSWORD) so secret material can be reused 1:1 between the operator
// pod and the per-PVC kopia-credentials Secrets the reconciler creates.
//
// KOPIA_S3_DISABLE_TLS defaults to false; in-cluster RustFS / plaintext-MinIO
// deployments must set it to "true" explicitly.
func loadKopiaS3Config(cfg *Config) error {
	cfg.KopiaS3Endpoint = os.Getenv("KOPIA_S3_ENDPOINT")
	if cfg.KopiaS3Endpoint == "" {
		return fmt.Errorf("KOPIA_S3_ENDPOINT is required for kopia-s3 backend")
	}

	cfg.KopiaS3Bucket = os.Getenv("KOPIA_S3_BUCKET")
	if cfg.KopiaS3Bucket == "" {
		return fmt.Errorf("KOPIA_S3_BUCKET is required for kopia-s3 backend")
	}

	cfg.KopiaS3AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	if cfg.KopiaS3AccessKey == "" {
		return fmt.Errorf("AWS_ACCESS_KEY_ID is required for kopia-s3 backend")
	}

	cfg.KopiaS3SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	if cfg.KopiaS3SecretKey == "" {
		return fmt.Errorf("AWS_SECRET_ACCESS_KEY is required for kopia-s3 backend")
	}

	cfg.KopiaS3DisableTLS = false
	if disableStr := os.Getenv("KOPIA_S3_DISABLE_TLS"); disableStr != "" {
		v, err := strconv.ParseBool(disableStr)
		if err != nil {
			return fmt.Errorf("invalid KOPIA_S3_DISABLE_TLS: %w", err)
		}
		cfg.KopiaS3DisableTLS = v
	}

	cfg.KopiaPassword = os.Getenv("KOPIA_PASSWORD")
	if cfg.KopiaPassword == "" {
		return fmt.Errorf("KOPIA_PASSWORD is required for kopia-s3 backend")
	}

	return nil
}

// loadExternalSecretsConfig reads the (optional, defaulted) env vars that
// parameterize how the PVC reconciler renders each `volsync-<pvc>`
// ExternalSecret. Defaults match the reference cluster's 1Password Connect
// layout (vault item `rustfs`, properties for the kopia password and the
// S3 admin keys). Anyone running on a different secret store overrides
// these per-deployment; the env-var surface is the only place that name
// "1password" / "rustfs" / "kopia_password" appears.
func loadExternalSecretsConfig(cfg *Config) {
	cfg.ExternalSecretsStoreName = os.Getenv("EXTERNAL_SECRETS_STORE_NAME")
	if cfg.ExternalSecretsStoreName == "" {
		cfg.ExternalSecretsStoreName = "1password"
	}
	cfg.ExternalSecretsVaultKey = os.Getenv("EXTERNAL_SECRETS_VAULT_KEY")
	if cfg.ExternalSecretsVaultKey == "" {
		cfg.ExternalSecretsVaultKey = "rustfs"
	}
	cfg.ExternalSecretsKopiaPasswordProperty = os.Getenv("EXTERNAL_SECRETS_KOPIA_PASSWORD_PROPERTY")
	if cfg.ExternalSecretsKopiaPasswordProperty == "" {
		cfg.ExternalSecretsKopiaPasswordProperty = "kopia_password"
	}
	cfg.ExternalSecretsS3AccessKeyProperty = os.Getenv("EXTERNAL_SECRETS_S3_ACCESS_KEY_PROPERTY")
	if cfg.ExternalSecretsS3AccessKeyProperty == "" {
		cfg.ExternalSecretsS3AccessKeyProperty = "k8s-admin-access-key"
	}
	cfg.ExternalSecretsS3SecretKeyProperty = os.Getenv("EXTERNAL_SECRETS_S3_SECRET_KEY_PROPERTY")
	if cfg.ExternalSecretsS3SecretKeyProperty == "" {
		cfg.ExternalSecretsS3SecretKeyProperty = "k8s-admin-secret-key"
	}
}
