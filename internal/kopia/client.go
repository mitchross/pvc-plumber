package kopia

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// CommandExecutor interface for running commands (enables testing).
type CommandExecutor interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

// RealExecutor executes commands using os/exec.
type RealExecutor struct{}

func (e *RealExecutor) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.Output()
}

// Creds bundles the three credential strings every kopia subprocess needs.
// Carried as a struct so the loader returns one value and the connect call
// site doesn't have to track three returns.
type Creds struct {
	Password  string
	AccessKey string
	SecretKey string
}

// kopia CLI subcommand literals — extracted as constants because they
// appear in both the connect path and the HealthCheck status probe, and
// goconst (lint) treats >2 string-literal occurrences as a magic value.
const (
	kopiaCmdRepository = "repository"
	kopiaCmdConnect    = "connect"
	kopiaCmdStatus     = "status"
	kopiaCmdS3         = "s3"
)

// CredentialsSource hides where kopia credentials come from. v3.1.0 defaults
// to a directory-mounted Secret (DirCredentialsSource), but the legacy
// cmd/pvc-plumber/main.go path keeps using StaticCredentialsSource so the v1
// HTTP-only deployment shape continues to compile and run unchanged.
//
// Load is called on every kopia subprocess invocation that needs creds —
// Connect, CheckBackupExists, ListAllSources — so a Secret update from the
// External Secrets Operator is picked up on the next call without restarting
// the operator pod. That property is the whole point of this indirection:
// pre-v3.1.0 the operator read AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
// KOPIA_PASSWORD via secretKeyRef env vars at pod startup, which made pod
// startup race against ESO and failed with CreateContainerConfigError when
// the Secret was mid-update during ArgoCD sync waves.
type CredentialsSource interface {
	Load() (Creds, error)
}

// ErrCredentialsNotReady is returned by a CredentialsSource when the backing
// material isn't available yet (file missing, file empty, Secret not
// rendered, …). Callers distinguish this from "kopia repo error" via
// errors.Is — on this error class we retry with backoff rather than failing
// the operation.
var ErrCredentialsNotReady = errors.New("kopia credentials not ready")

// DirCredentialsSource reads each credential from a separate file under a
// directory, the shape kubelet writes when a Secret is mounted as a volume.
// Each key in the Secret becomes a file with the same name (e.g.
// AWS_ACCESS_KEY_ID → <dir>/AWS_ACCESS_KEY_ID).
//
// This sidesteps the v3.0.0 ES-race pod-startup deadlock: if any file is
// missing or empty when Load() is called, we return ErrCredentialsNotReady
// and let the caller retry. The pod itself starts cleanly regardless of the
// Secret's render state — there's no kubelet-level secretKeyRef resolution
// to fail.
type DirCredentialsSource struct {
	Dir string
}

// NewDirCredentialsSource constructs a CredentialsSource that reads files
// from the given mount directory. The default mount path
// (`/var/secret/pvc-plumber-kopia`) lines up with the deployment.yaml
// volumeMount in the consuming GitOps repo.
func NewDirCredentialsSource(dir string) *DirCredentialsSource {
	return &DirCredentialsSource{Dir: dir}
}

// Load reads the three credential files. On any read failure or empty value,
// returns ErrCredentialsNotReady wrapping the underlying error so the caller
// can backoff-and-retry rather than treating it as a hard failure. Trailing
// whitespace (newline left by `kubectl create secret` etc.) is trimmed.
func (d *DirCredentialsSource) Load() (Creds, error) {
	if d.Dir == "" {
		return Creds{}, fmt.Errorf("%w: credentials path is empty", ErrCredentialsNotReady)
	}
	pw, err := readSecretFile(filepath.Join(d.Dir, "KOPIA_PASSWORD"))
	if err != nil {
		return Creds{}, fmt.Errorf("%w: %w", ErrCredentialsNotReady, err)
	}
	ak, err := readSecretFile(filepath.Join(d.Dir, "AWS_ACCESS_KEY_ID"))
	if err != nil {
		return Creds{}, fmt.Errorf("%w: %w", ErrCredentialsNotReady, err)
	}
	sk, err := readSecretFile(filepath.Join(d.Dir, "AWS_SECRET_ACCESS_KEY"))
	if err != nil {
		return Creds{}, fmt.Errorf("%w: %w", ErrCredentialsNotReady, err)
	}
	return Creds{Password: pw, AccessKey: ak, SecretKey: sk}, nil
}

// readSecretFile reads a single Secret file and trims trailing whitespace.
// An empty file is treated the same as a missing file — both surface as
// "not ready".
func readSecretFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	v := strings.TrimRight(string(b), "\r\n\t ")
	if v == "" {
		return "", fmt.Errorf("file %s is empty", path)
	}
	return v, nil
}

// StaticCredentialsSource returns the three credentials it was constructed
// with on every Load. Used by the legacy HTTP-only cmd/pvc-plumber/main.go
// where credentials still come from secretKeyRef env vars. Operator-mode
// callers should use DirCredentialsSource so the ES-race fix applies.
type StaticCredentialsSource struct {
	creds Creds
}

// NewStaticCredentialsSource constructs a CredentialsSource that always
// returns the supplied creds. Returns ErrCredentialsNotReady if any field
// is empty so test fixtures and misconfigured callers fail in the same way
// as a missing-file dir source.
func NewStaticCredentialsSource(password, accessKey, secretKey string) *StaticCredentialsSource {
	return &StaticCredentialsSource{
		creds: Creds{Password: password, AccessKey: accessKey, SecretKey: secretKey},
	}
}

func (s *StaticCredentialsSource) Load() (Creds, error) {
	if s.creds.Password == "" || s.creds.AccessKey == "" || s.creds.SecretKey == "" {
		return Creds{}, fmt.Errorf("%w: static credentials missing one or more fields", ErrCredentialsNotReady)
	}
	return s.creds, nil
}

// S3Config bundles the static (non-credential) inputs `kopia repository
// connect s3` needs. v3.1.0 split credentials out of this struct — they're
// loaded lazily through CredentialsSource on each call so a Secret update
// from ESO is picked up without a pod restart. The constructor for
// S3Config no longer takes Password / AccessKey / SecretKey.
type S3Config struct {
	Endpoint   string
	Bucket     string
	DisableTLS bool
}

// Client wraps the kopia CLI for backup-existence checks against an S3-backed
// Kopia repository. v3.1.0 lazy-loads credentials from a CredentialsSource on
// every subprocess invocation, so a Secret update via ESO is observed
// without a pod restart and a Secret that hasn't rendered yet doesn't crash
// the pod at startup.
type Client struct {
	cfg            S3Config
	creds          CredentialsSource
	connectTimeout time.Duration
	logger         *slog.Logger
	executor       CommandExecutor

	// connected is set true once Connect() has succeeded. HealthCheck
	// requires this to be true before it spawns `kopia repository status`
	// — there's no point probing the repo over a never-connected client.
	mu        sync.RWMutex
	connected bool
}

// Options bundles the optional knobs NewClient accepts so the constructor
// surface stays small as we add more (connect timeout, future health-check
// timeout, …). All fields have sane defaults; supply zero values to keep
// them.
type Options struct {
	// ConnectTimeout caps the total time Connect() spends retrying on
	// ErrCredentialsNotReady. Defaults to 60s when zero. After this elapses
	// without seeing ready credentials, Connect returns an error and the
	// caller (controller-runtime) is expected to re-queue. The kubelet
	// backoff on a CrashLoopBackOff would be longer than this; we want to
	// fail fast and let the manager's own restart loop drive recovery.
	ConnectTimeout time.Duration
}

// NewClient creates a new Kopia client. creds may be nil for tests that
// stub the executor and never reach a real connect; production callers
// (cmd/operator/httpserver.go and cmd/pvc-plumber/main.go) MUST supply one.
func NewClient(cfg S3Config, creds CredentialsSource, logger *slog.Logger, opts Options) *Client {
	connectTimeout := opts.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 60 * time.Second
	}
	return &Client{
		cfg:            cfg,
		creds:          creds,
		connectTimeout: connectTimeout,
		logger:         logger,
		executor:       &RealExecutor{},
	}
}

// NewClientWithExecutor creates a new Kopia client with a custom executor
// (for testing). Mirrors NewClient's argument order — same opts shape.
func NewClientWithExecutor(cfg S3Config, creds CredentialsSource, logger *slog.Logger, executor CommandExecutor, opts Options) *Client {
	c := NewClient(cfg, creds, logger, opts)
	c.executor = executor
	return c
}

// Connect connects to the kopia repository over S3. v3.1.0 retries with
// exponential backoff on ErrCredentialsNotReady up to cfg.ConnectTimeout
// (default 60s, configurable via the KOPIA_CONNECT_TIMEOUT env var). If
// the deadline elapses without ready credentials, an error is returned;
// any other error from the CredentialsSource or the kopia subprocess
// propagates immediately — only the "not ready" class is retried.
//
// Mirrors the flag set VolSync's Kopia mover uses (endpoint, bucket,
// access-key, secret-key, password, optional --disable-tls), so the same
// RustFS bucket and creds the mover Jobs see also work here.
func (c *Client) Connect(ctx context.Context) error {
	deadline := time.Now().Add(c.connectTimeout)
	backoff := 250 * time.Millisecond
	const maxBackoff = 5 * time.Second

	attempt := 0
	for {
		attempt++
		creds, err := c.creds.Load()
		if err != nil {
			if !errors.Is(err, ErrCredentialsNotReady) {
				return fmt.Errorf("load kopia credentials: %w", err)
			}
			// Credentials not ready — back off and retry, unless we've
			// exhausted the budget.
			if time.Now().After(deadline) {
				return fmt.Errorf("kopia credentials still not ready after %s: %w", c.connectTimeout, err)
			}
			c.logger.Warn("kopia credentials not ready, retrying",
				"attempt", attempt,
				"backoff", backoff,
				"error", err,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		c.logger.Info("connecting to kopia repository (s3)",
			"endpoint", c.cfg.Endpoint,
			"bucket", c.cfg.Bucket,
			"disable_tls", c.cfg.DisableTLS,
			"attempt", attempt,
		)

		args := connectArgs(c.cfg, creds)
		output, err := c.executor.Run(ctx, "kopia", args...)
		if err != nil {
			c.logger.Error("failed to connect to kopia repository", "error", err, "output", string(output))
			return fmt.Errorf("failed to connect to kopia repository: %w", err)
		}

		c.mu.Lock()
		c.connected = true
		c.mu.Unlock()
		c.logger.Info("connected to kopia repository (s3)")
		return nil
	}
}

// connectArgs assembles the `kopia repository connect s3` argv. Extracted so
// Connect's retry loop and unit tests can both call it without duplicating
// the flag list.
func connectArgs(cfg S3Config, creds Creds) []string {
	args := []string{
		kopiaCmdRepository, kopiaCmdConnect, kopiaCmdS3,
		"--endpoint", cfg.Endpoint,
		"--bucket", cfg.Bucket,
		"--access-key", creds.AccessKey,
		"--secret-access-key", creds.SecretKey,
		"--password", creds.Password,
	}
	if cfg.DisableTLS {
		args = append(args, "--disable-tls")
	}
	return args
}

// CheckBackupExists checks if a backup exists for the given namespace/pvc.
//
// v3.1.0: this no longer requires creds at call time — kopia keeps the
// connection state in its on-disk config (KOPIA_CONFIG_PATH), so a subprocess
// can list snapshots without re-supplying credentials. If the on-disk config
// has aged out (kopia's own session expiry) we surface that as a normal
// command-failed error and return DecisionUnknown — the validating webhook
// then denies as today, ArgoCD retries, and the next reconcile triggers
// Connect() which picks up fresh creds via the lazy-load path.
func (c *Client) CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult {
	// VolSync creates snapshots with source: {pvc}-backup@{namespace}:/data
	source := fmt.Sprintf("%s-backup@%s:/data", pvc, namespace)

	c.logger.Debug("checking kopia snapshot", "source", source)

	output, err := c.executor.Run(ctx, "kopia", "snapshot", "list", source, "--json")
	if err != nil {
		// Check if it's an exit error (command ran but returned non-zero)
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			c.logger.Error("kopia snapshot list failed",
				"source", source,
				"error", err,
				"stderr", string(exitErr.Stderr))
		}
		return backend.CheckResult{
			Exists:        false,
			Decision:      backend.DecisionUnknown,
			Authoritative: false,
			Namespace:     namespace,
			Pvc:           pvc,
			Backend:       backend.TypeKopiaS3,
			Source:        source,
			Error:         fmt.Sprintf("failed to list snapshots: %v", err),
		}
	}

	// Parse JSON output - empty array means no snapshots
	var snapshots []any
	if err := json.Unmarshal(output, &snapshots); err != nil {
		c.logger.Error("failed to parse kopia output", "error", err, "output", string(output))
		return backend.CheckResult{
			Exists:        false,
			Decision:      backend.DecisionUnknown,
			Authoritative: false,
			Namespace:     namespace,
			Pvc:           pvc,
			Backend:       backend.TypeKopiaS3,
			Source:        source,
			Error:         fmt.Sprintf("failed to parse kopia output: %v", err),
		}
	}

	exists := len(snapshots) > 0
	decision := backend.DecisionFresh
	if exists {
		decision = backend.DecisionRestore
	}
	c.logger.Debug("kopia snapshot check complete", "source", source, "exists", exists, "count", len(snapshots))

	return backend.CheckResult{
		Exists:        exists,
		Decision:      decision,
		Authoritative: true,
		Namespace:     namespace,
		Pvc:           pvc,
		Backend:       backend.TypeKopiaS3,
		Source:        source,
	}
}

// snapshotSource represents the source field in kopia snapshot JSON output.
type snapshotSource struct {
	Host     string `json:"host"`
	UserName string `json:"userName"`
	Path     string `json:"path"`
}

type snapshotEntry struct {
	Source snapshotSource `json:"source"`
}

// ListAllSources returns all unique backup sources as namespace/pvc pairs.
// Uses "kopia snapshot list --all --json" — one call to scan the entire repo.
func (c *Client) ListAllSources(ctx context.Context) (map[string]bool, error) {
	c.logger.Info("listing all kopia snapshots for cache pre-warm")

	output, err := c.executor.Run(ctx, "kopia", "snapshot", "list", "--all", "--json")
	if err != nil {
		return nil, fmt.Errorf("failed to list all snapshots: %w", err)
	}

	var entries []snapshotEntry
	if err := json.Unmarshal(output, &entries); err != nil {
		return nil, fmt.Errorf("failed to parse snapshot list: %w", err)
	}

	// Build set of namespace/pvc pairs that have backups
	// Source format: userName={pvc}-backup, host={namespace}, path=/data
	sources := make(map[string]bool)
	for _, e := range entries {
		userName := e.Source.UserName
		namespace := e.Source.Host
		if len(userName) > 7 && userName[len(userName)-7:] == "-backup" {
			pvc := userName[:len(userName)-7]
			key := namespace + "/" + pvc
			sources[key] = true
		}
	}

	c.logger.Info("snapshot scan complete", "unique_sources", len(sources))
	return sources, nil
}

// IsConnected returns whether the client is connected to the repository.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// HealthCheck verifies the Kopia repository client is in a usable state.
// Used by the readiness probe.
//
// v3.1.0 behavior change: HealthCheck now actually invokes
// `kopia repository status` rather than just checking the in-memory
// `connected` flag. This makes the operator's `/readyz` semantically
// "the kopia connection is genuinely usable right now" rather than "the
// process started up successfully at some point in the past". Kubelet won't
// route admission webhook traffic to a not-Ready pod, so this gates the
// failurePolicy=Fail PVC webhook against a pod whose kopia connection has
// silently broken (creds rotated out from under us, on-disk session expired,
// S3 endpoint unreachable, …).
//
// Pre-v3.1.0 this was a cheap "did Connect succeed at startup" check — the
// concern then was that `kopia repository status` could exceed the kubelet
// probe timeout and produce false-negatives on a stuck S3 endpoint. The
// deployment in the consuming GitOps repo now bumps the readiness probe
// timeoutSeconds to give the subprocess room; if the endpoint really is
// stuck, a not-Ready pod is the correct answer (the webhook would deny
// anyway, this just makes it fail fast at the kubelet level).
//
// The status subprocess is bounded by a tight context timeout (5s default,
// inherited from the caller's ctx if shorter) so a wedged endpoint can't
// pin the readiness path past the kubelet probe budget.
func (c *Client) HealthCheck(ctx context.Context) error {
	c.mu.RLock()
	connected := c.connected
	c.mu.RUnlock()
	if !connected {
		return fmt.Errorf("kopia repository not connected")
	}

	// Cap the status call at 5s independent of the caller's ctx so a stuck
	// endpoint doesn't blow the entire probe timeout. If the caller already
	// supplied a tighter deadline we keep theirs.
	const statusTimeout = 5 * time.Second
	probeCtx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()

	if _, err := c.executor.Run(probeCtx, "kopia", kopiaCmdRepository, kopiaCmdStatus); err != nil {
		return fmt.Errorf("kopia repository status: %w", err)
	}
	return nil
}
