package kopia

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"

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

// S3Config bundles the inputs `kopia repository connect s3` needs. Carried
// as a struct (rather than seven positional arguments) because v3.0.0
// switched the operator's own kopia repository from a filesystem mount to a
// shared S3 bucket and the constructor surface would otherwise be unwieldy.
type S3Config struct {
	Endpoint   string
	Bucket     string
	AccessKey  string
	SecretKey  string
	Password   string
	DisableTLS bool
}

// Client wraps the kopia CLI for backup-existence checks against an S3-backed
// Kopia repository. v3.0.0 removed the legacy filesystem-mount path
// (`KOPIA_REPOSITORY_PATH`) — both the operator pod and VolSync mover Jobs
// now use the same S3 endpoint, eliminating the shared-volume admission
// dance that the deleted JobMutator was previously responsible for.
type Client struct {
	cfg       S3Config
	logger    *slog.Logger
	connected bool
	executor  CommandExecutor
}

// NewClient creates a new Kopia client.
func NewClient(cfg S3Config, logger *slog.Logger) *Client {
	return &Client{
		cfg:      cfg,
		logger:   logger,
		executor: &RealExecutor{},
	}
}

// NewClientWithExecutor creates a new Kopia client with a custom executor (for testing).
func NewClientWithExecutor(cfg S3Config, logger *slog.Logger, executor CommandExecutor) *Client {
	return &Client{
		cfg:      cfg,
		logger:   logger,
		executor: executor,
	}
}

// Connect connects to the kopia repository over S3. Mirrors the flag set
// VolSync's Kopia mover uses (endpoint, bucket, access-key, secret-key,
// password, optional --disable-tls), so the same RustFS bucket and creds
// the mover Jobs see also work here without a separate "operator-only"
// credential set.
func (c *Client) Connect(ctx context.Context) error {
	c.logger.Info("connecting to kopia repository (s3)",
		"endpoint", c.cfg.Endpoint,
		"bucket", c.cfg.Bucket,
		"disable_tls", c.cfg.DisableTLS,
	)

	args := []string{
		"repository", "connect", "s3",
		"--endpoint", c.cfg.Endpoint,
		"--bucket", c.cfg.Bucket,
		"--access-key", c.cfg.AccessKey,
		"--secret-access-key", c.cfg.SecretKey,
		"--password", c.cfg.Password,
	}
	if c.cfg.DisableTLS {
		args = append(args, "--disable-tls")
	}

	output, err := c.executor.Run(ctx, "kopia", args...)
	if err != nil {
		c.logger.Error("failed to connect to kopia repository", "error", err, "output", string(output))
		return fmt.Errorf("failed to connect to kopia repository: %w", err)
	}

	c.connected = true
	c.logger.Info("connected to kopia repository (s3)")
	return nil
}

// CheckBackupExists checks if a backup exists for the given namespace/pvc.
func (c *Client) CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult {
	// VolSync creates snapshots with source: {pvc}-backup@{namespace}:/data
	source := fmt.Sprintf("%s-backup@%s:/data", pvc, namespace)

	c.logger.Debug("checking kopia snapshot", "source", source)

	output, err := c.executor.Run(ctx, "kopia", "snapshot", "list", source, "--json")
	if err != nil {
		// Check if it's an exit error (command ran but returned non-zero)
		if exitErr, ok := err.(*exec.ExitError); ok {
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
	return c.connected
}

// HealthCheck verifies the Kopia repository client is in a usable state.
// Used by the readiness probe.
//
// Intentionally cheap: just confirms the startup Connect() succeeded. We
// deliberately do NOT spawn a fresh `kopia repository status` subprocess on
// every probe — over a stuck S3 endpoint that command can take >1s and the
// kubelet probe timeout SIGKILLs it mid-query, producing false-negative
// "signal: killed" readiness failures that block the fail-closed PVC gate.
//
// Pre-v3.0.0 this also stat()'d KOPIA_REPOSITORY_PATH; that check is gone
// because the repo is no longer a local mount. Deep validation (creds,
// reachability, corruption) surfaces on the next CheckBackupExists call,
// which returns an error to the validating webhook; the fail-closed validate
// rule then denies PVC creation — same safety guarantee, without hammering
// the S3 endpoint every 10 seconds.
func (c *Client) HealthCheck(_ context.Context) error {
	if !c.connected {
		return fmt.Errorf("kopia repository not connected")
	}
	return nil
}
