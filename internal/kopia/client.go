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

// Client wraps kopia CLI for backup existence checks.
type Client struct {
	repoPath  string
	password  string
	logger    *slog.Logger
	connected bool
	executor  CommandExecutor
}

// NewClient creates a new Kopia client.
func NewClient(repoPath, password string, logger *slog.Logger) *Client {
	return &Client{
		repoPath: repoPath,
		password: password,
		logger:   logger,
		executor: &RealExecutor{},
	}
}

// NewClientWithExecutor creates a new Kopia client with a custom executor (for testing).
func NewClientWithExecutor(repoPath, password string, logger *slog.Logger, executor CommandExecutor) *Client {
	return &Client{
		repoPath: repoPath,
		password: password,
		logger:   logger,
		executor: executor,
	}
}

// Connect connects to the kopia repository.
func (c *Client) Connect(ctx context.Context) error {
	c.logger.Info("connecting to kopia repository", "path", c.repoPath)

	output, err := c.executor.Run(ctx, "kopia", "repository", "connect", "filesystem",
		"--path", c.repoPath,
		"--password", c.password)
	if err != nil {
		c.logger.Error("failed to connect to kopia repository", "error", err, "output", string(output))
		return fmt.Errorf("failed to connect to kopia repository: %w", err)
	}

	c.connected = true
	c.logger.Info("connected to kopia repository")
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
			Exists:    false,
			Namespace: namespace,
			Pvc:       pvc,
			Backend:   "kopia-fs",
			Error:     fmt.Sprintf("failed to list snapshots: %v", err),
		}
	}

	// Parse JSON output - empty array means no snapshots
	var snapshots []any
	if err := json.Unmarshal(output, &snapshots); err != nil {
		c.logger.Error("failed to parse kopia output", "error", err, "output", string(output))
		return backend.CheckResult{
			Exists:    false,
			Namespace: namespace,
			Pvc:       pvc,
			Backend:   "kopia-fs",
			Error:     fmt.Sprintf("failed to parse kopia output: %v", err),
		}
	}

	exists := len(snapshots) > 0
	c.logger.Debug("kopia snapshot check complete", "source", source, "exists", exists, "count", len(snapshots))

	return backend.CheckResult{
		Exists:    exists,
		Namespace: namespace,
		Pvc:       pvc,
		Backend:   "kopia-fs",
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
