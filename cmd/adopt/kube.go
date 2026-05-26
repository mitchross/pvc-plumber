package main

import (
	"fmt"
	"io"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mitchross/pvc-plumber/internal/v4/adopt"
	"github.com/mitchross/pvc-plumber/internal/v4/mode"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
	"github.com/mitchross/pvc-plumber/internal/v4/runtimeconfig"
)

// kubeconfigPath holds an optional --kubeconfig override. nil/empty
// means "use controller-runtime's default discovery." Set by the
// subcommand FlagSet definitions.
//
// Package-level so the flag binding is shared across plan/apply/undo
// without each handler re-defining the same flag.
var kubeconfigPath string

// newRuntime builds the production runtime: kube client from default
// discovery, current wall clock. Tests inject a runtime directly
// rather than calling newRuntime.
func newRuntime(stdout, stderr io.Writer) (*cliRuntime, error) {
	cfg, err := loadKubeconfig()
	if err != nil {
		return nil, &infraError{err: fmt.Errorf("kubeconfig: %w", err)}
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, &infraError{err: fmt.Errorf("build client: %w", err)}
	}
	return &cliRuntime{
		client: c,
		stdout: stdout,
		stderr: stderr,
		now:    time.Now(),
	}, nil
}

// loadKubeconfig respects --kubeconfig, then $KUBECONFIG, then
// ~/.kube/config, then in-cluster service account. Same chain
// controller-runtime's GetConfig uses by default, but with the
// --kubeconfig override threaded in first.
func loadKubeconfig() (*rest.Config, error) {
	if kubeconfigPath != "" {
		// Explicit override — use clientcmd directly.
		return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath},
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
	}
	return ctrl.GetConfig()
}

// loadDefaults projects runtimeconfig.Load output into adopt.Defaults.
// Returns the unprojected runtimeconfig.Config for callers that need
// to surface the raw mode / source for diagnostics.
//
// loadDefaults itself never fails — that's runtimeconfig.Load's
// contract ("always returns a usable Config"). The optional warning
// is returned as-is so callers can render it without coupling to the
// runtimeconfig package directly.
func loadDefaults() (adopt.Defaults, runtimeconfig.Config, error) {
	cfg, warn := runtimeconfig.Load()
	defaults := adopt.Defaults{
		SnapshotClass: cfg.DefaultSnapshotClass,
		CacheCapacity: cfg.DefaultCacheCapacity,
		StorageClass:  cfg.DefaultStorageClass,
		RepoSecret:    naming.DefaultRepoSecretName,
	}
	if cfg.DefaultUID != nil {
		defaults.UID = *cfg.DefaultUID
	}
	if cfg.DefaultGID != nil {
		defaults.GID = *cfg.DefaultGID
	}
	if cfg.DefaultFSGroup != nil {
		defaults.FSGroup = *cfg.DefaultFSGroup
	}
	return defaults, cfg, warn
}

// requireWriteDefaults enforces that all six PVC_PLUMBER_DEFAULT_* env
// vars are set + non-zero, the same contract the operator binary
// applies in permissive mode. The CLI's apply command calls this; plan
// and undo do not (plan is read-only inspection; undo doesn't consult
// defaults).
//
// Returns an *infraError when defaults are missing so exitCodeFor
// maps it to exitInfra.
func requireWriteDefaults(cfg runtimeconfig.Config) error {
	// Temporarily flip the mode to Permissive for the check —
	// RequireV4WriteDefaults only enforces when Mode == Permissive.
	// We do this without persisting the change so audit-mode operators
	// who run the CLI still get the same fail-closed defaults check.
	checkCfg := cfg
	checkCfg.Mode = mode.Permissive
	if err := runtimeconfig.RequireV4WriteDefaults(checkCfg); err != nil {
		return &infraError{err: err}
	}
	return nil
}
