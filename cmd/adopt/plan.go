package main

import (
	"context"
	"errors"
	"flag"
	"fmt"

	"github.com/mitchross/pvc-plumber/internal/v4/adopt"
)

// commonInputFlags holds the flags shared by plan and apply. Both
// subcommands' FlagSets bind the same vars.
type commonInputFlags struct {
	namespace string
	pvc       string
	tier      string

	uid     optionalInt64
	gid     optionalInt64
	fsGroup optionalInt64

	snapshotClass string
	cacheCapacity string
	storageClass  string
	repoSecret    string

	requireFreshBackup      bool
	allowStaleBackup        bool
	allowNoSuccessfulBackup bool

	fieldManager string
	output       string
}

// bindCommonFlags wires the shared flags onto fs. Subcommand-specific
// flags (--dry-run, --confirm, --remove-override-annotations) are
// bound by the caller after this returns.
func (c *commonInputFlags) bindCommonFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.namespace, "namespace", "", "PVC namespace (required)")
	fs.StringVar(&c.namespace, "n", "", "PVC namespace (shorthand)")
	fs.StringVar(&c.pvc, "pvc", "", "PVC name (required)")
	fs.StringVar(&c.tier, "tier", "", "backup tier: hourly|daily|weekly|manual|disabled (required for plan/apply)")
	fs.Var(&c.uid, "uid", "override mover UID")
	fs.Var(&c.gid, "gid", "override mover GID")
	fs.Var(&c.fsGroup, "fs-group", "override mover fsGroup")
	fs.StringVar(&c.snapshotClass, "snapshot-class", "", "override volume snapshot class")
	fs.StringVar(&c.cacheCapacity, "cache-capacity", "", "override kopia cache capacity")
	fs.StringVar(&c.storageClass, "storage-class", "", "override storage class")
	fs.StringVar(&c.repoSecret, "repo-secret", "", "override kopia repository Secret name")
	fs.BoolVar(&c.requireFreshBackup, "require-fresh-backup", true, "require a recent successful backup for the chosen tier")
	fs.BoolVar(&c.allowStaleBackup, "allow-stale-backup", false, "allow a backup older than the tier window")
	fs.BoolVar(&c.allowNoSuccessfulBackup, "allow-no-successful-backup", false, "allow a PVC with no successful backup recorded")
	fs.StringVar(&c.fieldManager, "field-manager", adopt.DefaultFieldManager, "SSA field manager (default pvc-plumber-adopt)")
	fs.StringVar(&c.output, "output", "table", "output format: table|json")
	fs.StringVar(&kubeconfigPath, "kubeconfig", "", "path to kubeconfig (default: $KUBECONFIG, ~/.kube/config, in-cluster)")
}

// validateRequired checks namespace, pvc, tier, and output. Returns
// a *usageError for the caller to bubble up.
func (c *commonInputFlags) validateRequired(needTier bool) error {
	if c.namespace == "" {
		return &usageError{msg: "missing required flag --namespace"}
	}
	if c.pvc == "" {
		return &usageError{msg: "missing required flag --pvc"}
	}
	if needTier && c.tier == "" {
		return &usageError{msg: "missing required flag --tier"}
	}
	switch c.output {
	case "", outTable, outJSON:
	default:
		return &usageError{msg: fmt.Sprintf("invalid --output %q (want table|json)", c.output)}
	}
	if c.output == "" {
		c.output = outTable
	}
	return nil
}

// toInputs projects the parsed flags into adopt.Inputs. Defaults
// must be supplied by the caller (loaded via loadDefaults).
func (c *commonInputFlags) toInputs(defaults adopt.Defaults) adopt.Inputs {
	in := adopt.Inputs{
		Namespace:               c.namespace,
		PVCName:                 c.pvc,
		Tier:                    c.tier,
		UID:                     c.uid.ptr(),
		GID:                     c.gid.ptr(),
		FSGroup:                 c.fsGroup.ptr(),
		SnapshotClass:           c.snapshotClass,
		CacheCapacity:           c.cacheCapacity,
		StorageClass:            c.storageClass,
		RepoSecret:              c.repoSecret,
		Defaults:                defaults,
		RequireFreshBackup:      c.requireFreshBackup,
		AllowStaleBackup:        c.allowStaleBackup,
		AllowNoSuccessfulBackup: c.allowNoSuccessfulBackup,
	}
	return in
}

// runPlan handles `pvc-plumber-adopt plan ...`.
//
// Read-only: calls adopt.PlanFor only. Does not write, does not call
// adopt.Apply, does not modify any cluster resource. Exit code maps:
// 0 safe, 1 usage, 2 blocked, 4 infrastructure.
func runPlan(rt *cliRuntime, args []string) int {
	fs := flag.NewFlagSet("plan", flag.ContinueOnError)
	fs.SetOutput(rt.stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprintln(rt.stderr, "Usage: pvc-plumber-adopt plan --namespace <ns> --pvc <name> --tier <t> [flags]")
		fs.PrintDefaults()
	}

	var c commonInputFlags
	c.bindCommonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if err := c.validateRequired(true); err != nil {
		_, _ = fmt.Fprintln(rt.stderr, err)
		return exitCodeFor(err)
	}

	defaults, _, warn := loadDefaults()
	if warn != nil {
		_, _ = fmt.Fprintf(rt.stderr, "warning: %v\n", warn)
	}

	plan, err := adopt.PlanFor(context.Background(), rt.client, c.toInputs(defaults))
	if err != nil {
		// Infrastructure-level error (PVC not found may surface as a
		// blocker inside the plan, but raw transport errors come back
		// here).
		wrapped := &infraError{err: err}
		_, _ = fmt.Fprintln(rt.stderr, wrapped)
		return exitCodeFor(wrapped)
	}

	if err := renderPlan(rt.stdout, plan, c.output); err != nil {
		_, _ = fmt.Fprintln(rt.stderr, err)
		return exitInfra
	}

	switch plan.Verdict {
	case adopt.VerdictBlocked:
		return exitRefused
	default:
		return exitSuccess
	}
}

// compile-time guard: errors package is used (via errors.As in
// exitCodeFor); ensures import stays.
var _ = errors.As
