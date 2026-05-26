package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/mitchross/pvc-plumber/internal/v4/adopt"
)

// runApply handles `pvc-plumber-adopt apply ...`.
//
// Calls PlanFor first; if the plan's verdict is BlockedOrAlreadyAdopted,
// refuses without calling Apply. Otherwise calls adopt.Apply with the
// options from --dry-run and --confirm. Exit codes: 0 success, 1 usage,
// 2 refused, 3 conflict, 4 infrastructure.
func runApply(rt *cliRuntime, args []string) int {
	fs := flag.NewFlagSet("apply", flag.ContinueOnError)
	fs.SetOutput(rt.stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprintln(rt.stderr, "Usage: pvc-plumber-adopt apply --namespace <ns> --pvc <name> --tier <t> [--dry-run] [--confirm] [flags]")
		fs.PrintDefaults()
	}

	var c commonInputFlags
	c.bindCommonFlags(fs)
	var dryRun, confirm bool
	fs.BoolVar(&dryRun, "dry-run", false, "server-side dry-run; do not persist")
	fs.BoolVar(&confirm, "confirm", false, "force ownership transfer on field-manager conflicts")

	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if err := c.validateRequired(true); err != nil {
		_, _ = fmt.Fprintln(rt.stderr, err)
		return exitCodeFor(err)
	}

	defaults, runtimeCfg, warn := loadDefaults()
	if warn != nil {
		_, _ = fmt.Fprintf(rt.stderr, "warning: %v\n", warn)
	}
	// Apply is a write commitment — enforce the same permissive-mode
	// defaults contract the operator binary applies on startup.
	if err := requireWriteDefaults(runtimeCfg); err != nil {
		_, _ = fmt.Fprintln(rt.stderr, err)
		return exitCodeFor(err)
	}

	ctx := context.Background()
	plan, err := adopt.PlanFor(ctx, rt.client, c.toInputs(defaults))
	if err != nil {
		wrapped := &infraError{err: err}
		_, _ = fmt.Fprintln(rt.stderr, wrapped)
		return exitCodeFor(wrapped)
	}

	// Render the plan summary first so operators see what was decided
	// before the apply result.
	if err := renderPlan(rt.stdout, plan, c.output); err != nil {
		_, _ = fmt.Fprintln(rt.stderr, err)
		return exitInfra
	}

	switch plan.Verdict {
	case adopt.VerdictBlocked, adopt.VerdictAlreadyAdopted:
		// PlanFor already returned a non-actionable verdict. Apply
		// would refuse — short-circuit and let the operator see the
		// reason via the rendered plan.
		_, _ = fmt.Fprintf(rt.stderr, "Apply refused: verdict=%s\n", plan.Verdict)
		return exitRefused
	}

	result, applyErr := adopt.Apply(ctx, rt.client, plan, adopt.ApplyOptions{
		FieldManager: c.fieldManager,
		DryRun:       dryRun,
		Confirm:      confirm,
	})
	if err := renderApply(rt.stdout, result, applyErr, c.output, dryRun); err != nil {
		_, _ = fmt.Fprintln(rt.stderr, err)
		return exitInfra
	}
	if applyErr != nil {
		return exitCodeFor(applyErr)
	}
	return exitSuccess
}
