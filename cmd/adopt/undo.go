package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/mitchross/pvc-plumber/internal/v4/adopt"
)

// runUndo handles `pvc-plumber-adopt undo ...`.
//
// Calls adopt.Undo only. Does not run PlanFor — undo operates on live
// labels directly. Exit codes: 0 success (including nothing-to-undo),
// 1 usage, 4 infrastructure.
func runUndo(rt *cliRuntime, args []string) int {
	fs := flag.NewFlagSet("undo", flag.ContinueOnError)
	fs.SetOutput(rt.stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprintln(rt.stderr, "Usage: pvc-plumber-adopt undo --namespace <ns> --pvc <name> [--dry-run] [--remove-override-annotations] [flags]")
		fs.PrintDefaults()
	}

	var namespace, pvcName, fieldManager, output string
	var dryRun, confirm, removeOverrides bool
	fs.StringVar(&namespace, "namespace", "", "PVC namespace (required)")
	fs.StringVar(&namespace, "n", "", "PVC namespace (shorthand)")
	fs.StringVar(&pvcName, "pvc", "", "PVC name (required)")
	fs.StringVar(&fieldManager, "field-manager", adopt.DefaultFieldManager, "SSA field manager (default pvc-plumber-adopt)")
	fs.StringVar(&output, "output", "table", "output format: table|json")
	fs.BoolVar(&dryRun, "dry-run", false, "server-side dry-run; do not persist")
	fs.BoolVar(&confirm, "confirm", false, "force ownership transfer on field-manager conflicts")
	fs.BoolVar(&removeOverrides, "remove-override-annotations", false, "also release adopt's override annotation keys")
	fs.StringVar(&kubeconfigPath, "kubeconfig", "", "path to kubeconfig")

	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if namespace == "" {
		err := &usageError{msg: "missing required flag --namespace"}
		_, _ = fmt.Fprintln(rt.stderr, err)
		return exitCodeFor(err)
	}
	if pvcName == "" {
		err := &usageError{msg: "missing required flag --pvc"}
		_, _ = fmt.Fprintln(rt.stderr, err)
		return exitCodeFor(err)
	}
	switch output {
	case "", outTable, outJSON:
	default:
		err := &usageError{msg: fmt.Sprintf("invalid --output %q (want table|json)", output)}
		_, _ = fmt.Fprintln(rt.stderr, err)
		return exitCodeFor(err)
	}
	if output == "" {
		output = outTable
	}

	result, err := adopt.Undo(context.Background(), rt.client, namespace, pvcName, adopt.UndoOptions{
		FieldManager:              fieldManager,
		DryRun:                    dryRun,
		Confirm:                   confirm,
		RemoveOverrideAnnotations: removeOverrides,
	})
	if renderErr := renderUndo(rt.stdout, result, err, output, dryRun); renderErr != nil {
		_, _ = fmt.Fprintln(rt.stderr, renderErr)
		return exitInfra
	}
	if err != nil {
		return exitCodeFor(err)
	}
	return exitSuccess
}
