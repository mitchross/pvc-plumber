// Command adopt is the user-facing CLI for the pvc-plumber v4 adoption
// flow. It exposes three subcommands — plan, apply, undo — that wrap
// the internal/v4/adopt package's PlanFor / Apply / Undo entrypoints.
//
// The binary is built to the file name pvc-plumber-adopt via the
// Makefile's build-adopt target (go build -o pvc-plumber-adopt
// ./cmd/adopt). Operators invoke it as:
//
//	pvc-plumber-adopt plan  --namespace nginx-example --pvc storage --tier daily
//	pvc-plumber-adopt apply --namespace nginx-example --pvc storage --tier daily [--dry-run] [--confirm]
//	pvc-plumber-adopt undo  --namespace nginx-example --pvc storage [--remove-override-annotations]
//
// Hard boundaries (enforced structurally by the imported package):
//   - reads/writes PVC metadata only;
//   - never touches ReplicationSource, ReplicationDestination, Secret,
//     ExternalSecret, or Argo Application resources;
//   - never makes Git or argocd API calls;
//   - is read-only when subcommand is plan (client.Reader signature).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mitchross/pvc-plumber/internal/v4/adopt"
)

// Exit codes. Stable; consumer scripts depend on them.
const (
	exitSuccess  = 0 // safe / success / no-op
	exitUsage    = 1 // unknown subcommand, missing required flag, parse error
	exitRefused  = 2 // VerdictBlocked / VerdictAlreadyAdopted / nothing-to-undo refusal
	exitConflict = 3 // field-manager ownership conflict, needs --confirm
	exitInfra    = 4 // kubeconfig missing, RBAC denied, PVC not found, network, missing required defaults
)

// Subcommand and output-format string constants. Stable across the
// codebase so handler dispatch and test assertions reference the same
// literals.
const (
	cmdPlan  = "plan"
	cmdApply = "apply"
	cmdUndo  = "undo"
	cmdHelp  = "help"
	outTable = "table"
	outJSON  = "json"
)

// runtime carries the per-invocation dependencies each subcommand
// handler needs. Tests inject a fake client + buffered writers; main()
// builds the production runtime via newRuntime.
type cliRuntime struct {
	client client.Client
	stdout io.Writer
	stderr io.Writer
	now    time.Time
}

// usageError is returned by handlers for flag-parsing / required-flag /
// validation failures. exitCodeFor maps it to exitUsage.
type usageError struct{ msg string }

func (e *usageError) Error() string { return e.msg }

// infraError wraps a low-level error so exitCodeFor maps it to
// exitInfra. Used for kubeconfig / RBAC / network failures and for
// missing required defaults.
type infraError struct{ err error }

func (e *infraError) Error() string { return e.err.Error() }
func (e *infraError) Unwrap() error { return e.err }

// exitCodeFor maps an error to the documented exit code. The mapping
// is the public contract for scripts; tests assert on this function
// directly.
func exitCodeFor(err error) int {
	if err == nil {
		return exitSuccess
	}
	var conflict *adopt.ConflictError
	if errors.As(err, &conflict) {
		return exitConflict
	}
	var refused *adopt.RefusedError
	if errors.As(err, &refused) {
		return exitRefused
	}
	var usage *usageError
	if errors.As(err, &usage) {
		return exitUsage
	}
	var infra *infraError
	if errors.As(err, &infra) {
		return exitInfra
	}
	return exitInfra
}

// scheme is the runtime.Scheme the CLI registers. Only corev1 is
// needed — PVCs and Namespaces. VolSync RS/RD reads inside the adopt
// package go through unstructured.Unstructured, so we don't need to
// register the VolSync API group here.
var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is the testable entrypoint. Returns the exit code rather than
// calling os.Exit so tests can assert on it without subprocessing.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return exitUsage
	}
	sub := args[0]
	rest := args[1:]

	switch sub {
	case cmdPlan:
		rt, err := newRuntime(stdout, stderr)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, err)
			return exitCodeFor(err)
		}
		return runPlan(rt, rest)
	case cmdApply:
		rt, err := newRuntime(stdout, stderr)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, err)
			return exitCodeFor(err)
		}
		return runApply(rt, rest)
	case cmdUndo:
		rt, err := newRuntime(stdout, stderr)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, err)
			return exitCodeFor(err)
		}
		return runUndo(rt, rest)
	case "-h", "--help", cmdHelp:
		printUsage(stdout)
		return exitSuccess
	default:
		_, _ = fmt.Fprintf(stderr, "pvc-plumber-adopt: unknown subcommand %q\n\n", sub)
		printUsage(stderr)
		return exitUsage
	}
}

// printUsage prints the top-level help. Each subcommand's `-h` form
// uses its own FlagSet usage, defined in plan.go / apply.go / undo.go.
func printUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, `pvc-plumber-adopt — read-only planner and metadata-only writer for v4 PVC adoption

Usage:
  pvc-plumber-adopt <command> [flags]

Commands:
  plan    Validate adoption readiness for a PVC. Read-only.
  apply   Write v4 gate labels (+ override annotations) via server-side apply.
  undo    Release adopt-owned labels via server-side apply with an empty payload.

Run "pvc-plumber-adopt <command> -h" for command-specific flags.

First live smoke command (intended; not executed by this binary):
  pvc-plumber-adopt plan --namespace nginx-example --pvc storage --tier daily --output table

Exit codes:
  0  safe / success / no-op
  1  usage error (bad subcommand, missing required flag)
  2  plan blocked, apply refused, AlreadyAdopted plan
  3  ownership conflict — re-run with --confirm
  4  infrastructure error (no kubeconfig, RBAC, PVC not found, missing defaults)

This binary never writes ReplicationSource/ReplicationDestination,
never touches Secrets/ExternalSecrets, never reaches argocd, and never
shells out to Git. PVC spec is structurally untouchable.
`)
}

// loadDefaults reads runtimeconfig.Load and projects the v4 default
// env vars into adopt.Defaults. Subcommands decide what to do on
// missing-required defaults; the loader itself never fails — that's
// runtimeconfig.Load's contract.
//
// Defined in kube.go to keep main.go focused on dispatch.

// Compile-time guard: the imported package surface used here.
var _ = context.Background
var _ corev1.PersistentVolumeClaim
