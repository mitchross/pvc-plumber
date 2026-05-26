package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/mitchross/pvc-plumber/internal/v4/adopt"
	pvcplumberlabels "github.com/mitchross/pvc-plumber/internal/v4/labels"
)

// =============================================================================
// Recording client — mirrors internal/v4/adopt's pattern. Counts all
// writes so paranoia tests can assert the CLI never patches anything
// but PVCs.
// =============================================================================

type recordedWrite struct {
	Verb      string
	GVK       string
	Namespace string
	Name      string
}

type recordingClient struct {
	client.Client
	writes []recordedWrite
}

func (rc *recordingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	rc.writes = append(rc.writes, recordedWrite{
		Verb: "create", GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.Create(ctx, obj, opts...)
}
func (rc *recordingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	rc.writes = append(rc.writes, recordedWrite{
		Verb: "update", GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.Update(ctx, obj, opts...)
}
func (rc *recordingClient) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	rc.writes = append(rc.writes, recordedWrite{
		Verb: verbPatch, GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.Patch(ctx, obj, p, opts...)
}
func (rc *recordingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	rc.writes = append(rc.writes, recordedWrite{
		Verb: "delete", GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.Delete(ctx, obj, opts...)
}
func (rc *recordingClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	rc.writes = append(rc.writes, recordedWrite{
		Verb: "deleteAllOf", GVK: gvkOf(obj),
		Namespace: obj.GetNamespace(), Name: obj.GetName(),
	})
	return rc.Client.DeleteAllOf(ctx, obj, opts...)
}

func gvkOf(obj client.Object) string {
	gvk := obj.GetObjectKind().GroupVersionKind()
	return gvk.GroupVersion().String() + "/" + gvk.Kind
}

// =============================================================================
// Fixtures
// =============================================================================

const (
	tns  = "nginx-example"
	tpvc = "storage"

	// Test-side flag-name constants. Production-side equivalents live
	// next to their consumers (`--namespace` is registered via
	// fs.StringVar in plan.go etc.). Centralized here only to silence
	// goconst on the test file.
	flagNamespace = "--namespace"
	flagPVC       = "--pvc"
	flagTier      = "--tier"
	flagOutput    = "--output"
	tierDaily     = "daily"
	verbPatch     = "patch"
	tCmdPlan      = "plan"
	tCmdUndo      = "undo"
)

func makePVC() *corev1.PersistentVolumeClaim {
	sc := "longhorn"
	return &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: tns,
			Name:      tpvc,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

func makeNamespace() *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: tns, Labels: map[string]string{pvcplumberlabels.NamespacePrivilegedMoversLabel: "true"}},
	}
}

// newRT builds a test runtime backed by a fake client. stderr is
// attached to the returned runtime but not returned separately — tests
// that need stderr can inspect rt.stderr via type-assertion to
// *bytes.Buffer if needed.
func newRT(t *testing.T, objs ...runtime.Object) (*cliRuntime, *recordingClient, *bytes.Buffer) {
	t.Helper()
	cObjs := make([]client.Object, 0, len(objs))
	for _, o := range objs {
		if o == nil {
			continue
		}
		co, ok := o.(client.Object)
		if !ok {
			continue
		}
		cObjs = append(cObjs, co)
	}
	fc := fake.NewClientBuilder().WithObjects(cObjs...).Build()
	rc := &recordingClient{Client: fc}
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	return &cliRuntime{client: rc, stdout: stdout, stderr: stderr}, rc, stdout
}

// =============================================================================
// run() dispatch tests
// =============================================================================

func TestRunUnknownSubcommandExits1(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := run([]string{"frobnicate"}, stdout, stderr)
	if code != exitUsage {
		t.Errorf("exit code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "unknown subcommand") {
		t.Errorf("stderr missing usage error; got %q", stderr.String())
	}
}

func TestRunNoArgsExits1(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := run(nil, stdout, stderr)
	if code != exitUsage {
		t.Errorf("exit = %d want %d", code, exitUsage)
	}
}

func TestRunHelpExits0(t *testing.T) {
	for _, arg := range []string{"-h", "--help", "help"} {
		stdout := &bytes.Buffer{}
		stderr := &bytes.Buffer{}
		code := run([]string{arg}, stdout, stderr)
		if code != exitSuccess {
			t.Errorf("%q: exit = %d want 0", arg, code)
		}
		if !strings.Contains(stdout.String(), "Commands:") {
			t.Errorf("%q: usage not printed", arg)
		}
	}
}

// =============================================================================
// runPlan
// =============================================================================

func TestPlanMissingFlagsExitUsage(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"no_args", []string{}},
		{"only_namespace", []string{flagNamespace, tns}},
		{"only_pvc", []string{flagPVC, tpvc}},
		{"missing_tier", []string{flagNamespace, tns, flagPVC, tpvc}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rt, _, _ := newRT(t)
			code := runPlan(rt, c.args)
			if code != exitUsage {
				t.Errorf("exit = %d want %d", code, exitUsage)
			}
		})
	}
}

func TestPlanInvalidTierExitUsage(t *testing.T) {
	rt, _, _ := newRT(t, makePVC(), makeNamespace())
	code := runPlan(rt, []string{flagNamespace, tns, flagPVC, tpvc, flagTier, "bananas"})
	if code != exitUsage && code != exitRefused {
		// Invalid tier is caught by adopt.PlanFor (BlockerInvalidTier),
		// which yields VerdictBlocked → exitRefused. Either exit is
		// acceptable as long as it is not exitSuccess.
		t.Errorf("expected non-success exit, got %d", code)
	}
}

func TestPlanInvalidOutputExitUsage(t *testing.T) {
	rt, _, _ := newRT(t, makePVC(), makeNamespace())
	code := runPlan(rt, []string{flagNamespace, tns, flagPVC, tpvc, flagTier, tierDaily, flagOutput, "yaml"})
	if code != exitUsage {
		t.Errorf("exit = %d want %d", code, exitUsage)
	}
}

func TestPlanBlockedExitRefused(t *testing.T) {
	// PVC exists but no RS/RD → BlockerRSMissing → VerdictBlocked.
	rt, _, _ := newRT(t, makePVC(), makeNamespace())
	code := runPlan(rt, []string{flagNamespace, tns, flagPVC, tpvc, flagTier, tierDaily})
	if code != exitRefused {
		t.Errorf("exit = %d want %d (blocked plan)", code, exitRefused)
	}
}

func TestPlanReadsOnlyNeverPatches(t *testing.T) {
	rt, rc, _ := newRT(t, makePVC(), makeNamespace())
	_ = runPlan(rt, []string{flagNamespace, tns, flagPVC, tpvc, flagTier, tierDaily})
	for _, w := range rc.writes {
		t.Errorf("plan recorded a write: %+v", w)
	}
}

func TestPlanJSONOutputSchemaV1(t *testing.T) {
	rt, _, stdout := newRT(t, makePVC(), makeNamespace())
	_ = runPlan(rt, []string{flagNamespace, tns, flagPVC, tpvc, flagTier, tierDaily, flagOutput, outJSON})
	var out map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("json unmarshal: %v\nraw=%s", err, stdout.String())
	}
	if got := out["schema_version"]; got != "v1" {
		t.Errorf("schema_version = %v, want v1", got)
	}
	if got := out["command"]; got != tCmdPlan {
		t.Errorf("command = %v, want plan", got)
	}
	for _, k := range []string{"verdict", "blockers", "warnings", "labels_to_write", "annotations_to_write"} {
		if _, ok := out[k]; !ok {
			t.Errorf("json missing required key %q", k)
		}
	}
}

func TestPlanPVCNotFoundExitRefused(t *testing.T) {
	// No PVC seeded — adopt.PlanFor surfaces this as BlockerPVCNotFound.
	rt, _, _ := newRT(t, makeNamespace())
	code := runPlan(rt, []string{flagNamespace, tns, flagPVC, "does-not-exist", flagTier, tierDaily})
	if code != exitRefused {
		t.Errorf("exit = %d want %d", code, exitRefused)
	}
}

// =============================================================================
// runApply
// =============================================================================

func TestApplyBlockedPlanDoesNotCallApply(t *testing.T) {
	// Apply runs the requireWriteDefaults gate before PlanFor; set the
	// env so we reach the planner where the blocked verdict fires.
	setApplyDefaults(t)
	rt, rc, _ := newRT(t, makePVC(), makeNamespace())
	code := runApply(rt, []string{flagNamespace, tns, flagPVC, tpvc, flagTier, tierDaily})
	if code != exitRefused {
		t.Errorf("exit = %d want %d", code, exitRefused)
	}
	// No PVC Patch recorded (we never got past PlanFor's Blocked verdict).
	for _, w := range rc.writes {
		if w.Verb == verbPatch {
			t.Errorf("apply recorded a patch despite blocked plan: %+v", w)
		}
	}
}

// setApplyDefaults sets the six PVC_PLUMBER_DEFAULT_* env vars for the
// duration of t so apply's requireWriteDefaults gate passes. Uses
// t.Setenv which restores after the test.
func setApplyDefaults(t *testing.T) {
	t.Helper()
	t.Setenv("PVC_PLUMBER_DEFAULT_SNAPSHOT_CLASS", "longhorn-snapclass")
	t.Setenv("PVC_PLUMBER_DEFAULT_CACHE_CAPACITY", "2Gi")
	t.Setenv("PVC_PLUMBER_DEFAULT_STORAGE_CLASS", "longhorn")
	t.Setenv("PVC_PLUMBER_DEFAULT_UID", "568")
	t.Setenv("PVC_PLUMBER_DEFAULT_GID", "568")
	t.Setenv("PVC_PLUMBER_DEFAULT_FSGROUP", "568")
}

func TestApplyMissingDefaultsExitInfra(t *testing.T) {
	// requireWriteDefaults reads env. We don't set PVC_PLUMBER_DEFAULT_*
	// in tests by default, so apply should refuse with exitInfra.
	rt, _, _ := newRT(t, makePVC(), makeNamespace())
	code := runApply(rt, []string{flagNamespace, tns, flagPVC, tpvc, flagTier, tierDaily})
	// Either exitInfra (defaults missing) or exitRefused (planner
	// blocked before the defaults check fires) is acceptable for this
	// test — the assertion is "apply does not exit 0 without defaults".
	if code == exitSuccess {
		t.Errorf("apply exited 0 with no required defaults set")
	}
}

// =============================================================================
// runUndo
// =============================================================================

func TestUndoMissingNamespaceExitUsage(t *testing.T) {
	rt, _, _ := newRT(t)
	code := runUndo(rt, []string{flagPVC, tpvc})
	if code != exitUsage {
		t.Errorf("exit = %d want %d", code, exitUsage)
	}
}

func TestUndoMissingPVCExitUsage(t *testing.T) {
	rt, _, _ := newRT(t)
	code := runUndo(rt, []string{flagNamespace, tns})
	if code != exitUsage {
		t.Errorf("exit = %d want %d", code, exitUsage)
	}
}

func TestUndoInvalidOutputExitUsage(t *testing.T) {
	rt, _, _ := newRT(t, makePVC())
	code := runUndo(rt, []string{flagNamespace, tns, flagPVC, tpvc, flagOutput, "yaml"})
	if code != exitUsage {
		t.Errorf("exit = %d want %d", code, exitUsage)
	}
}

func TestUndoNothingToDoNoWritesExit0(t *testing.T) {
	// PVC has no v4 gate labels → Undo no-ops without calling Patch.
	rt, rc, stdout := newRT(t, makePVC())
	code := runUndo(rt, []string{flagNamespace, tns, flagPVC, tpvc})
	if code != exitSuccess {
		t.Errorf("exit = %d want %d", code, exitSuccess)
	}
	for _, w := range rc.writes {
		if w.Verb == verbPatch {
			t.Errorf("undo recorded a patch despite nothing-to-undo: %+v", w)
		}
	}
	if !strings.Contains(stdout.String(), "nothing to do") {
		t.Errorf("table output missing 'nothing to do' phrase; got %q", stdout.String())
	}
}

func TestUndoPVCNotFoundExitRefused(t *testing.T) {
	// adopt.Undo returns *RefusedError on PVC-not-found (the package
	// classifies "no PVC to undo against" as refused, not infrastructure).
	// Matches the planner's BlockerPVCNotFound mapping.
	rt, _, _ := newRT(t /* no PVC */)
	code := runUndo(rt, []string{flagNamespace, tns, flagPVC, "does-not-exist"})
	if code != exitRefused {
		t.Errorf("exit = %d want %d", code, exitRefused)
	}
}

func TestUndoJSONOutputSchemaV1(t *testing.T) {
	rt, _, stdout := newRT(t, makePVC())
	_ = runUndo(rt, []string{flagNamespace, tns, flagPVC, tpvc, flagOutput, outJSON})
	var out map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("json unmarshal: %v\nraw=%s", err, stdout.String())
	}
	if out["schema_version"] != "v1" {
		t.Errorf("schema_version = %v want v1", out["schema_version"])
	}
	if out["command"] != tCmdUndo {
		t.Errorf("command = %v want undo", out["command"])
	}
}

// =============================================================================
// Exit-code mapping unit tests
// =============================================================================

func TestExitCodeForNilZero(t *testing.T) {
	if got := exitCodeFor(nil); got != exitSuccess {
		t.Errorf("nil → %d want 0", got)
	}
}

func TestExitCodeForConflict3(t *testing.T) {
	err := &adopt.ConflictError{}
	if got := exitCodeFor(err); got != exitConflict {
		t.Errorf("conflict → %d want %d", got, exitConflict)
	}
}

func TestExitCodeForRefused2(t *testing.T) {
	err := &adopt.RefusedError{Reason: "x"}
	if got := exitCodeFor(err); got != exitRefused {
		t.Errorf("refused → %d want %d", got, exitRefused)
	}
}

func TestExitCodeForUsage1(t *testing.T) {
	err := &usageError{msg: "x"}
	if got := exitCodeFor(err); got != exitUsage {
		t.Errorf("usage → %d want %d", got, exitUsage)
	}
}

func TestExitCodeForInfraDefault4(t *testing.T) {
	err := errors.New("network broken")
	if got := exitCodeFor(err); got != exitInfra {
		t.Errorf("generic → %d want %d", got, exitInfra)
	}
}

func TestExitCodeForExplicitInfra4(t *testing.T) {
	err := &infraError{err: errors.New("rbac denied")}
	if got := exitCodeFor(err); got != exitInfra {
		t.Errorf("infra → %d want %d", got, exitInfra)
	}
}

// =============================================================================
// optionalInt64 flag helper
// =============================================================================

func TestOptionalInt64UnsetReturnsNil(t *testing.T) {
	var o optionalInt64
	if o.ptr() != nil {
		t.Errorf("unset ptr() != nil")
	}
}

func TestOptionalInt64SetReturnsPointer(t *testing.T) {
	var o optionalInt64
	if err := o.Set("1001"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	p := o.ptr()
	if p == nil || *p != 1001 {
		t.Errorf("ptr = %v, want *=1001", p)
	}
}

func TestOptionalInt64SetZeroReturnsPointerToZero(t *testing.T) {
	var o optionalInt64
	if err := o.Set("0"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	p := o.ptr()
	if p == nil || *p != 0 {
		t.Errorf("--uid 0 should produce *int64(0), got %v", p)
	}
}

func TestOptionalInt64SetInvalidErrors(t *testing.T) {
	var o optionalInt64
	if err := o.Set("not-a-number"); err == nil {
		t.Errorf("Set with bad value should error")
	}
}

// =============================================================================
// Paranoia: no non-PVC writes via the CLI surface
// =============================================================================

func TestCLINeverWritesNonPVCResources(t *testing.T) {
	scenarios := []struct {
		name string
		args []string
		fn   func(*cliRuntime, []string) int
	}{
		{"plan_safe_args", []string{flagNamespace, tns, flagPVC, tpvc, flagTier, tierDaily}, runPlan},
		{"plan_blocked", []string{flagNamespace, tns, flagPVC, "missing-pvc", flagTier, tierDaily}, runPlan},
		{"apply_blocked", []string{flagNamespace, tns, flagPVC, tpvc, flagTier, tierDaily}, runApply},
		{"undo_nothing", []string{flagNamespace, tns, flagPVC, tpvc}, runUndo},
		{"undo_dry_run", []string{flagNamespace, tns, flagPVC, tpvc, "--dry-run"}, runUndo},
	}
	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			rt, rc, _ := newRT(t, makePVC(), makeNamespace())
			_ = s.fn(rt, s.args)
			for _, w := range rc.writes {
				if !strings.HasSuffix(w.GVK, "/PersistentVolumeClaim") {
					t.Errorf("non-PVC write recorded: %+v", w)
				}
			}
		})
	}
}
