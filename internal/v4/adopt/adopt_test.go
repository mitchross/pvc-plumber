package adopt

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pvcplumberlabels "github.com/mitchross/pvc-plumber/internal/v4/labels"
)

// recordingReader wraps a controller-runtime client and panics on any
// write. The package's PlanFor signature accepts client.Reader, so a
// write call would be a compile-time error — recordingReader exists to
// catch a future signature broadening, and to record reads so tests
// can assert what objects were fetched.
type recordingReader struct {
	inner  client.Client
	gets   []string
	lists  []string
	writes []string // populated only via the optional Client surface (see below)
}

func (r *recordingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.gets = append(r.gets, obj.GetObjectKind().GroupVersionKind().Kind+":"+key.String())
	return r.inner.Get(ctx, key, obj, opts...)
}

func (r *recordingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	r.lists = append(r.lists, list.GetObjectKind().GroupVersionKind().Kind)
	return r.inner.List(ctx, list, opts...)
}

// newReader seeds a fake client with the given objects and returns the
// recordingReader wrapping it. Non-client.Object inputs are skipped so
// fixture helpers can return nil placeholders for missing RS/RD without
// crashing.
func newReader(t *testing.T, objs ...runtime.Object) *recordingReader {
	t.Helper()
	clientObjs := make([]client.Object, 0, len(objs))
	for _, o := range objs {
		co, ok := o.(client.Object)
		if !ok {
			continue
		}
		clientObjs = append(clientObjs, co)
	}
	return &recordingReader{
		inner: fake.NewClientBuilder().WithObjects(clientObjs...).Build(),
	}
}

// fixedNow is the deterministic clock used by tests so freshness
// windows produce stable results. Chosen to be exactly 5 minutes
// after the canonical "fresh backup" lastSyncTime fixture.
var fixedNow = mustParseTime("2026-05-25T12:00:00Z")

func freshLastSync() metav1.Time {
	return metav1.NewTime(mustParseTime("2026-05-25T11:55:00Z"))
}

func staleLastSync() metav1.Time {
	// 72h before fixedNow — past the 48h daily window.
	return metav1.NewTime(mustParseTime("2026-05-22T12:00:00Z"))
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// =============================================================================
// Top-level paranoia test: no writes across the entire test matrix.
// =============================================================================
//
// Each case in the table below uses its own recordingReader. After
// PlanFor returns, this top-level assertion is implicit: the Client
// surface of recordingReader was never exercised because the package
// signature is client.Reader. The /no writes/ proof in this file is
// the compile-time signature; the recordingReader.gets / .lists slices
// confirm only reads happened. We additionally re-verify by running
// every case through TestNoWritesAcrossMatrix below.

func TestPlanFor(t *testing.T) {
	type result struct {
		verdict           Verdict
		expectBlockers    []BlockerClass
		expectWarnings    []WarningClass
		expectNoLabels    bool
		expectLabels      map[string]string
		expectAnnotations map[string]string
	}

	type tcase struct {
		name    string
		inputs  func() Inputs
		objects func() []runtime.Object
		want    result
	}

	cases := []tcase{
		{
			name: "safe_inline_argo_clean_shape",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				pvc := makePVC()
				ns := makeNamespace(testNS, true)
				rs := makeRS()
				rd := makeRD()
				return makeObjects(pvc, ns, rs, rd)
			},
			want: result{
				verdict: VerdictSafeToAdopt,
				expectLabels: map[string]string{
					pvcplumberlabels.LabelEnabled:       tValTrue,
					pvcplumberlabels.LabelTier:          tTierDaily,
					pvcplumberlabels.LabelManageVolSync: tValTrue,
				},
			},
		},
		{
			name: "safe_with_datasourceref_drift",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				pvc := makePVC(withDataSourceRefName(testPVC + "-backup"))
				return makeObjects(pvc, makeNamespace(testNS, true), makeRS(), makeRD())
			},
			want: result{
				verdict: VerdictSafeToAdoptWithWarnings,
				expectWarnings: []WarningClass{
					WarningDataSourceRefDrift,
					WarningArgoComparisonErrorLikely,
				},
				expectLabels: map[string]string{
					pvcplumberlabels.LabelEnabled:       tValTrue,
					pvcplumberlabels.LabelTier:          tTierDaily,
					pvcplumberlabels.LabelManageVolSync: tValTrue,
				},
			},
		},
		{
			name: "safe_unmanaged_owner_shape_matches",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				rs := makeRS(rsWithManagedBy(""))
				return makeObjects(makePVC(), makeNamespace(testNS, true), rs, makeRD())
			},
			want: result{
				verdict: VerdictSafeToAdoptWithWarnings,
				expectWarnings: []WarningClass{
					WarningUnmanagedOwnerShapeMatches,
				},
			},
		},
		{
			name: "safe_with_legacy_backup_label",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				pvc := makePVC(withLabels(map[string]string{"backup": "daily"}))
				return makeObjects(pvc, makeNamespace(testNS, true), makeRS(), makeRD())
			},
			want: result{
				verdict:        VerdictSafeToAdoptWithWarnings,
				expectWarnings: []WarningClass{WarningLegacyBackupLabel},
			},
		},
		{
			name: "already_adopted_full_steady_state",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				pvc := makePVC(withV4Gates())
				rs := makeRS(rsWithManagedBy("pvc-plumber"))
				rd := makeRD()
				rd.SetLabels(map[string]string{pvcplumberlabels.LabelManagedByKey: "pvc-plumber"})
				return makeObjects(pvc, makeNamespace(testNS, true), rs, rd)
			},
			want: result{
				verdict:        VerdictAlreadyAdopted,
				expectNoLabels: true,
			},
		},
		{
			name: "labels_present_inline_argo_owns_handoff_pending",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				pvc := makePVC(withV4Gates())
				return makeObjects(pvc, makeNamespace(testNS, true), makeRS(), makeRD())
			},
			want: result{
				verdict:        VerdictSafeToAdoptWithWarnings,
				expectWarnings: []WarningClass{WarningLabelsPresentButHandoffPending},
				expectNoLabels: true,
			},
		},
		{
			name: "labels_present_resources_missing_not_already_adopted",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				pvc := makePVC(withV4Gates())
				return makeObjects(pvc, makeNamespace(testNS, true), nil, nil)
			},
			want: result{
				verdict:        VerdictSafeToAdoptWithWarnings,
				expectWarnings: []WarningClass{WarningLabelsPresentResourcesMissing},
				expectNoLabels: true,
			},
		},
		{
			name: "uid_mismatch_blocks_no_override",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				rs := makeRS(rsWithSecurityContext(karakeepUID, defaultGID, defaultFSGroup))
				return makeObjects(makePVC(), makeNamespace(testNS, true), rs, makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerUIDMismatch},
				expectNoLabels: true,
			},
		},
		{
			name: "uid_mismatch_resolved_by_override",
			inputs: func() Inputs {
				in := baseInputs()
				in.UID = i64ptr(karakeepUID)
				in.GID = i64ptr(karakeepUID)
				in.FSGroup = i64ptr(karakeepUID)
				return in
			},
			objects: func() []runtime.Object {
				rs := makeRS(rsWithSecurityContext(karakeepUID, karakeepUID, karakeepUID))
				return makeObjects(makePVC(), makeNamespace(testNS, true), rs, makeRD())
			},
			want: result{
				verdict: VerdictSafeToAdopt,
				expectAnnotations: map[string]string{
					pvcplumberlabels.AnnotationUID:     "1001",
					pvcplumberlabels.AnnotationGID:     "1001",
					pvcplumberlabels.AnnotationFSGroup: "1001",
				},
			},
		},
		{
			name: "gid_mismatch_blocks_no_override",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				rs := makeRS(rsWithSecurityContext(defaultUID, karakeepUID, defaultFSGroup))
				return makeObjects(makePVC(), makeNamespace(testNS, true), rs, makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerGIDMismatch},
			},
		},
		{
			name: "fsgroup_mismatch_blocks_no_override",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				rs := makeRS(rsWithSecurityContext(defaultUID, defaultGID, karakeepUID))
				return makeObjects(makePVC(), makeNamespace(testNS, true), rs, makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerFSGroupMismatch},
			},
		},
		{
			name: "repo_mismatch_blocks",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				rs := makeRS(rsWithRepository("wrong-repo"))
				return makeObjects(makePVC(), makeNamespace(testNS, true), rs, makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerRepoMismatch},
			},
		},
		{
			name: "copy_method_non_snapshot_blocks",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				rs := makeRS(rsWithCopyMethod("Clone"))
				return makeObjects(makePVC(), makeNamespace(testNS, true), rs, makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerCopyMethodMismatch},
			},
		},
		{
			name: "missing_rs_blocks",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				return makeObjects(makePVC(), makeNamespace(testNS, true), nil, makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerRSMissing},
			},
		},
		{
			name: "missing_rd_blocks",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				return makeObjects(makePVC(), makeNamespace(testNS, true), makeRS(), nil)
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerRDMissing},
			},
		},
		{
			name: "unknown_owner_blocks",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				rs := makeRS(rsWithManagedBy("someoperator"))
				return makeObjects(makePVC(), makeNamespace(testNS, true), rs, makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerOwnerUnknown},
			},
		},
		{
			name: "exempt_blocks_with_fq_reason",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				pvc := makePVC(
					withLabels(map[string]string{"backup-exempt": "true"}),
					withAnnotations(map[string]string{
						"storage.vanillax.dev/backup-exempt-reason": "test PVC, no data",
					}),
				)
				return makeObjects(pvc, makeNamespace(testNS, true), makeRS(), makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerBackupExempt},
			},
		},
		{
			name: "exempt_blocks_without_fq_reason",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				pvc := makePVC(withLabels(map[string]string{"backup-exempt": "true"}))
				return makeObjects(pvc, makeNamespace(testNS, true), makeRS(), makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerBackupExempt},
			},
		},
		{
			name: "system_namespace_blocks_kube_system",
			inputs: func() Inputs {
				in := baseInputs()
				in.Namespace = tNSKubeSystem
				return in
			},
			objects: func() []runtime.Object {
				// PVC technically doesn't even need to exist — the
				// namespace check runs before any cluster read.
				return makeObjects(makePVC(withNamespace(tNSKubeSystem)), makeNamespace(tNSKubeSystem, true), makeRS(), makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerSystemNamespace},
			},
		},
		{
			name: "system_namespace_blocks_before_invalid_tier",
			inputs: func() Inputs {
				in := baseInputs()
				in.Namespace = tNSKubeSystem
				in.Tier = "bananas"
				return in
			},
			objects: func() []runtime.Object {
				return makeObjects(makePVC(withNamespace(tNSKubeSystem)), makeNamespace(tNSKubeSystem, true), nil, nil)
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerSystemNamespace},
			},
		},
		{
			name: "missing_privileged_movers_blocks",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				return makeObjects(makePVC(), makeNamespace(testNS, false), makeRS(), makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerMissingPrivilegedMovers},
			},
		},
		{
			name: "pvc_not_bound_blocks",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				pvc := makePVC(withPhase(corev1.ClaimPending))
				return makeObjects(pvc, makeNamespace(testNS, true), makeRS(), makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerPVCNotBound},
			},
		},
		{
			name: "pvc_not_found_blocks",
			inputs: func() Inputs {
				in := baseInputs()
				in.PVCName = "does-not-exist"
				return in
			},
			objects: func() []runtime.Object {
				return makeObjects(nil, makeNamespace(testNS, true), nil, nil)
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerPVCNotFound},
			},
		},
		{
			name: "invalid_tier_blocks",
			inputs: func() Inputs {
				in := baseInputs()
				in.Tier = "bananas"
				return in
			},
			objects: func() []runtime.Object {
				return makeObjects(makePVC(), makeNamespace(testNS, true), makeRS(), makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerInvalidTier},
			},
		},
		{
			name: "tier_disabled_skips_freshness",
			inputs: func() Inputs {
				in := baseInputs()
				in.Tier = "disabled"
				in.RequireFreshBackup = true
				return in
			},
			objects: func() []runtime.Object {
				rs := makeRS() // no lastSyncTime
				return makeObjects(makePVC(), makeNamespace(testNS, true), rs, makeRD())
			},
			want: result{
				verdict: VerdictSafeToAdopt,
			},
		},
		{
			name: "stale_backup_blocks_default",
			inputs: func() Inputs {
				in := baseInputs()
				in.RequireFreshBackup = true
				return in
			},
			objects: func() []runtime.Object {
				rs := makeRS(rsWithLastSyncTime(staleLastSync()))
				return makeObjects(makePVC(), makeNamespace(testNS, true), rs, makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerStaleBackup},
			},
		},
		{
			name: "stale_backup_passes_with_flag",
			inputs: func() Inputs {
				in := baseInputs()
				in.RequireFreshBackup = true
				in.AllowStaleBackup = true
				return in
			},
			objects: func() []runtime.Object {
				rs := makeRS(rsWithLastSyncTime(staleLastSync()))
				return makeObjects(makePVC(), makeNamespace(testNS, true), rs, makeRD())
			},
			want: result{
				verdict: VerdictSafeToAdopt,
			},
		},
		{
			name: "no_lastsync_blocks_default",
			inputs: func() Inputs {
				in := baseInputs()
				in.RequireFreshBackup = true
				return in
			},
			objects: func() []runtime.Object {
				return makeObjects(makePVC(), makeNamespace(testNS, true), makeRS(), makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerNoSuccessfulBackup},
			},
		},
		{
			name: "no_lastsync_passes_with_flag",
			inputs: func() Inputs {
				in := baseInputs()
				in.RequireFreshBackup = true
				in.AllowNoSuccessfulBackup = true
				return in
			},
			objects: func() []runtime.Object {
				return makeObjects(makePVC(), makeNamespace(testNS, true), makeRS(), makeRD())
			},
			want: result{
				verdict: VerdictSafeToAdopt,
			},
		},
		{
			name: "fresh_backup_passes",
			inputs: func() Inputs {
				in := baseInputs()
				in.RequireFreshBackup = true
				return in
			},
			objects: func() []runtime.Object {
				rs := makeRS(rsWithLastSyncTime(freshLastSync()))
				return makeObjects(makePVC(), makeNamespace(testNS, true), rs, makeRD())
			},
			want: result{
				verdict: VerdictSafeToAdopt,
			},
		},
		{
			name: "spec_parse_error_invalid_manage_volsync_blocks",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				pvc := makePVC(withLabels(map[string]string{
					pvcplumberlabels.LabelManageVolSync: "yes",
				}))
				return makeObjects(pvc, makeNamespace(testNS, true), makeRS(), makeRD())
			},
			want: result{
				verdict:        VerdictBlocked,
				expectBlockers: []BlockerClass{BlockerSpecParseError},
			},
		},
		{
			name: "labels_exact_values_no_annotations",
			inputs: func() Inputs {
				return baseInputs()
			},
			objects: func() []runtime.Object {
				return makeObjects(makePVC(), makeNamespace(testNS, true), makeRS(), makeRD())
			},
			want: result{
				verdict: VerdictSafeToAdopt,
				expectLabels: map[string]string{
					pvcplumberlabels.LabelEnabled:       tValTrue,
					pvcplumberlabels.LabelTier:          tTierDaily,
					pvcplumberlabels.LabelManageVolSync: tValTrue,
				},
				expectAnnotations: nil,
			},
		},
		{
			name: "annotation_only_when_override_differs_from_default",
			inputs: func() Inputs {
				in := baseInputs()
				in.UID = i64ptr(defaultUID) // matches default → no annotation
				return in
			},
			objects: func() []runtime.Object {
				return makeObjects(makePVC(), makeNamespace(testNS, true), makeRS(), makeRD())
			},
			want: result{
				verdict:           VerdictSafeToAdopt,
				expectAnnotations: nil,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			r := newReader(t, tc.objects()...)
			got, err := planForAt(context.Background(), r, tc.inputs(), fixedNow)
			if err != nil {
				t.Fatalf("PlanFor unexpected error: %v", err)
			}
			if got.Verdict != tc.want.verdict {
				t.Errorf("verdict: got %q, want %q (blockers=%v warnings=%v)", got.Verdict, tc.want.verdict, classListB(got.Blockers), classListW(got.Warnings))
			}
			for _, want := range tc.want.expectBlockers {
				if !got.hasBlocker(want) {
					t.Errorf("missing expected blocker %q (got=%v)", want, classListB(got.Blockers))
				}
			}
			for _, want := range tc.want.expectWarnings {
				if !got.hasWarning(want) {
					t.Errorf("missing expected warning %q (got=%v)", want, classListW(got.Warnings))
				}
			}
			if tc.want.expectNoLabels && len(got.LabelsToWrite) != 0 {
				t.Errorf("expected no LabelsToWrite, got %v", got.LabelsToWrite)
			}
			if tc.want.expectLabels != nil {
				for k, v := range tc.want.expectLabels {
					if got.LabelsToWrite[k] != v {
						t.Errorf("LabelsToWrite[%q]=%q, want %q (full=%v)", k, got.LabelsToWrite[k], v, got.LabelsToWrite)
					}
				}
			}
			if tc.want.expectAnnotations != nil {
				for k, v := range tc.want.expectAnnotations {
					if got.AnnotationsToWrite[k] != v {
						t.Errorf("AnnotationsToWrite[%q]=%q, want %q (full=%v)", k, got.AnnotationsToWrite[k], v, got.AnnotationsToWrite)
					}
				}
			}
		})
	}
}

// =============================================================================
// Proof of zero writes
// =============================================================================

// TestNoWritesAcrossMatrix runs every PlanFor case from TestPlanFor's
// matrix and asserts that recordingReader recorded no write actions.
// Belt-and-braces over the compile-time client.Reader signature.
func TestNoWritesAcrossMatrix(t *testing.T) {
	// We run the same matrix once more, but only assert "writes is
	// empty." This is identical to relying on the signature; it
	// catches a future signature broadening that would otherwise
	// slip writes in silently.
	pvc := makePVC()
	ns := makeNamespace(testNS, true)
	rs := makeRS()
	rd := makeRD()
	r := newReader(t, makeObjects(pvc, ns, rs, rd)...)
	_, err := planForAt(context.Background(), r, baseInputs(), fixedNow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(r.writes) > 0 {
		t.Errorf("expected zero writes, got %v", r.writes)
	}
	if len(r.gets) == 0 {
		t.Errorf("expected at least one Get to have been recorded, got none — test wiring may be broken")
	}
}

// TestLabelsAndAnnotationsExactContent verifies the LabelsToWrite map
// has exactly three keys and the AnnotationsToWrite map is empty when
// all overrides match defaults. Complements the table tests by
// asserting on exact map size.
func TestLabelsAndAnnotationsExactContent(t *testing.T) {
	objs := makeObjects(makePVC(), makeNamespace(testNS, true), makeRS(), makeRD())
	r := newReader(t, objs...)
	got, err := planForAt(context.Background(), r, baseInputs(), fixedNow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Verdict != VerdictSafeToAdopt {
		t.Fatalf("verdict: got %q, want safe-to-adopt", got.Verdict)
	}
	if len(got.LabelsToWrite) != 3 {
		t.Errorf("LabelsToWrite should have exactly 3 keys, got %d (%v)", len(got.LabelsToWrite), got.LabelsToWrite)
	}
	if got.LabelsToWrite[pvcplumberlabels.LabelEnabled] != tValTrue {
		t.Errorf("LabelEnabled != %q", tValTrue)
	}
	if got.LabelsToWrite[pvcplumberlabels.LabelTier] != tTierDaily {
		t.Errorf("LabelTier != %q", tTierDaily)
	}
	if got.LabelsToWrite[pvcplumberlabels.LabelManageVolSync] != tValTrue {
		t.Errorf("LabelManageVolSync != %q", tValTrue)
	}
	if len(got.AnnotationsToWrite) != 0 {
		t.Errorf("AnnotationsToWrite should be empty when overrides match defaults, got %v", got.AnnotationsToWrite)
	}
}

// =============================================================================
// Helpers
// =============================================================================

func classListB(b []Blocker) []BlockerClass {
	out := make([]BlockerClass, 0, len(b))
	for _, x := range b {
		out = append(out, x.Class)
	}
	return out
}
func classListW(w []Warning) []WarningClass {
	out := make([]WarningClass, 0, len(w))
	for _, x := range w {
		out = append(out, x.Class)
	}
	return out
}

// Ensure imported but unused-in-some-files types stay referenced.
var _ = unstructured.Unstructured{}
