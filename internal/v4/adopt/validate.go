package adopt

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/planner"
)

// Namespace names referenced by the deny-list and elsewhere.
const (
	nsKubeSystem       = "kube-system"
	nsKubePublic       = "kube-public"
	nsKubeNodeLease    = "kube-node-lease"
	nsVolSyncSystem    = "volsync-system"
	nsArgoCD           = "argocd"
	nsLonghornSystem   = "longhorn-system"
	nsPVCPlumberSystem = "pvc-plumber-system"
	nsExternalSecrets  = "external-secrets"
	nsCertManager      = "cert-manager"
	nsFluxSystem       = "flux-system"
	nsLocalPathStorage = "local-path-storage"
)

// systemNamespaces is the deny-list adopt refuses to operate in.
// Mirrors the talos repo's CLAUDE.md rule "DO NOT add backup labels to
// system namespace PVCs" and adds pvc-plumber's own control namespaces.
// Never overridable.
var systemNamespaces = map[string]struct{}{
	nsKubeSystem:       {},
	nsKubePublic:       {},
	nsKubeNodeLease:    {},
	nsVolSyncSystem:    {},
	nsArgoCD:           {},
	nsLonghornSystem:   {},
	nsPVCPlumberSystem: {},
	nsExternalSecrets:  {},
	nsCertManager:      {},
	nsFluxSystem:       {},
	nsLocalPathStorage: {},
}

// PlanFor evaluates the adoption pipeline against the live cluster
// state and returns a Plan. The client.Reader signature is the
// read-only contract — the package cannot mutate cluster state.
//
// Order of operations (matches the approved 7.1 plan):
//
//  1. System-namespace deny-list
//  2. Tier sanity (parseTierString)
//  3. PVC Get + Bound check
//  4. Exempt check
//  5. Namespace privileged-movers
//  6. PVC labels.Parse → Spec.Errors block
//  7. Read RS + RD; build CurrentVolSyncSummary; classify owner
//  8. Build ExpectedVolSyncSummary
//  9. Verdict branching:
//     - V4 gates live + operator-owned shape-matching RS/RD → AlreadyAdopted
//     - V4 gates live + non-operator-owned or missing → labels-present
//     warnings (handoff-pending or resources-not-managed)
//     - Owner unknown → BlockerOwnerUnknown
//     - RS missing → BlockerRSMissing
//     - RD missing → BlockerRDMissing
//     - Shape mismatch → shape blockers (overridable per field)
//  10. dataSourceRef drift → warning (NEVER a blocker, by design)
//  11. Legacy backup label → warning
//  12. Freshness check (skipped for disabled / manual; tier-relative)
//  13. Compute LabelsToWrite + AnnotationsToWrite (when not Blocked /
//     AlreadyAdopted)
//  14. Resolve verdict: Blockers → Blocked; else Warnings → SafeWithWarnings;
//     else SafeToAdopt
func PlanFor(ctx context.Context, c client.Reader, in Inputs) (Plan, error) {
	return planForAt(ctx, c, in, time.Now())
}

// planForAt is the testable form of PlanFor: caller injects "now" so
// freshness windows are deterministic. Exported PlanFor calls this with
// time.Now().
func planForAt(ctx context.Context, c client.Reader, in Inputs, now time.Time) (Plan, error) {
	p := Plan{
		PVC: PVCSummary{
			Namespace: in.Namespace,
			Name:      in.PVCName,
		},
	}

	// 1. System-namespace deny-list — comes first so an invalid tier in
	// kube-system still reports system-ns.
	if _, blocked := systemNamespaces[in.Namespace]; blocked {
		p.Blockers = append(p.Blockers, Blocker{
			Class:  BlockerSystemNamespace,
			Detail: "namespace " + in.Namespace + " is in the adopt deny-list",
		})
		p.Verdict = VerdictBlocked
		return p, nil
	}

	// 2. Tier sanity.
	tier, tierOK := parseTierString(in.Tier)
	if !tierOK {
		p.Blockers = append(p.Blockers, Blocker{
			Class:          BlockerInvalidTier,
			Detail:         fmt.Sprintf("invalid tier %q (expected hourly|daily|weekly|manual|disabled)", in.Tier),
			ResolvableWith: "supply a valid Tier value",
		})
		p.Verdict = VerdictBlocked
		return p, nil
	}

	// 3. PVC read.
	pvc := &corev1.PersistentVolumeClaim{}
	err := c.Get(ctx, types.NamespacedName{Namespace: in.Namespace, Name: in.PVCName}, pvc)
	if apierrors.IsNotFound(err) {
		p.Blockers = append(p.Blockers, Blocker{
			Class:  BlockerPVCNotFound,
			Detail: "PVC " + in.Namespace + "/" + in.PVCName + " not found",
		})
		p.Verdict = VerdictBlocked
		return p, nil
	}
	if err != nil {
		return Plan{}, fmt.Errorf("get PVC %s/%s: %w", in.Namespace, in.PVCName, err)
	}
	p.PVC.Phase = pvc.Status.Phase
	p.PVC.StorageClass = derefString(pvc.Spec.StorageClassName)
	p.PVC.AccessModes = pvc.Spec.AccessModes
	if c := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; !c.IsZero() {
		p.PVC.Capacity = c
	}
	if pvc.Spec.DataSourceRef != nil {
		p.PVC.DataSourceRef = pvc.Spec.DataSourceRef.Name
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		p.Blockers = append(p.Blockers, Blocker{
			Class:  BlockerPVCNotBound,
			Detail: fmt.Sprintf("PVC phase is %q, expected Bound", pvc.Status.Phase),
		})
		p.Verdict = VerdictBlocked
		return p, nil
	}

	// 4-6. Parse PVC metadata.
	parsed := labels.Parse(pvc.Labels, pvc.Annotations)
	p.Spec = parsed
	p.PVC.Exempt = parsed.ExemptKind == labels.ExemptValid
	p.PVC.ExemptReason = parsed.ExemptReason
	p.PVC.HasLegacyBackupLabel = parsed.LegacyTier != labels.TierUnspecified
	p.PVC.V4GatesLive = parsed.Enabled && parsed.ManageVolSync

	// 4. Exempt check. Both valid-exempt and missing-reason-exempt block.
	if parsed.ExemptKind != labels.ExemptNone {
		detail := "PVC carries backup-exempt=true"
		if parsed.ExemptKind == labels.ExemptMissingReason {
			detail += " but the FQ reason annotation (storage.vanillax.dev/backup-exempt-reason) is missing"
		}
		p.Blockers = append(p.Blockers, Blocker{
			Class:  BlockerBackupExempt,
			Detail: detail,
		})
		p.Verdict = VerdictBlocked
		return p, nil
	}

	// 5. Spec.Errors. The labels parser folds malformed UID/GID/FSGroup,
	// invalid manage-volsync value, etc. into Spec.Errors. Surface each
	// as its own blocker.
	for _, e := range parsed.Errors {
		p.Blockers = append(p.Blockers, Blocker{
			Class:  BlockerSpecParseError,
			Detail: e.Error(),
		})
	}
	if len(p.Blockers) > 0 {
		p.Verdict = VerdictBlocked
		return p, nil
	}

	// 6. Namespace privileged-movers gate.
	ns := &corev1.Namespace{}
	err = c.Get(ctx, types.NamespacedName{Name: in.Namespace}, ns)
	if err != nil && !apierrors.IsNotFound(err) {
		return Plan{}, fmt.Errorf("get Namespace %s: %w", in.Namespace, err)
	}
	if err == nil {
		p.PVC.PrivilegedMovers = labels.NamespaceHasPrivilegedMovers(ns.Labels)
	}
	if !p.PVC.PrivilegedMovers {
		p.Blockers = append(p.Blockers, Blocker{
			Class:          BlockerMissingPrivilegedMovers,
			Detail:         "namespace " + in.Namespace + " lacks " + labels.NamespacePrivilegedMoversLabel + "=true",
			ResolvableWith: "label the namespace before adopting: kubectl label ns " + in.Namespace + " " + labels.NamespacePrivilegedMoversLabel + "=true",
		})
		p.Verdict = VerdictBlocked
		return p, nil
	}

	// 7. Read RS + RD; build CurrentVolSyncSummary; classify owner.
	rs, err := readReplicationSource(ctx, c, in.Namespace, in.PVCName)
	if err != nil {
		return Plan{}, err
	}
	rd, err := readReplicationDestination(ctx, c, in.Namespace, in.PVCName)
	if err != nil {
		return Plan{}, err
	}
	p.Current = extractCurrent(rs, rd)

	// 8. Build ExpectedVolSyncSummary.
	pvcCap := ""
	if !p.PVC.Capacity.IsZero() {
		pvcCap = p.PVC.Capacity.String()
	}
	p.Expected = buildExpected(in, parsed, pvcCap, p.PVC.StorageClass)

	// 9. Verdict branching based on (V4GatesLive, Owner, shape).

	// 9a. Owner-unknown is a hard stop regardless of label state.
	if p.Current.Owner == planner.OwnerUnknown {
		p.Blockers = append(p.Blockers, Blocker{
			Class:  BlockerOwnerUnknown,
			Detail: "RS/RD carries an unrecognized app.kubernetes.io/managed-by value",
		})
		p.Verdict = VerdictBlocked
		return p, nil
	}

	// 9b. Missing-resource blockers (only when at least one of RS/RD is
	// expected to exist). For VerdictAlreadyAdopted to be valid, BOTH
	// must be present and operator-owned — we'll re-check below.
	rsMissing := !p.Current.RSPresent
	rdMissing := !p.Current.RDPresent

	// 9c. V4 gates already live on the PVC.
	if p.PVC.V4GatesLive {
		switch {
		case p.Current.Owner == planner.OwnerPVCPlumber &&
			p.Current.RSPresent && p.Current.RDPresent &&
			shapeMatches(p.Current, p.Expected):
			// True steady-state AlreadyAdopted.
			p.Verdict = VerdictAlreadyAdopted
			return p, nil
		case p.Current.Owner == planner.OwnerPVCPlumber:
			// Operator owns the resources but shape drifted or one
			// child is missing. The reconciler will reconcile this on
			// its own; adopt should not write the labels again.
			p.Warnings = append(p.Warnings, Warning{
				Class:  WarningLabelsPresentResourcesMissing,
				Detail: "v4 gates live and operator owns RS/RD but shape drift or partial state detected; reconciler will resolve",
			})
		case rsMissing || rdMissing:
			// Labels are live but no RS/RD exist at all. Caller is
			// likely re-running adopt mid-handoff; safe to proceed.
			p.Warnings = append(p.Warnings, Warning{
				Class:  WarningLabelsPresentResourcesMissing,
				Detail: "v4 gates live but RS/RD missing; reconciler will create them on next pass",
			})
		case p.Current.Owner == planner.OwnerInlineArgo:
			// Adopt step 2 ran; Git step 5 (remove inline RS/RD) has
			// not happened yet. Expected intermediate state.
			p.Warnings = append(p.Warnings, Warning{
				Class:  WarningLabelsPresentButHandoffPending,
				Detail: "v4 gates live but inline-argo RS/RD still present; remove inline RS/RD from Git to complete handoff",
			})
		case p.Current.Owner == planner.OwnerUnmanagedOrGitopsObserved:
			p.Warnings = append(p.Warnings, Warning{
				Class:  WarningLabelsPresentButHandoffPending,
				Detail: "v4 gates live but unmanaged RS/RD still present; identify the Git source and remove",
			})
		}
		// Fall through to step 10+ so dataSourceRef drift, legacy
		// label, freshness, etc. still get warned. Verdict resolves at
		// step 14.
	}

	// 9d. Missing-resource blockers (only when not in a handoff state).
	if !p.PVC.V4GatesLive {
		if rsMissing {
			p.Blockers = append(p.Blockers, Blocker{
				Class:  BlockerRSMissing,
				Detail: "ReplicationSource/" + in.PVCName + " is not present",
			})
		}
		if rdMissing {
			p.Blockers = append(p.Blockers, Blocker{
				Class:  BlockerRDMissing,
				Detail: "ReplicationDestination/" + in.PVCName + "-dst is not present",
			})
		}
	}

	// 9e. Shape blockers (only when both children are present — no
	// point reporting shape drift on a missing child).
	if p.Current.RSPresent && p.Current.RDPresent {
		p.Blockers = append(p.Blockers, shapeBlockers(p.Current, p.Expected)...)
	}

	// 9f. Unmanaged-owner-with-shape-match → warning, not blocker.
	if p.Current.Owner == planner.OwnerUnmanagedOrGitopsObserved &&
		p.Current.RSPresent && p.Current.RDPresent &&
		shapeMatches(p.Current, p.Expected) {
		p.Warnings = append(p.Warnings, Warning{
			Class:  WarningUnmanagedOwnerShapeMatches,
			Detail: "RS/RD have no managed-by label but shape matches expected; identify the Git source for cleanup after handoff",
		})
	}

	// 10. dataSourceRef drift warning.
	expectedDSR := in.PVCName + "-dst"
	if p.PVC.DataSourceRef != "" && p.PVC.DataSourceRef != expectedDSR {
		p.Warnings = append(p.Warnings,
			Warning{
				Class:  WarningDataSourceRefDrift,
				Detail: "PVC dataSourceRef.name=" + p.PVC.DataSourceRef + " (expected " + expectedDSR + "); immutable on Bound PVCs",
			},
			Warning{
				Class:  WarningArgoComparisonErrorLikely,
				Detail: "PVC spec drift will likely produce ComparisonError on Argo sync; labels-in-Git may not apply through the PVC document",
			},
		)
	}

	// 11. Legacy backup label warning.
	if p.PVC.HasLegacyBackupLabel {
		p.Warnings = append(p.Warnings, Warning{
			Class:  WarningLegacyBackupLabel,
			Detail: "legacy backup=" + parsed.LegacyRaw + " label still present; clean up in the same Git PR",
		})
	}

	// 12. Schedule recomputed warning. Only meaningful when both
	// children are present and the live schedule differs from the
	// deterministic recomputation.
	if p.Current.RSPresent && p.Current.Schedule != "" && p.Current.Schedule != p.Expected.Schedule {
		p.Warnings = append(p.Warnings, Warning{
			Class:  WarningCronWillBeRecomputed,
			Detail: "current RS schedule=" + p.Current.Schedule + " differs from deterministic " + p.Expected.Schedule + "; operator will recompute on takeover",
		})
	}

	// 13. Freshness gates. Only run when no earlier blockers fired
	// (avoid double-reporting on already-malformed pairs).
	if len(p.Blockers) == 0 {
		p.Blockers = append(p.Blockers, freshnessBlockers(in, tier, p.Current, now)...)
	}

	// 14. Final verdict resolution + LabelsToWrite/AnnotationsToWrite.
	if len(p.Blockers) > 0 {
		p.Verdict = VerdictBlocked
		return p, nil
	}
	if !p.PVC.V4GatesLive {
		// Caller still needs to write the labels.
		p.LabelsToWrite = computeLabels(in.Tier)
		p.AnnotationsToWrite = computeAnnotations(in)
	} else {
		// Labels already live — adopt's job is done. We may still emit
		// override-annotation suggestions if overrides differ from
		// defaults and aren't already present, but Patch 7.1's surface
		// only describes the gate labels; the CLI re-write of stale
		// annotation overrides is a 7.2+ concern. Keep empty.
		p.LabelsToWrite = nil
		p.AnnotationsToWrite = nil
	}
	if len(p.Warnings) > 0 {
		p.Verdict = VerdictSafeToAdoptWithWarnings
	} else {
		p.Verdict = VerdictSafeToAdopt
	}
	return p, nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
