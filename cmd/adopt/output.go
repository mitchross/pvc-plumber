package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/mitchross/pvc-plumber/internal/v4/adopt"
)

// outputSchemaVersion is the stable JSON schema marker. Bumped only
// when the JSON shape changes incompatibly; new optional fields can
// be added without a version bump.
const outputSchemaVersion = "v1"

// renderPlan writes plan output in the requested format.
func renderPlan(w io.Writer, plan adopt.Plan, format string) error {
	switch format {
	case outJSON:
		return renderPlanJSON(w, plan)
	default:
		return renderPlanTable(w, plan)
	}
}

// renderApply writes apply output (success or error) in the requested
// format. applyErr may be nil (success), *adopt.ConflictError, or
// *adopt.RefusedError, or any infrastructure error.
func renderApply(w io.Writer, result adopt.ApplyResult, applyErr error, format string, dryRun bool) error {
	switch format {
	case outJSON:
		return renderApplyJSON(w, result, applyErr, dryRun)
	default:
		return renderApplyTable(w, result, applyErr, dryRun)
	}
}

// renderUndo writes undo output.
func renderUndo(w io.Writer, result adopt.ApplyResult, undoErr error, format string, dryRun bool) error {
	switch format {
	case outJSON:
		return renderUndoJSON(w, result, undoErr, dryRun)
	default:
		return renderUndoTable(w, result, undoErr, dryRun)
	}
}

// =============================================================================
// Table renderers
// =============================================================================

func renderPlanTable(w io.Writer, plan adopt.Plan) error {
	_, _ = fmt.Fprintf(w, "PVC:      %s/%s\n", plan.PVC.Namespace, plan.PVC.Name)
	_, _ = fmt.Fprintf(w, "Verdict:  %s\n", plan.Verdict)
	if plan.Spec.Tier != 0 {
		_, _ = fmt.Fprintf(w, "Tier:     %s\n", plan.Spec.Tier)
	}

	if len(plan.Blockers) > 0 {
		_, _ = fmt.Fprintln(w, "\nBlockers:")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		for _, b := range plan.Blockers {
			_, _ = fmt.Fprintf(tw, "  X\t%s\t%s\n", b.Class, b.Detail)
			if b.ResolvableWith != "" {
				_, _ = fmt.Fprintf(tw, "  \t\t→ %s\n", b.ResolvableWith)
			}
		}
		_ = tw.Flush()
	}

	if len(plan.Warnings) > 0 {
		_, _ = fmt.Fprintln(w, "\nWarnings:")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		for _, wn := range plan.Warnings {
			_, _ = fmt.Fprintf(tw, "  !\t%s\t%s\n", wn.Class, wn.Detail)
		}
		_ = tw.Flush()
	}

	if plan.Current.RSPresent || plan.Current.RDPresent {
		_, _ = fmt.Fprintln(w, "\nCurrent VolSync:")
		_, _ = fmt.Fprintf(w, "  Owner:        %s\n", plan.Current.Owner)
		_, _ = fmt.Fprintf(w, "  RS present:   %t\n", plan.Current.RSPresent)
		_, _ = fmt.Fprintf(w, "  RD present:   %t\n", plan.Current.RDPresent)
		_, _ = fmt.Fprintf(w, "  Repository:   %s\n", plan.Current.RepoSecret)
		if plan.Current.LastSyncTime != nil {
			_, _ = fmt.Fprintf(w, "  LastSync:     %s\n", plan.Current.LastSyncTime.Format("2006-01-02T15:04:05Z"))
		}
	}

	if plan.Verdict == adopt.VerdictSafeToAdopt || plan.Verdict == adopt.VerdictSafeToAdoptWithWarnings {
		_, _ = fmt.Fprintln(w, "\nLabels to write:")
		writeSortedMap(w, plan.LabelsToWrite)
		if len(plan.AnnotationsToWrite) > 0 {
			_, _ = fmt.Fprintln(w, "\nAnnotations to write:")
			writeSortedMap(w, plan.AnnotationsToWrite)
		} else {
			_, _ = fmt.Fprintln(w, "\nAnnotations to write: (none — overrides match defaults)")
		}
	}
	return nil
}

func renderApplyTable(w io.Writer, result adopt.ApplyResult, applyErr error, dryRun bool) error {
	if applyErr != nil {
		var conflict *adopt.ConflictError
		var refused *adopt.RefusedError
		switch {
		case errors.As(applyErr, &conflict):
			_, _ = fmt.Fprintln(w, "\nApply REFUSED — ownership conflict:")
			for _, c := range conflict.Conflicts {
				_, _ = fmt.Fprintf(w, "  %s owned by %q (live=%q desired=%q)\n",
					c.Path, c.OwnedBy, c.LiveValue, c.DesiredValue)
			}
			_, _ = fmt.Fprintln(w, "\nRe-run with --confirm to force ownership transfer to pvc-plumber-adopt.")
		case errors.As(applyErr, &refused):
			_, _ = fmt.Fprintf(w, "\nApply REFUSED: %s\n", refused.Reason)
		default:
			_, _ = fmt.Fprintf(w, "\nApply ERROR: %v\n", applyErr)
		}
		return nil
	}

	_, _ = fmt.Fprintln(w, "\nApply result:")
	_, _ = fmt.Fprintf(w, "  Patched: %t\n", result.Patched)
	_, _ = fmt.Fprintf(w, "  DryRun:  %t\n", dryRun)
	if len(result.WroteLabels) > 0 {
		_, _ = fmt.Fprintln(w, "\nLabels written:")
		writeSortedMap(w, result.WroteLabels)
	}
	if len(result.WroteAnnotations) > 0 {
		_, _ = fmt.Fprintln(w, "\nAnnotations written:")
		writeSortedMap(w, result.WroteAnnotations)
	}
	return nil
}

func renderUndoTable(w io.Writer, result adopt.ApplyResult, undoErr error, dryRun bool) error {
	if undoErr != nil {
		var refused *adopt.RefusedError
		if errors.As(undoErr, &refused) {
			_, _ = fmt.Fprintf(w, "\nUndo REFUSED: %s\n", refused.Reason)
		} else {
			_, _ = fmt.Fprintf(w, "\nUndo ERROR: %v\n", undoErr)
		}
		return nil
	}

	if !result.Patched {
		_, _ = fmt.Fprintln(w, "\nUndo: nothing to do.")
		_, _ = fmt.Fprintln(w, "  None of the three pvc-plumber.io gate labels are live on this PVC.")
		return nil
	}
	_, _ = fmt.Fprintln(w, "\nUndo result:")
	_, _ = fmt.Fprintf(w, "  Patched: %t\n", result.Patched)
	_, _ = fmt.Fprintf(w, "  DryRun:  %t\n", dryRun)
	_, _ = fmt.Fprintln(w, "\nNote: rollback of inline RS/RD remains a manual Git/Argo step.")
	_, _ = fmt.Fprintln(w, "      See docs/pvc-plumber-v4-cutover.md (talos-argocd-proxmox repo).")
	return nil
}

// =============================================================================
// JSON renderers — stable schema
// =============================================================================
//
// Schema v1:
//
//   plan:
//     { schema_version, command="plan", verdict, pvc{...}, current{...},
//       expected{...}, blockers[], warnings[], labels_to_write{},
//       annotations_to_write{} }
//
//   apply success:
//     { schema_version, command="apply", verdict, patched, dry_run,
//       before_labels{}, after_labels{}, before_annotations{},
//       after_annotations{}, wrote_labels{}, wrote_annotations{} }
//
//   apply error:
//     { schema_version, command="apply", error{kind, conflicts[]|reason} }
//
//   undo success:
//     { schema_version, command="undo", patched, dry_run,
//       before_labels{}, after_labels{} }
//
//   undo error:
//     { schema_version, command="undo", error{kind, reason} }

type jsonBlocker struct {
	Class          string `json:"class"`
	Detail         string `json:"detail"`
	ResolvableWith string `json:"resolvable_with,omitempty"`
}

type jsonWarning struct {
	Class  string `json:"class"`
	Detail string `json:"detail"`
}

type jsonConflict struct {
	Path         string `json:"path"`
	OwnedBy      string `json:"owned_by"`
	LiveValue    string `json:"live_value"`
	DesiredValue string `json:"desired_value"`
}

type jsonPlanOutput struct {
	SchemaVersion      string            `json:"schema_version"`
	Command            string            `json:"command"`
	Verdict            string            `json:"verdict"`
	PVCNamespace       string            `json:"pvc_namespace"`
	PVCName            string            `json:"pvc_name"`
	Blockers           []jsonBlocker     `json:"blockers"`
	Warnings           []jsonWarning     `json:"warnings"`
	LabelsToWrite      map[string]string `json:"labels_to_write"`
	AnnotationsToWrite map[string]string `json:"annotations_to_write"`
}

func renderPlanJSON(w io.Writer, plan adopt.Plan) error {
	out := jsonPlanOutput{
		SchemaVersion:      outputSchemaVersion,
		Command:            cmdPlan,
		Verdict:            string(plan.Verdict),
		PVCNamespace:       plan.PVC.Namespace,
		PVCName:            plan.PVC.Name,
		Blockers:           []jsonBlocker{},
		Warnings:           []jsonWarning{},
		LabelsToWrite:      plan.LabelsToWrite,
		AnnotationsToWrite: plan.AnnotationsToWrite,
	}
	for _, b := range plan.Blockers {
		out.Blockers = append(out.Blockers, jsonBlocker{
			Class: string(b.Class), Detail: b.Detail, ResolvableWith: b.ResolvableWith,
		})
	}
	for _, wn := range plan.Warnings {
		out.Warnings = append(out.Warnings, jsonWarning{Class: string(wn.Class), Detail: wn.Detail})
	}
	if out.LabelsToWrite == nil {
		out.LabelsToWrite = map[string]string{}
	}
	if out.AnnotationsToWrite == nil {
		out.AnnotationsToWrite = map[string]string{}
	}
	return writeJSON(w, out)
}

type jsonApplyOutput struct {
	SchemaVersion     string             `json:"schema_version"`
	Command           string             `json:"command"`
	Verdict           string             `json:"verdict,omitempty"`
	Patched           bool               `json:"patched"`
	DryRun            bool               `json:"dry_run"`
	BeforeLabels      map[string]string  `json:"before_labels"`
	AfterLabels       map[string]string  `json:"after_labels"`
	BeforeAnnotations map[string]string  `json:"before_annotations"`
	AfterAnnotations  map[string]string  `json:"after_annotations"`
	WroteLabels       map[string]string  `json:"wrote_labels"`
	WroteAnnotations  map[string]string  `json:"wrote_annotations"`
	Error             *jsonErrorEnvelope `json:"error,omitempty"`
}

type jsonErrorEnvelope struct {
	Kind      string         `json:"kind"`
	Reason    string         `json:"reason,omitempty"`
	Conflicts []jsonConflict `json:"conflicts,omitempty"`
}

func renderApplyJSON(w io.Writer, result adopt.ApplyResult, applyErr error, dryRun bool) error {
	out := jsonApplyOutput{
		SchemaVersion:     outputSchemaVersion,
		Command:           cmdApply,
		Patched:           result.Patched,
		DryRun:            dryRun,
		BeforeLabels:      emptyIfNil(result.BeforeLabels),
		AfterLabels:       emptyIfNil(result.AfterLabels),
		BeforeAnnotations: emptyIfNil(result.BeforeAnnotations),
		AfterAnnotations:  emptyIfNil(result.AfterAnnotations),
		WroteLabels:       emptyIfNil(result.WroteLabels),
		WroteAnnotations:  emptyIfNil(result.WroteAnnotations),
	}
	if applyErr != nil {
		out.Error = errorEnvelope(applyErr)
	}
	return writeJSON(w, out)
}

type jsonUndoOutput struct {
	SchemaVersion     string             `json:"schema_version"`
	Command           string             `json:"command"`
	Patched           bool               `json:"patched"`
	DryRun            bool               `json:"dry_run"`
	BeforeLabels      map[string]string  `json:"before_labels"`
	AfterLabels       map[string]string  `json:"after_labels"`
	BeforeAnnotations map[string]string  `json:"before_annotations"`
	AfterAnnotations  map[string]string  `json:"after_annotations"`
	Error             *jsonErrorEnvelope `json:"error,omitempty"`
}

func renderUndoJSON(w io.Writer, result adopt.ApplyResult, undoErr error, dryRun bool) error {
	out := jsonUndoOutput{
		SchemaVersion:     outputSchemaVersion,
		Command:           cmdUndo,
		Patched:           result.Patched,
		DryRun:            dryRun,
		BeforeLabels:      emptyIfNil(result.BeforeLabels),
		AfterLabels:       emptyIfNil(result.AfterLabels),
		BeforeAnnotations: emptyIfNil(result.BeforeAnnotations),
		AfterAnnotations:  emptyIfNil(result.AfterAnnotations),
	}
	if undoErr != nil {
		out.Error = errorEnvelope(undoErr)
	}
	return writeJSON(w, out)
}

// errorEnvelope maps an error into the JSON envelope shape. Public
// kinds: "conflict", "refused", "infrastructure", "usage".
func errorEnvelope(err error) *jsonErrorEnvelope {
	if err == nil {
		return nil
	}
	var conflict *adopt.ConflictError
	if errors.As(err, &conflict) {
		env := &jsonErrorEnvelope{Kind: "conflict"}
		for _, c := range conflict.Conflicts {
			env.Conflicts = append(env.Conflicts, jsonConflict{
				Path: c.Path, OwnedBy: c.OwnedBy,
				LiveValue: c.LiveValue, DesiredValue: c.DesiredValue,
			})
		}
		return env
	}
	var refused *adopt.RefusedError
	if errors.As(err, &refused) {
		return &jsonErrorEnvelope{Kind: "refused", Reason: refused.Reason}
	}
	var usage *usageError
	if errors.As(err, &usage) {
		return &jsonErrorEnvelope{Kind: "usage", Reason: usage.msg}
	}
	return &jsonErrorEnvelope{Kind: "infrastructure", Reason: err.Error()}
}

// =============================================================================
// Helpers
// =============================================================================

func writeJSON(w io.Writer, v interface{}) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// writeSortedMap renders m as "  <key>\t<value>" lines via tabwriter,
// in lexical key order. The leading two-space indent is hard-coded — at
// present every renderer uses the same indent, so making it a parameter
// would add a knob with no live caller variation.
func writeSortedMap(w io.Writer, m map[string]string) {
	if len(m) == 0 {
		return
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, k := range keys {
		_, _ = fmt.Fprintf(tw, "  %s\t%s\n", k, m[k])
	}
	_ = tw.Flush()
}

func emptyIfNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// silence unused-import vet warnings if compilation drops one of the
// helper imports later.
var _ = strings.Contains
