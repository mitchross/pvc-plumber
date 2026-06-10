package controller

import (
	"sort"
	"sync"
	"time"

	"github.com/mitchross/pvc-plumber/internal/v4/executor"
)

// Phase 5 — Patch 1: parity-report data model.
//
// This file defines the structured output of the future V4AuditReconciler
// (Patch 3) and the in-memory Store the /audit HTTP endpoint (Patch 4) will
// read. It is pure data + a small thread-safe registry — no reconciler
// logic, no HTTP server, no cluster comparison code yet.
//
// Why these types live in internal/controller, not internal/v4/decision:
// the v4 decision engine is pure ("what should exist for this PVC?"). The
// audit reconciler is what compares desired-vs-current and produces an
// ActionKind ("already-matches" / "would-create" / …). Keeping ActionKind
// + ParityEntry here preserves the engine's purity and groups the
// reporting model with the reconciler that produces it.

// ActionKind describes what the v4 audit reconciler would do for a PVC if
// it had write permission. In audit mode it never actually does any of
// these — the auditclient wrapper blocks all writes. The kind is purely
// reporting.
type ActionKind string

const (
	// ActionAlreadyMatches: every expected resource for this PVC is
	// present in the cluster and its shape matches the v4 expected spec
	// (RS+RD names, repository secret, kopia identity). No work needed.
	ActionAlreadyMatches ActionKind = "already-matches"

	// ActionWouldCreate: an expected resource is missing in the cluster
	// and the operator would create it. Most common during a fresh app
	// rollout before the inline RS/RD YAML lands.
	ActionWouldCreate ActionKind = "would-create"

	// ActionWouldUpdate: an expected resource is present but its spec
	// differs from what v4 would render. May be benign (e.g. schedule
	// minute differs) or significant (e.g. moverSecurityContext drift).
	ActionWouldUpdate ActionKind = "would-update"

	// ActionWouldAdopt: an existing resource carries the operator's
	// historical label (app.kubernetes.io/managed-by=pvc-plumber, used
	// by the v3.1 reconciler) and could be claimed by the v4 reconciler
	// in Phase 6. In audit, this is a flag for the operator to review.
	ActionWouldAdopt ActionKind = "would-adopt"

	// ActionWouldDelete: an operator-owned resource exists for a PVC
	// that is no longer eligible (label removed, marked exempt, PVC
	// deleted, etc.). Only set when the resource is unambiguously
	// operator-owned (managed-by=pvc-plumber). Inline-Argo and
	// unmanaged-or-gitops resources are NEVER flagged here.
	ActionWouldDelete ActionKind = "would-delete"

	// ActionInlineArgoObserved: an expected-name resource exists, is
	// not labeled managed-by=pvc-plumber, and matches the v4 expected
	// shape. Treated as observed-only — the audit reconciler will not
	// propose any action on these. Phase 7 cutover (per the PRD) is
	// the migration path: app PRs add pvc-plumber.io/enabled, remove
	// inline RS/RD; v4 then takes ownership.
	ActionInlineArgoObserved ActionKind = "inline-argo-observed"

	// ActionSkippedExempt: the PVC carries `backup-exempt: "true"` plus
	// the FQ reason annotation. Nothing to do; reported for completeness.
	ActionSkippedExempt ActionKind = "skipped-exempt"

	// ActionSkippedNotOptedIn: the PVC carries no opt-in signal — no v4
	// label and no legacy backup: label. Listed for visibility (so an
	// operator can see "did I miss labeling this PVC?") but no action.
	ActionSkippedNotOptedIn ActionKind = "skipped-not-opted-in"

	// ActionWriteGateMissing: the PVC IS visible to v4 (the operator
	// can see / protect / report it), but it is NOT write-eligible.
	// Two situations land here:
	//
	//   1. pvc-plumber.io/enabled="true" is set but
	//      pvc-plumber.io/manage-volsync="true" is missing or "false".
	//      The operator may report on the PVC but the explicit write
	//      fuse is off, so create/update/delete are not planned.
	//
	//   2. Only the legacy `backup: hourly|daily` label is set.
	//      Legacy alone is an audit/reporting opt-in but never a
	//      write opt-in — to make this PVC write-eligible an operator
	//      must add both pvc-plumber.io/enabled=true AND
	//      pvc-plumber.io/manage-volsync=true.
	//
	// Distinct from ActionSkippedNotOptedIn: the PVC IS opted in to
	// v4 reporting (label_source is v4 or legacy). It is just not
	// write-eligible.
	//
	// Distinct from ActionAlreadyMatches: emitted only when the
	// operator would OTHERWISE plan a write (create / update /
	// delete). If existing inline RS/RD already match the expected
	// shape, the verdict stays ActionAlreadyMatches and a note in
	// Blockers (or future Notes) records that writes are gated off.
	//
	// Distinct from ActionSkippedExempt: backup-exempt wins over
	// everything and short-circuits before the write gate is
	// evaluated. A PVC with both `backup-exempt: "true"` and
	// `pvc-plumber.io/manage-volsync: "true"` lands in
	// ActionSkippedExempt, not here.
	//
	// Emitted by the v4 planner (Patch 6.4). DecideAction does NOT
	// emit this in Patch 6.2 — only the ActionKind type + summary
	// bucket plumbing land in 6.2 so the planner can rely on the
	// constant in 6.4.
	ActionWriteGateMissing ActionKind = "write-gate-missing"

	// ActionNeedsHumanReview: the PVC has an opt-in signal but its
	// configuration is malformed (bad tier, bad UID/GID, exempt without
	// FQ reason, skip-restore without reason, etc.). The audit
	// reconciler reports the blockers and refuses to compute a clean
	// expected/current diff until they're resolved.
	ActionNeedsHumanReview ActionKind = "needs-human-review"

	// ActionSkippedNamespaceNotManaged: the PVC is fully write-eligible
	// (both fuse labels + valid tier) but its namespace lacks
	// pvc-plumber.io/managed-namespace=true. The operator suppresses all
	// writes (zero planned_ops). This is the namespace write gate (v4.0.1)
	// that makes a DRY cluster-wide RS/RD write ClusterRoleBinding safe.
	// Mirrors planner.ActionSkippedNamespaceNotManaged (same wire string).
	ActionSkippedNamespaceNotManaged ActionKind = "skipped-namespace-not-managed"
)

// AllActionKinds returns every defined ActionKind, sorted for deterministic
// iteration. Used by report aggregation and tests.
func AllActionKinds() []ActionKind {
	return []ActionKind{
		ActionAlreadyMatches,
		ActionInlineArgoObserved,
		ActionNeedsHumanReview,
		ActionSkippedExempt,
		ActionSkippedNamespaceNotManaged,
		ActionSkippedNotOptedIn,
		ActionWouldAdopt,
		ActionWouldCreate,
		ActionWouldDelete,
		ActionWouldUpdate,
		ActionWriteGateMissing,
	}
}

// OwnerClassification labels who appears to own an existing RS/RD/ES
// resource the audit reconciler observed. Used to decide whether the
// resource is a candidate for v4 adoption/deletion (only managed-by=
// pvc-plumber) or must be left alone (everything else).
type OwnerClassification string

const (
	// OwnerNone: no current resource exists. The Expected.* fields
	// describe what SHOULD exist; Current.* fields are empty.
	OwnerNone OwnerClassification = "none"

	// OwnerPVCPlumber: resource carries app.kubernetes.io/managed-by=
	// pvc-plumber. This is the only owner the v4 reconciler will ever
	// propose to adopt/update/delete.
	OwnerPVCPlumber OwnerClassification = "managed-by-pvc-plumber"

	// OwnerInlineArgo: resource carries app.kubernetes.io/managed-by=
	// argocd (or a similar GitOps controller). Authoritatively owned by
	// the GitOps pipeline; never a v4 adopt/delete candidate.
	OwnerInlineArgo OwnerClassification = "inline-argo"

	// OwnerUnmanagedOrGitopsObserved: resource exists without an
	// app.kubernetes.io/managed-by label (or with a non-pvc-plumber
	// label other than argocd), but its name + namespace + repository
	// + sourcePVC shape matches what v4 would expect. Treated as
	// "presumed GitOps-managed by some path we don't recognize" —
	// observed only, NOT a delete candidate. This is the catch-all that
	// keeps the audit reconciler safe when Argo's labels are missing or
	// when a Helm chart's `extraDeploy:` doesn't carry managed-by.
	OwnerUnmanagedOrGitopsObserved OwnerClassification = "unmanaged-or-gitops-observed"

	// OwnerUnknown: resource exists but its shape doesn't match v4
	// expectations AND it doesn't carry a recognizable managed-by
	// label. Reported as needs-human-review with a blocker.
	OwnerUnknown OwnerClassification = "unknown"
)

// AllOwnerClassifications returns every defined OwnerClassification,
// sorted for deterministic iteration.
func AllOwnerClassifications() []OwnerClassification {
	return []OwnerClassification{
		OwnerInlineArgo,
		OwnerNone,
		OwnerPVCPlumber,
		OwnerUnknown,
		OwnerUnmanagedOrGitopsObserved,
	}
}

// LabelSource records how a PVC was opted in for backup. This is the
// audit-only distinction that lets the report show "you have 27 PVCs
// still on legacy `backup:` labels and 0 on v4 labels — migration is
// not yet started." Future admission webhooks will still match only on
// pvc-plumber.io/enabled=true (per the PRD constraint), but the audit
// reconciler must treat both as opted in so the parity report covers
// the talos repo's current state.
type LabelSource string

const (
	// LabelSourceNone: the PVC has neither v4 nor legacy opt-in labels.
	// Reported via ActionSkippedNotOptedIn.
	LabelSourceNone LabelSource = "none"

	// LabelSourceV4: pvc-plumber.io/enabled=true is set. This is the
	// only origin a future admission webhook will recognize.
	LabelSourceV4 LabelSource = "v4"

	// LabelSourceLegacy: only the v1-v3 label `backup: hourly|daily|…`
	// is set. The audit reconciler still computes expected state for
	// these PVCs and includes them in the report — they're the bulk of
	// the talos repo today. Phase 7 cutover migrates them to V4 origin.
	LabelSourceLegacy LabelSource = "legacy"

	// LabelSourceBoth: both v4 and legacy labels are present. Treated
	// as V4 for any opt-in decision (per PRD constraint that webhooks
	// match only on v4) but flagged in the report so operators can see
	// migration progress: "23 still both, 27 v4-only, 0 legacy-only".
	LabelSourceBoth LabelSource = "both"
)

// AllLabelSources returns every defined LabelSource, sorted for
// deterministic iteration.
func AllLabelSources() []LabelSource {
	return []LabelSource{
		LabelSourceBoth,
		LabelSourceLegacy,
		LabelSourceNone,
		LabelSourceV4,
	}
}

// ExpectedState is what the v4 reconciler thinks should exist for the PVC.
// Populated from internal/v4/naming.Compute() + internal/v4/decision
// output by the V4AuditReconciler (Patch 3). All fields are values, not
// pointers — zero-value means "not computed" (only valid for
// not-opted-in / exempt PVCs).
type ExpectedState struct {
	RSName           string `json:"rs_name,omitempty"`
	RDName           string `json:"rd_name,omitempty"`
	RepositorySecret string `json:"repository_secret,omitempty"`
	KopiaUsername    string `json:"kopia_username,omitempty"`
	KopiaHostname    string `json:"kopia_hostname,omitempty"`
	BackupIdentity   string `json:"backup_identity,omitempty"`
}

// CurrentState captures what the audit reconciler observed in the cluster
// for this PVC's expected resources. Empty fields mean "not present" —
// the booleans disambiguate "present but empty value" from "not present".
type CurrentState struct {
	RSPresent    bool   `json:"rs_present"`
	RSName       string `json:"rs_name,omitempty"`
	RSManagedBy  string `json:"rs_managed_by,omitempty"`
	RSRepository string `json:"rs_repository,omitempty"`
	RSSourcePVC  string `json:"rs_source_pvc,omitempty"`
	// RSSchedule is the live spec.trigger.schedule on the observed RS.
	// Captured since v4.0.2 so the planner's schedule-drift comparison
	// has real input (the 2026-06-09 review found tier changes were
	// silently ignored because this was never read). Additive /audit
	// JSON field.
	RSSchedule string `json:"rs_schedule,omitempty"`

	RDPresent    bool   `json:"rd_present"`
	RDName       string `json:"rd_name,omitempty"`
	RDManagedBy  string `json:"rd_managed_by,omitempty"`
	RDRepository string `json:"rd_repository,omitempty"`
}

// PlannedOpSummary is the audit-surfaced shape of a single planner operation.
// Carries enough identifying information to prove (in /audit output and in
// the cutover runbook) that the planner only ever targets VolSync RS/RD,
// without embedding the full unstructured resource body. Kind matches
// planner.OpKind ("create" | "update" | "delete"); GVK is the canonical
// "group/version/Kind" string for the targeted resource.
type PlannedOpSummary struct {
	Kind      string `json:"kind"`
	GVK       string `json:"gvk"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// ExecutionOpOutcome is the /audit-friendly per-op record produced by the
// bounded executor (Patch 6.7). Mirrors executor.OpOutcome but strips the
// raw Go error — apiserver error details are emitted to the reconciler's
// structured log instead, so /audit consumers see a stable JSON shape
// without opaque error strings leaking into the report.
//
// Status values come straight from executor.OpStatus: "skipped" (audit
// mode short-circuit), "succeeded", "refused", "failed". Reason is a
// short stable code: "forbidden-kind", "exists", "not-owned", "absent",
// "create-failed", "update-failed", "delete-failed", "get-failed",
// "mode=audit", and similar.
type ExecutionOpOutcome struct {
	Kind      string `json:"kind"`
	GVK       string `json:"gvk"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	Status    string `json:"status"`
	Reason    string `json:"reason,omitempty"`
}

// ExecutionResultSummary is the executor's verdict for a single reconcile
// pass, attached to ParityEntry.ExecutionResult only when the planner
// actually emitted ops. already-matches / skipped-exempt / skipped-not-
// opted-in / write-gate-missing entries leave ExecutionResult nil so the
// /audit response stays skimmable for the bulk of the cluster's PVCs.
//
// In audit mode every op shows up under Counts.Skipped (executor short-
// circuit, no apiserver writes). In permissive+ modes Counts reflects
// the real apiserver verdicts produced by Execute.
type ExecutionResultSummary struct {
	Counts   executor.Counts      `json:"counts"`
	Outcomes []ExecutionOpOutcome `json:"outcomes,omitempty"`
}

// ParityEntry is one row in the audit report — the desired-vs-current
// view for a single PVC at a single point in time.
type ParityEntry struct {
	Namespace       string                  `json:"namespace"`
	PVC             string                  `json:"pvc"`
	Mode            string                  `json:"mode"`
	Tier            string                  `json:"tier"`
	LabelSource     LabelSource             `json:"label_source"`
	BackupIdentity  string                  `json:"backup_identity,omitempty"`
	Expected        ExpectedState           `json:"expected,omitzero"`
	Current         CurrentState            `json:"current,omitzero"`
	Owner           OwnerClassification     `json:"owner_classification"`
	Action          ActionKind              `json:"action"`
	Blockers        []string                `json:"blockers,omitempty"`
	Notes           []string                `json:"notes,omitempty"`
	PlannedOps      []PlannedOpSummary      `json:"planned_ops,omitempty"`
	ExecutionResult *ExecutionResultSummary `json:"execution_result,omitempty"`
	ReasonCode      string                  `json:"reason_code,omitempty"`
	EvaluatedAt     time.Time               `json:"evaluated_at"`

	// AgeSeconds and Stale are NOT stored — they are computed by
	// Snapshot() at read time as (GeneratedAt - EvaluatedAt). They make
	// the freshness of each row explicit so /audit can never silently
	// look fresh while the underlying verdict is hours old (the
	// 2026-05-28 nginx-example/storage incident: /audit kept reporting
	// "inline-argo / already-matches" for ~15h after the RS/RD were
	// pruned, because GeneratedAt was stamped now() on every request
	// while the entry itself was never re-evaluated). AgeSeconds is
	// always populated; Stale is true only when the Store has a non-zero
	// MaxAge and the entry's age exceeds it.
	AgeSeconds int64 `json:"age_seconds"`
	Stale      bool  `json:"stale"`
}

// Key returns the stable map key used by the Store and by the /audit
// endpoint's per-PVC routing.
func (e ParityEntry) Key() string {
	return e.Namespace + "/" + e.PVC
}

// ParityReport is the aggregated cluster-wide view returned by /audit.
// Always reconstructed via Store.Snapshot() so the JSON timestamp + the
// entries form a consistent moment-in-time view.
type ParityReport struct {
	GeneratedAt       time.Time     `json:"generated_at"`
	OperatorMode      string        `json:"operator_mode"`
	NamingStrategy    string        `json:"naming_strategy"`
	DefaultRepoSecret string        `json:"default_repo_secret"`
	Summary           ReportSummary `json:"summary"`
	Entries           []ParityEntry `json:"entries"`
}

// ReportSummary holds the aggregate counts shown at the top of /audit.
// Counts are by ActionKind, OwnerClassification, and LabelSource so an
// operator can see at a glance "how many PVCs in each bucket."
type ReportSummary struct {
	TotalPVCs int                         `json:"total_pvcs"`
	ByAction  map[ActionKind]int          `json:"by_action"`
	ByOwner   map[OwnerClassification]int `json:"by_owner_classification"`
	BySource  map[LabelSource]int         `json:"by_label_source"`

	// EntriesStale counts entries whose age exceeds the Store's MaxAge
	// (0 when MaxAge is unset). OldestEvaluatedAt is the earliest
	// EvaluatedAt across all entries (zero when there are no entries) —
	// an at-a-glance "is any verdict going stale?" signal for operators
	// and monitoring. Both added in rc7 after the nginx-example incident.
	EntriesStale      int       `json:"entries_stale"`
	OldestEvaluatedAt time.Time `json:"oldest_evaluated_at,omitzero"`
}

// Store is the in-memory parity registry. Written to by the
// V4AuditReconciler (Patch 3) on every Reconcile pass; read by the
// /audit HTTP handler (Patch 4) via Snapshot(). Safe for concurrent
// use.
//
// The Store does NOT persist across restarts — each pod restart begins
// with an empty store and the reconciler refills it as it walks PVCs.
// This is intentional: stale parity data after a crash is worse than
// missing data, and re-walking 50-ish PVCs is sub-second on this
// cluster.
type Store struct {
	mu      sync.RWMutex
	entries map[string]ParityEntry

	// Metadata included in every Snapshot(); set at construction.
	operatorMode      string
	namingStrategy    string
	defaultRepoSecret string

	// maxAge, when > 0, is the threshold beyond which a Snapshot() marks
	// an entry Stale (age = GeneratedAt - EvaluatedAt). Zero disables the
	// stale flag (AgeSeconds is still computed). Set via SetMaxAge; the
	// operator binary configures it from runtime config.
	maxAge time.Duration

	// now is injected for deterministic tests. Defaults to time.Now.
	now func() time.Time
}

// SetMaxAge sets the staleness threshold used by Snapshot(). Safe to call
// once at startup before the reconciler begins writing. A non-positive
// value disables the Stale flag (per-entry AgeSeconds is still reported).
func (s *Store) SetMaxAge(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxAge = d
}

// NewStore constructs an empty Store. The three metadata strings appear
// verbatim in every Snapshot() output.
func NewStore(operatorMode, namingStrategy, defaultRepoSecret string) *Store {
	return &Store{
		entries:           make(map[string]ParityEntry),
		operatorMode:      operatorMode,
		namingStrategy:    namingStrategy,
		defaultRepoSecret: defaultRepoSecret,
		now:               time.Now,
	}
}

// Set inserts or replaces the entry for a (namespace, pvc) pair.
// EvaluatedAt is set to now() if the caller left it zero; that lets
// reconciler callers omit the field and tests inject a fixed value.
func (s *Store) Set(e ParityEntry) {
	if e.EvaluatedAt.IsZero() {
		e.EvaluatedAt = s.now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[e.Key()] = e
}

// Get returns the entry for (namespace, pvc), or zero value + false.
func (s *Store) Get(namespace, pvc string) (ParityEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[namespace+"/"+pvc]
	return e, ok
}

// Delete removes the entry for (namespace, pvc). No-op if absent.
func (s *Store) Delete(namespace, pvc string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, namespace+"/"+pvc)
}

// Len returns the current number of entries.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// Snapshot returns an immutable point-in-time copy of the store as a
// fully-populated ParityReport. The caller can freely modify the
// returned report; nothing leaks back into the store.
//
// Entries are sorted by (namespace, pvc) for deterministic JSON.
// Summary counts include zero buckets for every defined ActionKind,
// OwnerClassification, and LabelSource so consumers always see the
// full taxonomy.
func (s *Store) Snapshot() ParityReport {
	s.mu.RLock()
	entries := make([]ParityEntry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e)
	}
	maxAge := s.maxAge
	generatedAt := s.now()
	s.mu.RUnlock()

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Namespace != entries[j].Namespace {
			return entries[i].Namespace < entries[j].Namespace
		}
		return entries[i].PVC < entries[j].PVC
	})

	summary := ReportSummary{
		TotalPVCs: len(entries),
		ByAction:  zeroActionMap(),
		ByOwner:   zeroOwnerMap(),
		BySource:  zeroSourceMap(),
	}
	for i := range entries {
		e := &entries[i]
		summary.ByAction[e.Action]++
		summary.ByOwner[e.Owner]++
		summary.BySource[e.LabelSource]++

		// Compute per-entry freshness against the same generatedAt the
		// report header carries, so age is self-consistent. EvaluatedAt
		// is always set by Store.Set, so it is never zero here in
		// practice; guard anyway so a hand-built entry can't report a
		// nonsense negative/huge age.
		if !e.EvaluatedAt.IsZero() {
			age := generatedAt.Sub(e.EvaluatedAt)
			if age < 0 {
				age = 0
			}
			e.AgeSeconds = int64(age.Seconds())
			if maxAge > 0 && age > maxAge {
				e.Stale = true
				summary.EntriesStale++
			}
			if summary.OldestEvaluatedAt.IsZero() || e.EvaluatedAt.Before(summary.OldestEvaluatedAt) {
				summary.OldestEvaluatedAt = e.EvaluatedAt
			}
		}
	}

	return ParityReport{
		GeneratedAt:       generatedAt,
		OperatorMode:      s.operatorMode,
		NamingStrategy:    s.namingStrategy,
		DefaultRepoSecret: s.defaultRepoSecret,
		Summary:           summary,
		Entries:           entries,
	}
}

func zeroActionMap() map[ActionKind]int {
	out := make(map[ActionKind]int, 10)
	for _, k := range AllActionKinds() {
		out[k] = 0
	}
	return out
}

func zeroOwnerMap() map[OwnerClassification]int {
	out := make(map[OwnerClassification]int, 5)
	for _, k := range AllOwnerClassifications() {
		out[k] = 0
	}
	return out
}

func zeroSourceMap() map[LabelSource]int {
	out := make(map[LabelSource]int, 4)
	for _, k := range AllLabelSources() {
		out[k] = 0
	}
	return out
}
