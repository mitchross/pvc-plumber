package builder

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// Test-scope canonical values matching the live talos repo conventions.
const (
	tnsOpenWebUI  = "open-webui"
	tpvcStorage   = "storage"
	tcap10Gi      = "10Gi"
	tscLonghorn   = "longhorn"
	tsnapLonghorn = "longhorn-snapclass"
	tcache2Gi     = "2Gi"
	tshareRepo    = "volsync-kopia-repository"
)

func baseInputs() Inputs {
	return Inputs{
		Namespace:            tnsOpenWebUI,
		PVCName:              tpvcStorage,
		PVCCapacity:          tcap10Gi,
		PVCAccessModes:       []string{accessModeRWO},
		PVCStorageClass:      tscLonghorn,
		Spec:                 labels.Spec{Tier: labels.TierDaily},
		NamingStrategy:       naming.StrategyBareDst,
		DefaultRepoSecret:    tshareRepo,
		DefaultSnapshotClass: tsnapLonghorn,
		DefaultCacheCapacity: tcache2Gi,
		DefaultStorageClass:  tscLonghorn,
		DefaultUID:           568,
		DefaultGID:           568,
		DefaultFSGroup:       568,
	}
}

func TestBuildRS_NameAndNamespace(t *testing.T) {
	rs := BuildRS(baseInputs())
	if rs.GetName() != tpvcStorage {
		t.Errorf("RS name: got %q, want %q (must be bare PVC name, no -backup suffix)", rs.GetName(), tpvcStorage)
	}
	if rs.GetNamespace() != tnsOpenWebUI {
		t.Errorf("RS namespace: got %q, want %q", rs.GetNamespace(), tnsOpenWebUI)
	}
	if rs.GetKind() != "ReplicationSource" {
		t.Errorf("RS kind: got %q, want ReplicationSource", rs.GetKind())
	}
	if rs.GetAPIVersion() != "volsync.backube/v1alpha1" {
		t.Errorf("RS apiVersion: got %q, want volsync.backube/v1alpha1", rs.GetAPIVersion())
	}
}

func TestBuildRD_NameAndNamespace(t *testing.T) {
	rd := BuildRD(baseInputs())
	if rd.GetName() != tpvcStorage+"-dst" {
		t.Errorf("RD name: got %q, want %q-dst (bare-dst convention)", rd.GetName(), tpvcStorage)
	}
	if rd.GetNamespace() != tnsOpenWebUI {
		t.Errorf("RD namespace: got %q, want %q", rd.GetNamespace(), tnsOpenWebUI)
	}
	if rd.GetKind() != "ReplicationDestination" {
		t.Errorf("RD kind: got %q, want ReplicationDestination", rd.GetKind())
	}
}

func TestBuildRS_ManagedByLabel(t *testing.T) {
	rs := BuildRS(baseInputs())
	if got := rs.GetLabels()[labels.LabelManagedByKey]; got != labels.LabelManagedByValue {
		t.Errorf("RS managed-by label: got %q, want %q (only pvc-plumber-owned resources may be patched/deleted by the operator)",
			got, labels.LabelManagedByValue)
	}
}

func TestBuildRD_ManagedByLabel(t *testing.T) {
	rd := BuildRD(baseInputs())
	if got := rd.GetLabels()[labels.LabelManagedByKey]; got != labels.LabelManagedByValue {
		t.Errorf("RD managed-by label: got %q, want %q", got, labels.LabelManagedByValue)
	}
}

func TestBuildRS_SourcePointerLabels(t *testing.T) {
	rs := BuildRS(baseInputs())
	lbls := rs.GetLabels()
	if lbls[labels.LabelSourceNamespace] != tnsOpenWebUI {
		t.Errorf("source-namespace label: got %q, want %q", lbls[labels.LabelSourceNamespace], tnsOpenWebUI)
	}
	if lbls[labels.LabelSourcePVC] != tpvcStorage {
		t.Errorf("source-pvc label: got %q, want %q", lbls[labels.LabelSourcePVC], tpvcStorage)
	}
	if lbls[labels.LabelTierOnChild] != labels.TierDaily.String() {
		t.Errorf("tier-on-child label: got %q, want %q", lbls[labels.LabelTierOnChild], labels.TierDaily.String())
	}
	if lbls[labels.LabelBackupIdentity] != tnsOpenWebUI+"/"+tpvcStorage {
		t.Errorf("backup-identity label: got %q, want %q",
			lbls[labels.LabelBackupIdentity], tnsOpenWebUI+"/"+tpvcStorage)
	}
}

func TestBuildRS_BackupIdentityOverride(t *testing.T) {
	in := baseInputs()
	in.Spec.BackupIdentity = "immich-library"
	rs := BuildRS(in)
	if got := rs.GetLabels()[labels.LabelBackupIdentity]; got != "immich-library" {
		t.Errorf("override identity: got %q, want %q", got, "immich-library")
	}
}

func TestBuildRS_SourcePVCAndSchedule(t *testing.T) {
	rs := BuildRS(baseInputs())
	if got, _, _ := unstructured.NestedString(rs.Object, "spec", "sourcePVC"); got != tpvcStorage {
		t.Errorf("spec.sourcePVC: got %q, want %q", got, tpvcStorage)
	}
	got, _, _ := unstructured.NestedString(rs.Object, "spec", "trigger", "schedule")
	if got == "" {
		t.Error("spec.trigger.schedule must be non-empty for any tier the builder accepts")
	}
}

func TestBuildRS_RepositoryAndIdentity(t *testing.T) {
	rs := BuildRS(baseInputs())
	if got, _, _ := unstructured.NestedString(rs.Object, "spec", "kopia", "repository"); got != tshareRepo {
		t.Errorf("spec.kopia.repository: got %q, want %q (shared repo Secret name only — no per-PVC variant)", got, tshareRepo)
	}
	// Username/hostname follow the existing inline convention:
	// username = PVC name, hostname = namespace.
	if got, _, _ := unstructured.NestedString(rs.Object, "spec", "kopia", "username"); got != tpvcStorage {
		t.Errorf("spec.kopia.username: got %q, want %q", got, tpvcStorage)
	}
	if got, _, _ := unstructured.NestedString(rs.Object, "spec", "kopia", "hostname"); got != tnsOpenWebUI {
		t.Errorf("spec.kopia.hostname: got %q, want %q", got, tnsOpenWebUI)
	}
}

func TestBuildRS_RetainBlock(t *testing.T) {
	rs := BuildRS(baseInputs())
	m, found, err := unstructured.NestedMap(rs.Object, "spec", "kopia", "retain")
	if err != nil || !found {
		t.Fatalf("retain block missing: found=%v err=%v", found, err)
	}
	checks := map[string]int64{"hourly": defaultRetainH, "daily": defaultRetainD, "weekly": defaultRetainW, "monthly": defaultRetainM}
	for k, want := range checks {
		got, ok := m[k].(int64)
		if !ok {
			t.Errorf("retain[%q] missing or wrong type: %v", k, m[k])
			continue
		}
		if got != want {
			t.Errorf("retain[%q]: got %d, want %d", k, got, want)
		}
	}
}

func TestBuildRS_CopyMethodSnapshot(t *testing.T) {
	rs := BuildRS(baseInputs())
	if got, _, _ := unstructured.NestedString(rs.Object, "spec", "kopia", "copyMethod"); got != "Snapshot" {
		t.Errorf("copyMethod: got %q, want Snapshot", got)
	}
}

func TestBuildRD_CapacityFromPVC(t *testing.T) {
	rd := BuildRD(baseInputs())
	if got, _, _ := unstructured.NestedString(rd.Object, "spec", "kopia", "capacity"); got != tcap10Gi {
		t.Errorf("RD capacity: got %q, want %q (MUST equal PVC requests.storage)", got, tcap10Gi)
	}
}

func TestBuildRD_AccessModesFromPVC(t *testing.T) {
	rd := BuildRD(baseInputs())
	am, found, err := unstructured.NestedSlice(rd.Object, "spec", "kopia", "accessModes")
	if err != nil || !found || len(am) != 1 || am[0] != accessModeRWO {
		t.Errorf("RD accessModes: got %v (found=%v err=%v), want [ReadWriteOnce]", am, found, err)
	}
}

func TestBuildRD_SourceIdentityBlock(t *testing.T) {
	rd := BuildRD(baseInputs())
	got, found, err := unstructured.NestedStringMap(rd.Object, "spec", "kopia", "sourceIdentity")
	if err != nil || !found {
		t.Fatalf("sourceIdentity missing: found=%v err=%v", found, err)
	}
	want := map[string]string{
		"sourceName":      tpvcStorage,
		"sourceNamespace": tnsOpenWebUI,
		"sourcePVCName":   tpvcStorage,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("sourceIdentity[%q]: got %q, want %q", k, got[k], v)
		}
	}
}

func TestBuildRD_ManualTriggerOnly(t *testing.T) {
	rd := BuildRD(baseInputs())
	if got, _, _ := unstructured.NestedString(rd.Object, "spec", "trigger", "manual"); got != "restore-once" {
		t.Errorf("RD trigger.manual: got %q, want %q (static — fires only when value changes)", got, "restore-once")
	}
	// RD must NOT carry a schedule — only RS runs on schedule.
	if _, found, _ := unstructured.NestedString(rd.Object, "spec", "trigger", "schedule"); found {
		t.Error("RD must not carry trigger.schedule")
	}
}

func TestBuildRS_MoverSecurityContextDefaults(t *testing.T) {
	rs := BuildRS(baseInputs())
	m, found, err := unstructured.NestedMap(rs.Object, "spec", "kopia", "moverSecurityContext")
	if err != nil || !found {
		t.Fatalf("moverSecurityContext missing: %v %v", found, err)
	}
	for _, k := range []string{"runAsUser", "runAsGroup", "fsGroup"} {
		if v, ok := m[k].(int64); !ok || v != 568 {
			t.Errorf("moverSecurityContext[%q]: got %v, want 568", k, m[k])
		}
	}
}

func TestBuildRS_MoverSecurityContextOverride(t *testing.T) {
	uid := int64(1000)
	gid := int64(1000)
	fsg := int64(1000)
	in := baseInputs()
	in.Spec.UID = &uid
	in.Spec.GID = &gid
	in.Spec.FSGroup = &fsg

	rs := BuildRS(in)
	m, _, _ := unstructured.NestedMap(rs.Object, "spec", "kopia", "moverSecurityContext")
	if v, _ := m["runAsUser"].(int64); v != 1000 {
		t.Errorf("override runAsUser: got %v, want 1000", v)
	}
}

func TestBuildRS_StorageClassOverride(t *testing.T) {
	in := baseInputs()
	in.Spec.StorageClass = "custom-class"
	rs := BuildRS(in)
	if got, _, _ := unstructured.NestedString(rs.Object, "spec", "kopia", "storageClassName"); got != "custom-class" {
		t.Errorf("storageClassName override: got %q, want %q", got, "custom-class")
	}
}

func TestBuildRS_CacheCapacityOverride(t *testing.T) {
	in := baseInputs()
	in.Spec.CacheCapacity = "4Gi"
	rs := BuildRS(in)
	if got, _, _ := unstructured.NestedString(rs.Object, "spec", "kopia", "cacheCapacity"); got != "4Gi" {
		t.Errorf("cacheCapacity override: got %q, want %q", got, "4Gi")
	}
}

// TestBuildRS_NoExternalSecretReference is a structural guard: the
// produced RS must NOT carry any reference to a per-PVC
// volsync-<pvc> ExternalSecret. Phase 6 contract — only the
// cluster-wide volsync-kopia-repository Secret is referenced, by
// name, in spec.kopia.repository.
func TestBuildRS_NoExternalSecretReference(t *testing.T) {
	rs := BuildRS(baseInputs())
	// The repository field is a Secret reference by name. Anything
	// containing the legacy "volsync-<pvc>" naming would be a
	// regression.
	if got, _, _ := unstructured.NestedString(rs.Object, "spec", "kopia", "repository"); got != tshareRepo {
		t.Errorf("repository reference: got %q, want shared %q (no per-PVC ES)", got, tshareRepo)
	}
}

// TestBuildRS_RepoSecretFallbackToNamingDefault confirms that when
// the caller passes an empty DefaultRepoSecret, the builder falls
// back to naming.DefaultRepoSecretName. Avoids a silent regression
// where a caller forgetting to set the field would produce an RS
// with an empty repository field that VolSync would reject.
func TestBuildRS_RepoSecretFallbackToNamingDefault(t *testing.T) {
	in := baseInputs()
	in.DefaultRepoSecret = ""
	rs := BuildRS(in)
	if got, _, _ := unstructured.NestedString(rs.Object, "spec", "kopia", "repository"); got != naming.DefaultRepoSecretName {
		t.Errorf("repository fallback: got %q, want %q", got, naming.DefaultRepoSecretName)
	}
}

// TestBuildRD_DefaultAccessModesWhenPVCMissing: in tests / synthetic
// fixtures the caller might forget to set PVCAccessModes. The
// builder must not produce an empty list (VolSync would reject the
// RD on validation).
func TestBuildRD_DefaultAccessModesWhenPVCMissing(t *testing.T) {
	in := baseInputs()
	in.PVCAccessModes = nil
	rd := BuildRD(in)
	am, _, _ := unstructured.NestedSlice(rd.Object, "spec", "kopia", "accessModes")
	if len(am) != 1 || am[0] != accessModeRWO {
		t.Errorf("default accessModes: got %v, want [ReadWriteOnce]", am)
	}
}

// TestBuildRS_DeterministicSchedule confirms that two calls with the
// same inputs produce the same schedule (no time / rand) — the
// planner relies on this to dry-run-diff without churn.
func TestBuildRS_DeterministicSchedule(t *testing.T) {
	a, _, _ := unstructured.NestedString(BuildRS(baseInputs()).Object, "spec", "trigger", "schedule")
	b, _, _ := unstructured.NestedString(BuildRS(baseInputs()).Object, "spec", "trigger", "schedule")
	if a != b {
		t.Errorf("non-deterministic schedule: %q vs %q", a, b)
	}
}

// TestInt64ToStr is a tiny smoke test for the convenience helper.
func TestInt64ToStr(t *testing.T) {
	if got := int64ToStr(7); got != "7" {
		t.Errorf("int64ToStr(7): got %q, want %q", got, "7")
	}
}
