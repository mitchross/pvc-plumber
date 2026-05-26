package adopt

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/mitchross/pvc-plumber/internal/v4/builder"
	pvcplumberlabels "github.com/mitchross/pvc-plumber/internal/v4/labels"
)

// Test fixtures.
//
// Naming convention: every constructor takes optional functional opts
// so the table tests can compose minimal-divergence fixtures from a
// canonical baseline. This keeps each table row to a single mutation,
// which makes failures readable.

const (
	testNS         = "nginx-example"
	testPVC        = "storage"
	testStorageCls = "longhorn"
	testCapacity   = "10Gi"
	testRepoSecret = "volsync-kopia-repository"
	testSnapClass  = "longhorn-snapclass"
	testCacheCap   = "2Gi"
	defaultUID     = int64(568)
	defaultGID     = int64(568)
	defaultFSGroup = int64(568)
	karakeepUID    = int64(1001)

	// Test-side string literals extracted to silence goconst on
	// repeated occurrences. Production-side equivalents live next to
	// their consumers (e.g. valTrue in labels.go, nsKubeSystem in
	// validate.go) so the test file doesn't form a back-channel to
	// internal constants.
	tValTrue       = "true"
	tTierDaily     = TierDaily
	tNSKubeSystem  = "kube-system"
	tKindRD        = "ReplicationDestination"
	tManagedByArgo = "argocd"
)

func i64ptr(v int64) *int64 { return &v }

// defaultsBundle is the cluster-wide defaults shape used across tests.
func defaultsBundle() Defaults {
	return Defaults{
		UID:           defaultUID,
		GID:           defaultGID,
		FSGroup:       defaultFSGroup,
		SnapshotClass: testSnapClass,
		CacheCapacity: testCacheCap,
		StorageClass:  testStorageCls,
		RepoSecret:    testRepoSecret,
	}
}

// baseInputs returns the canonical Inputs that should produce
// VerdictSafeToAdopt against the canonical fixtures.
func baseInputs() Inputs {
	return Inputs{
		Namespace: testNS,
		PVCName:   testPVC,
		Tier:      tTierDaily,
		Defaults:  defaultsBundle(),
	}
}

type pvcOpt func(*corev1.PersistentVolumeClaim)

func makePVC(opts ...pvcOpt) *corev1.PersistentVolumeClaim {
	sc := testStorageCls
	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNS,
			Name:      testPVC,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(testCapacity),
				},
			},
			DataSourceRef: &corev1.TypedObjectReference{
				APIGroup: stringPtr("volsync.backube"),
				Kind:     tKindRD,
				Name:     testPVC + "-dst",
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}
	for _, o := range opts {
		o(pvc)
	}
	return pvc
}

func withNamespace(ns string) pvcOpt {
	return func(p *corev1.PersistentVolumeClaim) { p.Namespace = ns }
}

func withPhase(phase corev1.PersistentVolumeClaimPhase) pvcOpt {
	return func(p *corev1.PersistentVolumeClaim) { p.Status.Phase = phase }
}

func withLabels(kv map[string]string) pvcOpt {
	return func(p *corev1.PersistentVolumeClaim) {
		if p.Labels == nil {
			p.Labels = map[string]string{}
		}
		for k, v := range kv {
			p.Labels[k] = v
		}
	}
}

func withAnnotations(kv map[string]string) pvcOpt {
	return func(p *corev1.PersistentVolumeClaim) {
		if p.Annotations == nil {
			p.Annotations = map[string]string{}
		}
		for k, v := range kv {
			p.Annotations[k] = v
		}
	}
}

func withDataSourceRefName(name string) pvcOpt {
	return func(p *corev1.PersistentVolumeClaim) {
		p.Spec.DataSourceRef = &corev1.TypedObjectReference{
			APIGroup: stringPtr("volsync.backube"),
			Kind:     tKindRD,
			Name:     name,
		}
	}
}

func withV4Gates() pvcOpt {
	return withLabels(map[string]string{
		pvcplumberlabels.LabelEnabled:       tValTrue,
		pvcplumberlabels.LabelTier:          tTierDaily,
		pvcplumberlabels.LabelManageVolSync: tValTrue,
	})
}

func makeNamespace(name string, privilegedMovers bool) *corev1.Namespace {
	ns := &corev1.Namespace{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
	if privilegedMovers {
		ns.Labels = map[string]string{
			pvcplumberlabels.NamespacePrivilegedMoversLabel: tValTrue,
		}
	}
	return ns
}

// rsOpt mutates a baseline RS unstructured. Order-independent.
type rsOpt func(*unstructured.Unstructured)

// makeRS returns a canonical inline-Argo-owned RS that, paired with
// makeRD(), produces VerdictSafeToAdopt against baseInputs().
func makeRS(opts ...rsOpt) *unstructured.Unstructured {
	rs := &unstructured.Unstructured{}
	rs.SetGroupVersionKind(volsyncRSGVK)
	rs.SetNamespace(testNS)
	rs.SetName(testPVC)
	rs.SetLabels(map[string]string{
		pvcplumberlabels.LabelManagedByKey: tManagedByArgo,
	})
	rs.Object["spec"] = map[string]interface{}{
		"sourcePVC": testPVC,
		"trigger": map[string]interface{}{
			"schedule": builder.ScheduleFor(testNS, testPVC, pvcplumberlabels.TierDaily),
		},
		"kopia": map[string]interface{}{
			"repository":              testRepoSecret,
			"username":                testPVC,
			"hostname":                testNS,
			"copyMethod":              "Snapshot",
			"storageClassName":        testStorageCls,
			"volumeSnapshotClassName": testSnapClass,
			"cacheCapacity":           testCacheCap,
			"moverSecurityContext": map[string]interface{}{
				"runAsUser":  defaultUID,
				"runAsGroup": defaultGID,
				"fsGroup":    defaultFSGroup,
			},
		},
	}
	for _, o := range opts {
		o(rs)
	}
	return rs
}

func rsWithManagedBy(mb string) rsOpt {
	return func(u *unstructured.Unstructured) {
		l := u.GetLabels()
		if l == nil {
			l = map[string]string{}
		}
		if mb == "" {
			delete(l, pvcplumberlabels.LabelManagedByKey)
		} else {
			l[pvcplumberlabels.LabelManagedByKey] = mb
		}
		u.SetLabels(l)
	}
}

func rsWithRepository(repo string) rsOpt {
	return func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(u.Object, repo, "spec", "kopia", "repository")
	}
}

func rsWithSecurityContext(uid, gid, fsGroup int64) rsOpt {
	return func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(u.Object, uid, "spec", "kopia", "moverSecurityContext", "runAsUser")
		_ = unstructured.SetNestedField(u.Object, gid, "spec", "kopia", "moverSecurityContext", "runAsGroup")
		_ = unstructured.SetNestedField(u.Object, fsGroup, "spec", "kopia", "moverSecurityContext", "fsGroup")
	}
}

func rsWithCopyMethod(m string) rsOpt {
	return func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(u.Object, m, "spec", "kopia", "copyMethod")
	}
}

func rsWithLastSyncTime(t metav1.Time) rsOpt {
	return func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(u.Object, t.UTC().Format("2006-01-02T15:04:05Z"), "status", "lastSyncTime")
	}
}

// makeRD returns a canonical inline-Argo-owned RD paired with makeRS.
func makeRD() *unstructured.Unstructured {
	rd := &unstructured.Unstructured{}
	rd.SetGroupVersionKind(volsyncRDGVK)
	rd.SetNamespace(testNS)
	rd.SetName(testPVC + "-dst")
	rd.SetLabels(map[string]string{
		pvcplumberlabels.LabelManagedByKey: "argocd",
	})
	rd.Object["spec"] = map[string]interface{}{
		"trigger": map[string]interface{}{
			"manual": "restore-once",
		},
		"kopia": map[string]interface{}{
			"repository":              testRepoSecret,
			"username":                testPVC,
			"hostname":                testNS,
			"copyMethod":              "Snapshot",
			"storageClassName":        testStorageCls,
			"volumeSnapshotClassName": testSnapClass,
			"cacheCapacity":           testCacheCap,
		},
	}
	return rd
}

func stringPtr(s string) *string { return &s }

// makeObjects assembles a runtime.Object slice for fake-client seeding.
// Helper for table tests.
func makeObjects(pvc *corev1.PersistentVolumeClaim, ns *corev1.Namespace, rs, rd *unstructured.Unstructured) []runtime.Object {
	out := []runtime.Object{}
	if pvc != nil {
		out = append(out, pvc)
	}
	if ns != nil {
		out = append(out, ns)
	}
	if rs != nil {
		out = append(out, rs)
	}
	if rd != nil {
		out = append(out, rd)
	}
	return out
}
