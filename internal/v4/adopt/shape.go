package adopt

import (
	"github.com/mitchross/pvc-plumber/internal/v4/builder"
	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// Public-facing tier name constants. These mirror the labels package's
// canonical vocabulary but are exported separately so callers (CLI, JSON
// schema docs) have a single source for the strings they pass into
// Inputs.Tier.
const (
	TierHourly   = "hourly"
	TierDaily    = "daily"
	TierWeekly   = "weekly"
	TierManual   = "manual"
	TierDisabled = "disabled"
)

// buildExpected renders the ExpectedVolSyncSummary for the effective
// (PVC + Inputs + Defaults) tuple. It calls into builder.BuildRS/BuildRD
// so adopt's expected-shape view is byte-identical to what the
// reconciler would create on takeover. The schedule field reuses
// builder.ScheduleFor directly to avoid round-tripping through an
// unstructured object.
func buildExpected(in Inputs, parsed labels.Spec, pvcCapacity, pvcStorageClass string) ExpectedVolSyncSummary {
	// Compose the labels.Spec the builder consumes. Start from the
	// parsed PVC Spec so legacy fields (BackupIdentity, MinBackupAge,
	// etc.) survive, then layer Inputs-supplied overrides over the
	// override-eligible fields.
	spec := parsed
	if in.UID != nil {
		v := *in.UID
		spec.UID = &v
	} else if parsed.UID == nil {
		v := in.Defaults.UID
		spec.UID = &v
	}
	if in.GID != nil {
		v := *in.GID
		spec.GID = &v
	} else if parsed.GID == nil {
		v := in.Defaults.GID
		spec.GID = &v
	}
	if in.FSGroup != nil {
		v := *in.FSGroup
		spec.FSGroup = &v
	} else if parsed.FSGroup == nil {
		v := in.Defaults.FSGroup
		spec.FSGroup = &v
	}
	if in.SnapshotClass != "" {
		spec.SnapshotClass = in.SnapshotClass
	}
	if in.CacheCapacity != "" {
		spec.CacheCapacity = in.CacheCapacity
	}
	if in.StorageClass != "" {
		spec.StorageClass = in.StorageClass
	}
	// adopt always passes a tier explicitly via Inputs.Tier; the spec
	// field is populated upstream by parseTier. The builder uses
	// spec.Tier for the schedule computation.
	tier, _ := parseTierString(in.Tier)
	spec.Tier = tier

	bin := builder.Inputs{
		Namespace:            in.Namespace,
		PVCName:              in.PVCName,
		PVCCapacity:          pvcCapacity,
		PVCStorageClass:      pvcStorageClass,
		Spec:                 spec,
		NamingStrategy:       in.NamingStrategy,
		DefaultRepoSecret:    in.effectiveRepoSecret(),
		DefaultSnapshotClass: in.Defaults.SnapshotClass,
		DefaultCacheCapacity: in.Defaults.CacheCapacity,
		DefaultStorageClass:  in.Defaults.StorageClass,
		DefaultUID:           in.Defaults.UID,
		DefaultGID:           in.Defaults.GID,
		DefaultFSGroup:       in.Defaults.FSGroup,
	}

	rs := builder.BuildRS(bin)
	identity := naming.IdentityFor(in.Namespace, in.PVCName, spec.BackupIdentity)

	return ExpectedVolSyncSummary{
		RSName:        in.PVCName,
		RDName:        in.PVCName + "-dst",
		RepoSecret:    nestedString(rs.Object, "spec", "kopia", "repository"),
		Username:      identity.Username,
		Hostname:      identity.Hostname,
		CopyMethod:    nestedString(rs.Object, "spec", "kopia", "copyMethod"),
		SnapshotClass: nestedString(rs.Object, "spec", "kopia", "volumeSnapshotClassName"),
		CacheCapacity: nestedString(rs.Object, "spec", "kopia", "cacheCapacity"),
		StorageClass:  nestedString(rs.Object, "spec", "kopia", "storageClassName"),
		UID:           in.effectiveUID(),
		GID:           in.effectiveGID(),
		FSGroup:       in.effectiveFSGroup(),
		Schedule:      builder.ScheduleFor(in.Namespace, in.PVCName, tier),
	}
}

// parseTierString accepts the user-facing tier strings and maps them
// to labels.Tier. The labels package keeps its own parseTier unexported,
// so adopt re-implements the same vocabulary here. Empty string is
// returned as TierUnspecified so the caller can emit BlockerInvalidTier
// uniformly with all other invalid values.
func parseTierString(raw string) (labels.Tier, bool) {
	switch raw {
	case TierHourly:
		return labels.TierHourly, true
	case TierDaily:
		return labels.TierDaily, true
	case TierWeekly:
		return labels.TierWeekly, true
	case TierManual:
		return labels.TierManual, true
	case TierDisabled:
		return labels.TierDisabled, true
	default:
		return labels.TierUnspecified, false
	}
}

// shapeBlockers compares the observed RS shape against the expected
// shape and returns one Blocker per material divergence. The order is
// fixed (repo, copyMethod, UID, GID, FSGroup, snapshot, cache, storage)
// so test golden outputs stay stable.
//
// "Material" means: a field where the operator would write a different
// value on takeover than what is live today. Cosmetic differences
// (label key ordering, annotation presence) are ignored.
//
// UID/GID/FSGroup mismatch is *only* reported when the live value
// differs from the effective value. If the operator passed --uid 1001
// and the live RS has runAsUser: 1001, no blocker fires — the override
// resolved the mismatch.
func shapeBlockers(current CurrentVolSyncSummary, expected ExpectedVolSyncSummary) []Blocker {
	var out []Blocker

	if current.RepoSecret != "" && current.RepoSecret != expected.RepoSecret {
		out = append(out, Blocker{
			Class:  BlockerRepoMismatch,
			Detail: "RS repository " + current.RepoSecret + " != expected " + expected.RepoSecret,
		})
	}
	if current.CopyMethod != "" && current.CopyMethod != expected.CopyMethod {
		out = append(out, Blocker{
			Class:  BlockerCopyMethodMismatch,
			Detail: "RS copyMethod " + current.CopyMethod + " != expected " + expected.CopyMethod,
		})
	}
	if current.UID != nil && *current.UID != expected.UID {
		out = append(out, Blocker{
			Class:          BlockerUIDMismatch,
			Detail:         "RS moverSecurityContext.runAsUser != effective UID; supply --uid to override",
			ResolvableWith: "--uid <observed-value>",
		})
	}
	if current.GID != nil && *current.GID != expected.GID {
		out = append(out, Blocker{
			Class:          BlockerGIDMismatch,
			Detail:         "RS moverSecurityContext.runAsGroup != effective GID; supply --gid to override",
			ResolvableWith: "--gid <observed-value>",
		})
	}
	if current.FSGroup != nil && *current.FSGroup != expected.FSGroup {
		out = append(out, Blocker{
			Class:          BlockerFSGroupMismatch,
			Detail:         "RS moverSecurityContext.fsGroup != effective FSGroup; supply --fs-group to override",
			ResolvableWith: "--fs-group <observed-value>",
		})
	}
	if current.SnapshotClass != "" && expected.SnapshotClass != "" && current.SnapshotClass != expected.SnapshotClass {
		out = append(out, Blocker{
			Class:          BlockerSnapshotClassMismatch,
			Detail:         "RS volumeSnapshotClassName " + current.SnapshotClass + " != expected " + expected.SnapshotClass,
			ResolvableWith: "--snapshot-class " + current.SnapshotClass,
		})
	}
	if current.CacheCapacity != "" && expected.CacheCapacity != "" && current.CacheCapacity != expected.CacheCapacity {
		out = append(out, Blocker{
			Class:          BlockerCacheCapacityMismatch,
			Detail:         "RS cacheCapacity " + current.CacheCapacity + " != expected " + expected.CacheCapacity,
			ResolvableWith: "--cache-capacity " + current.CacheCapacity,
		})
	}
	if current.StorageClass != "" && expected.StorageClass != "" && current.StorageClass != expected.StorageClass {
		out = append(out, Blocker{
			Class:          BlockerStorageClassMismatch,
			Detail:         "RS storageClassName " + current.StorageClass + " != expected " + expected.StorageClass,
			ResolvableWith: "--storage-class " + current.StorageClass,
		})
	}
	return out
}

// shapeMatches is the equality used to decide AlreadyAdopted vs
// SafeToAdopt: an operator-owned matching pair is AlreadyAdopted. We
// reuse shapeBlockers' diff logic to avoid duplicating equality rules.
func shapeMatches(current CurrentVolSyncSummary, expected ExpectedVolSyncSummary) bool {
	return len(shapeBlockers(current, expected)) == 0
}
