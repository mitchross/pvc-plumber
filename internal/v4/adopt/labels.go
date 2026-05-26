package adopt

import (
	"strconv"

	"github.com/mitchross/pvc-plumber/internal/v4/labels"
)

// valTrue is the canonical label value for the two-gate write fuse.
// Extracted as a constant so test golden assertions and runtime emission
// share a single source.
const valTrue = "true"

// computeLabels returns the three v4 gate labels adopt would write on
// the live PVC. Always exactly three entries.
func computeLabels(tier string) map[string]string {
	return map[string]string{
		labels.LabelEnabled:       valTrue,
		labels.LabelTier:          tier,
		labels.LabelManageVolSync: valTrue,
	}
}

// computeAnnotations returns the override annotations adopt would write
// on the live PVC. An annotation is emitted only when the effective
// value (Inputs override merged with Defaults) differs from the
// Defaults value. This keeps live PVC metadata minimal — Defaults are
// implicit, overrides are explicit.
func computeAnnotations(in Inputs) map[string]string {
	out := map[string]string{}

	if in.UID != nil && *in.UID != in.Defaults.UID {
		out[labels.AnnotationUID] = strconv.FormatInt(*in.UID, 10)
	}
	if in.GID != nil && *in.GID != in.Defaults.GID {
		out[labels.AnnotationGID] = strconv.FormatInt(*in.GID, 10)
	}
	if in.FSGroup != nil && *in.FSGroup != in.Defaults.FSGroup {
		out[labels.AnnotationFSGroup] = strconv.FormatInt(*in.FSGroup, 10)
	}
	if in.SnapshotClass != "" && in.SnapshotClass != in.Defaults.SnapshotClass {
		out[labels.AnnotationSnapshotClass] = in.SnapshotClass
	}
	if in.CacheCapacity != "" && in.CacheCapacity != in.Defaults.CacheCapacity {
		out[labels.AnnotationCacheCapacity] = in.CacheCapacity
	}
	if in.StorageClass != "" && in.StorageClass != in.Defaults.StorageClass {
		out[labels.AnnotationStorageClass] = in.StorageClass
	}

	if len(out) == 0 {
		return nil
	}
	return out
}
