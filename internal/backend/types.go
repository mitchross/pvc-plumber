package backend

const (
	DecisionRestore = "restore"
	DecisionFresh   = "fresh"
	DecisionUnknown = "unknown"
)

// Backend type identifiers used in CheckResult.Backend and as the value of
// the BACKEND_TYPE env var that selects the runtime backend implementation.
//
// As of v3.0.0 the kopia backend connects via S3 (RustFS) instead of a local
// filesystem mount. The token name therefore changes from `kopia-fs` →
// `kopia-s3`. This is a breaking rename for anyone who scrapes
// `pvc_plumber_backup_check_total{backend="…"}` metrics or filters logs by
// backend label — see CHANGELOG v3.0.0 for the migration guidance.
const (
	TypeS3      = "s3"
	TypeKopiaS3 = "kopia-s3"
)

// CheckResult represents the result of a backup existence check.
type CheckResult struct {
	Exists        bool   `json:"exists"`
	Decision      string `json:"decision"`
	Authoritative bool   `json:"authoritative"`
	Namespace     string `json:"namespace"`
	Pvc           string `json:"pvc"`
	Backend       string `json:"backend"`
	Source        string `json:"source,omitempty"`
	Error         string `json:"error,omitempty"`
}
