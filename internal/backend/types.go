package backend

const (
	DecisionRestore = "restore"
	DecisionFresh   = "fresh"
	DecisionUnknown = "unknown"
)

// Backend type identifiers used in CheckResult.Backend and as the value of
// the BACKEND_TYPE env var that selects the runtime backend implementation.
const (
	TypeS3      = "s3"
	TypeKopiaFS = "kopia-fs"
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
