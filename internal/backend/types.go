package backend

// CheckResult represents the result of a backup existence check.
type CheckResult struct {
	Exists    bool   `json:"exists"`
	Namespace string `json:"namespace"`
	Pvc       string `json:"pvc"`
	Backend   string `json:"backend"`
	Error     string `json:"error,omitempty"`
}
