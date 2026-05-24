package controller

import (
	"github.com/mitchross/pvc-plumber/internal/v4/labels"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
)

// Phase 5 — Patch 2: expected-state computation.
//
// Pure function. Given a PVC's identity and parsed Spec, returns the
// ExpectedState the v4 reconciler would render. No Kubernetes client,
// no I/O, no cluster reads.

// DefaultRepoSecretName re-exports the canonical shared kopia repo
// Secret name from the naming package so callers in this package don't
// need to pull naming in just for the constant. Both should always
// equal "volsync-kopia-repository" — the Secret materialized in every
// privileged-movers namespace by ClusterExternalSecret in the talos repo.
const DefaultRepoSecretName = naming.DefaultRepoSecretName

// ComputeExpected returns the v4-expected RS / RD / repository / kopia
// identity / backup identity for a given (namespace, pvc) under the given
// naming strategy and operator-default repo secret.
//
// Contract (locked by Phase 5 behavior detail #4):
//
//   - RS name             = <pvc>                         (bare)
//   - RD name             = <pvc>-dst                     (bare + "-dst")
//   - Repository secret   = defaultRepoSecret OR
//     DefaultRepoSecretName if defaultRepoSecret == ""
//   - Kopia username      = <pvc>                         (matches inline RS)
//   - Kopia hostname      = <namespace>                   (matches inline RS)
//   - Backup identity     = spec.BackupIdentity OR <namespace>/<pvc>
//
// Explicitly NOT computed:
//   - per-PVC `volsync-<pvc>` ExternalSecret name (the v4 design uses the
//     shared volsync-kopia-repository Secret; no per-PVC ES is generated)
//   - `<pvc>-backup` legacy RS/RD names (the bare-dst strategy is default)
//
// For PVCs that are not opted in (LabelSourceNone), the caller should
// not display the expected state in the report (set ExpectedState to
// zero value via simply not calling ComputeExpected, or call it and
// ignore the result). ComputeExpected itself never gates on opt-in —
// it answers the structural question "if you were to render v4 children
// for this PVC, what would they look like?".
func ComputeExpected(namespace, pvcName string, spec labels.Spec, strategy naming.Strategy, defaultRepoSecret string) ExpectedState {
	names := naming.Compute(strategy, pvcName, defaultRepoSecret)
	identity := naming.IdentityFor(namespace, pvcName, spec.BackupIdentity)

	backupIdentity := spec.BackupIdentity
	if backupIdentity == "" {
		backupIdentity = namespace + "/" + pvcName
	}

	repoSecret := defaultRepoSecret
	if repoSecret == "" {
		repoSecret = DefaultRepoSecretName
	}

	return ExpectedState{
		RSName:           names.RS,
		RDName:           names.RD,
		RepositorySecret: repoSecret,
		KopiaUsername:    identity.Username,
		KopiaHostname:    identity.Hostname,
		BackupIdentity:   backupIdentity,
	}
}
