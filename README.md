# pvc-plumber

[![Build and Test](https://github.com/mitchross/pvc-plumber/actions/workflows/build.yaml/badge.svg)](https://github.com/mitchross/pvc-plumber/actions/workflows/build.yaml)
[![Release](https://github.com/mitchross/pvc-plumber/actions/workflows/release.yaml/badge.svg)](https://github.com/mitchross/pvc-plumber/actions/workflows/release.yaml)

pvc-plumber is a Kubernetes controller that owns VolSync
`ReplicationSource` and `ReplicationDestination` wiring for opted-in
application PVCs.

## Current Shipped Behavior

pvc-plumber `v4.0.1` is the shipped and proven release.

- permissive controller
- namespace software gate
- PVC fuse labels
- operator-owned RS/RD resources
- read-only `/audit` endpoint
- no admission webhook
- no Kyverno dependency
- no Prometheus dependency in core

pvc-plumber does not move bytes. VolSync and Kopia move bytes. It does not
generic-migrate CNPG, Redis, or PostHog PVCs: CNPG uses native Barman/S3;
Redis and PostHog are backup-exempt disposable data.

## Future v5

v5 remains design-only. Strict mode, admission webhooks, a backup-truth cache,
source gating, `minBackupAge`, and fail-closed rebuild protection are not
shipped.

## Documentation

- [Operator workflow](docs/operator-workflow.md)
- [Safety model](docs/safety-model.md)
- [`/audit` API](docs/audit-api.md)
- [v4 shipped vs v5 design-only](docs/v4-vs-v5.md)
- [Historical archive](docs/archive/README.md)
- [Talos ArgoCD integration](https://github.com/mitchross/talos-argocd-proxmox/blob/main/docs/talos-argocd-pvc-plumber-integration.md)
