# Safety Model

This page describes the shipped pvc-plumber `v4.0.1` safety boundary.

## v4 Is Permissive

v4 does not register an admission webhook. If pvc-plumber is unavailable, PVC
creation still works; RS/RD reconciliation pauses until the controller
returns.

Writes are bounded by:

1. Namespace software gate: `pvc-plumber.io/managed-namespace: "true"`.
2. PVC fuse labels: `pvc-plumber.io/enabled: "true"` and
   `pvc-plumber.io/manage-volsync: "true"`.
3. RS/RD-only RBAC.
4. Ownership checks before updating or deleting child resources.

## Explicit Non-Dependencies

- No Kyverno policies, CRDs, or webhooks are required.
- No Prometheus Operator CRDs are required by core.
- No ServiceMonitor or PrometheusRule resource belongs in core bootstrap.

Observability can be added later by the platform that deploys pvc-plumber.

## v5 Is Future Work

Strict mode, admission webhooks, backup-truth caching, source gating,
`minBackupAge`, and fail-closed rebuild protection are design-only. They would
change the availability risk and require a separate failure-matrix review.

## Related Docs

- [Operator workflow](operator-workflow.md)
- [v4 shipped vs v5 design-only](v4-vs-v5.md)
