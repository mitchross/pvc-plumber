# Operator Workflow

This page describes shipped pvc-plumber `v4.0.1` behavior.

## Responsibility Boundary

pvc-plumber owns VolSync `ReplicationSource` and `ReplicationDestination`
wiring. VolSync and Kopia move bytes. The PVC manifest in Git carries the
static `dataSourceRef` used for restore-on-recreate.

## Reconcile Flow

For each PVC:

1. Skip namespaces without `pvc-plumber.io/managed-namespace: "true"`.
2. Skip PVCs marked `backup-exempt: "true"`.
3. Skip PVCs without the opt-in fuses:
   `pvc-plumber.io/enabled: "true"` and
   `pvc-plumber.io/manage-volsync: "true"`.
4. Validate `pvc-plumber.io/tier`.
5. Inspect existing RS/RD ownership.
6. Create, update, delete, or leave the operator-owned RS/RD pair unchanged.
7. Publish the result through `/audit`.

## Ownership Rule

pvc-plumber writes only RS/RD resources labeled
`app.kubernetes.io/managed-by: pvc-plumber`. Historical Argo-owned inline
resources are audit-only and must not be patched by the operator.

## Restore-On-Recreate

The operator does not inject `dataSourceRef`. Git must carry:

```yaml
spec:
  dataSourceRef:
    apiGroup: volsync.backube
    kind: ReplicationDestination
    name: <pvc-name>-dst
```

Without that reference, a recreated PVC comes back empty even if a backup
exists.

## Exclusions

- CNPG uses native Barman/S3.
- Redis is backup-exempt and disposable.
- PostHog is backup-exempt and disposable.

## Related Docs

- [Safety model](safety-model.md)
- [`/audit` API](audit-api.md)
- [v4 shipped vs v5 design-only](v4-vs-v5.md)
