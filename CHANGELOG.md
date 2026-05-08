# Changelog

All notable changes to **pvc-plumber** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Major-version policy.** Major versions in pvc-plumber land when the
> deployment surface changes — what RBAC the operator needs, which webhooks
> register, which controllers must be installed in the cluster ahead of it.
> The HTTP `/exists` API has stayed source-compatible across every release
> since `1.0.0` and will continue to do so.

---

## [Unreleased]

## [2.1.1] — 2026-05-07

### Fixed

- **Container image now actually contains the v2 operator binary.** `v2.1.0`
  shipped with `Dockerfile` line 19 still pointing at `./cmd/pvc-plumber`
  (the v1 legacy HTTP-only entrypoint) instead of `./cmd/operator` (the
  controller-runtime entrypoint that runs the manager + webhook server).
  Effect on consuming clusters: the deployment looked healthy — the kopia
  cache pre-warm loop served `/exists` on `:8080` and the pod stayed
  `1/1 Ready` — but nothing listened on `:9443`, so the registered
  admission webhooks (`failurePolicy: Fail`) denied every new
  backup-labeled PVC creation with `connection refused`, and the
  reconciler never ran (zero `ExternalSecret` objects were created).
  Existing `ReplicationSource` / `ReplicationDestination` resources kept
  operating on autopilot via VolSync's own controllers, masking the
  regression in routine ops. Same path fix applied to `Dockerfile.debug`,
  `Makefile`, and `.github/workflows/build.yaml`. Added `EXPOSE 9443` to
  both Dockerfiles for documentation. Added inline build comment naming
  the `cmd/operator` choice as canonical.
- **Detection guidance.** `kubectl exec deploy/pvc-plumber -- netstat -tlnp`
  on a correctly-built operator pod shows BOTH `:8080` (HTTP API +
  Prometheus metrics + `/healthz`/`/readyz`) AND `:9443` (TLS webhook
  server) when `OPERATOR_MODE=true`. A v2.1.0 pod will only show `:8080`.
  `strings /pvc-plumber | grep controller-runtime` is non-empty on a
  correctly-built binary; on the v2.1.0 binary it returns nothing.

## [2.1.0] — 2026-05-07

Tracked in the `v2.1-cheap-wins` branch (PR #4).

> ⚠️ The published `:2.1.0` container image is broken — it ships the v1
> legacy binary instead of the operator binary. Use `:2.1.1` (or later)
> for any cluster that has the v2 webhook configurations registered.
> Details: see the `[2.1.1]` Fixed entry above.

### Changed

- **SHA256-based backup schedule** replaces the length-mod-60 formula for
  spreading `ReplicationSource` cron triggers across the minute field. The
  previous `len(ns + "-" + pvc) % 60` clustered PVCs with similar-length names
  on the same minute (e.g. every `data-pvc` in every namespace fired at the
  same minute mark, hammering the Kopia repository at once). Schedule is now
  `sha256(ns + "/" + pvc)[:4]` interpreted as big-endian uint32, modulo 60 —
  uniform distribution regardless of name length.
  - **Non-breaking.** Existing `ReplicationSource` objects keep their
    length-mod-derived minutes because reconciliation is `Get-or-Create` (no
    drift correction). Only newly-created backup-labeled PVCs get the new
    schedule. Operators can manually delete an RS to force re-creation if
    they want their PVC to migrate to the SHA256-derived minute, but there's
    no operational reason to.
- **Defense-in-depth ordering fix in admission webhooks.** The
  system-namespace exclusion check now runs *before* the backup-label /
  backup-exempt checks in all three webhook handlers, so a PVC accidentally
  carrying `backup: hourly` in `kube-system` is admitted unchanged rather
  than going down the (no-op but logged) backup-exempt path.

### Added

- **`backup-exempt: "true"` label opt-out contract.** PVCs intentionally
  excluded from backup automation can now declare so explicitly:
  - Label `backup-exempt: "true"` on the PVC.
  - Required annotation `storage.vanillax.dev/backup-exempt-reason` with one
    of: `cache`, `scratch`, `external-source`, `media-on-nas`,
    `database-native`, `test`. The validating webhook denies exempt PVCs
    that lack a reason from the allow-list — silent exemption is the actual
    foot-gun the contract exists to prevent.
  - Reconciler treats exempt PVCs the same as label-removed: any existing
    managed-by `ExternalSecret` / `ReplicationSource` / `ReplicationDestination`
    children are reaped on the next reconcile.
- **First-class orphan reaping.** The reconciler's `cleanup()` runs on PVC
  delete, label removal, label change to `backup-exempt`, or move into a
  system namespace. Replaces the previous out-of-band orphan-reaper CronJob
  pattern (which lived in the consuming cluster's manifests, not in this
  repo, and had to be maintained separately).

---

## [2.0.0] — 2026-05-07

> 🚨 **THIS IS A MAJOR BREAKING RELEASE.**
> Before bumping from any `1.x` tag, read [`MIGRATION-v1-to-v2.md`](MIGRATION-v1-to-v2.md).
> The deployment surface has changed substantially — new cluster-level
> dependencies, new RBAC, new admission webhooks with `failurePolicy: Fail`.
> Image: `ghcr.io/mitchross/pvc-plumber:2.0.0`.

### Changed (BREAKING)

- 🚨 **pvc-plumber is now a Kubernetes operator.** The binary embeds a
  `controller-runtime` manager with a PVC reconciler and three admission
  webhook endpoints. The legacy stateless HTTP `/exists` server is preserved
  in the same binary, gated by the `OPERATOR_MODE` env var, so a pure HTTP
  drop-in deployment is still possible (see `OPERATOR_MODE=false` below).
- 🚨 **Admission webhooks register with `failurePolicy: Fail`.** The
  `validate-pvc` webhook denies backup-labeled PVC creation cluster-wide if
  the operator pod is unreachable. **Misconfigured namespace exclusions are
  the canonical way to deadlock a cluster** — every controller that creates
  PVCs at startup (`cert-manager`, `external-secrets`, `longhorn`, `argocd`,
  `kyverno`, `snapshot-controller`, …) must be in the
  `namespaceSelector.NotIn` list. The operator ships a 9-entry default; the
  `MutatingWebhookConfiguration` and `ValidatingWebhookConfiguration` must
  carry the same list.
- 🚨 **Required cluster dependencies before bump:**
  - `cert-manager` (webhook serving certificate via a `Certificate` resource).
  - External Secrets Operator (ESO) with a `ClusterSecretStore` named
    `1password`. The name is hardcoded in this release; configurable in v3.
    Backend can be 1Password Connect, Vault, AWS Secrets Manager, etc. — only
    the `ClusterSecretStore` name matters.
  - A `Secret` reachable through that store, with a property/key path
    `rustfs` → `kopia_password`. Hardcoded in this release; configurable in v3.
- 🚨 **Required RBAC.** Hand-managed in
  `infrastructure/controllers/pvc-plumber/rbac.yaml` in the consuming
  GitOps repo — see `MIGRATION-v1-to-v2.md` § "ClusterRole permissions" for
  the canonical rule set. Missing any of the leases / events rules causes
  the manager to fail to start.
- 🚨 **Recommended deployment is `replicas: 2` with leader election** (lock
  name `pvc-plumber.mitchross.github.io`). Single-replica works but loses
  HA; the webhook still serves from the non-leader.
- 🚨 **Removed:** the consuming cluster's Kyverno `generate` policies for
  `ExternalSecret`, `ReplicationSource`, `ReplicationDestination` are
  obsolete and must be deleted as part of the cutover. The reconciler now
  owns those resources.
- 🚨 **Removed:** the consuming cluster's bash `orphan-reaper` CronJob is
  obsolete and must be deleted. The reconciler reaps orphans on PVC events.

### Added

- **PVC reconciler.** Watches `PersistentVolumeClaim` and manages the
  `ExternalSecret` + `ReplicationSource` + `ReplicationDestination` triplet
  for backup-labeled PVCs. Idempotent `Get-or-Create`, no drift
  reconciliation (operators can hand-tune children without the controller
  stomping on them).
- **`mutate-v1-pvc` webhook.** Injects `spec.dataSourceRef` pointing at
  `<pvc>-backup` `ReplicationDestination` when `pvc-plumber` reports an
  authoritative `restore` decision and the PVC has no operator-supplied
  data source. Fail-OPEN on Kopia errors — the validating webhook is the
  fail-closed gate.
- **`validate-v1-pvc` webhook.** Denies backup-labeled PVC creation under
  three conditions: (1) Kopia check returns `unknown` / non-authoritative /
  errored, (2) `decision=restore` but `dataSourceRef` is missing or
  malformed (belt-and-suspenders cross-check against the mutator), (3)
  `volsync.backup/skip-restore=true` without a non-empty
  `volsync.backup/skip-restore-reason` annotation.
- **`mutate-batch-v1-job` webhook.** Injects the shared NFS Kopia
  repository (volume + per-container `/repository` mount) into VolSync
  mover Jobs (`app.kubernetes.io/created-by=volsync`). Replaces the
  consuming cluster's previous `volsync-nfs-inject` Kyverno policy.
  Fail-IGNORE — webhook outage degrades backups but doesn't wedge the
  cluster.
- **`OPERATOR_MODE=true|false` feature flag.** When `false` (or unset), the
  binary runs HTTP-only and behaves as a drop-in replacement for `1.x`.
  When `true`, the controller-runtime manager and webhooks run alongside
  the HTTP server in the same process, sharing one Kopia connection and
  one cache instance.
- **`SYSTEM_NAMESPACES` env var (CSV, additive).** Always seeded with the
  9-entry deadlock-prevention default; env entries are added on top, never
  replace. See [v3 spec § 3.4](MIGRATION-v1-to-v2.md#known-v2-quirks-fixed-in-v3).
- **`NFS_SERVER` and `NFS_PATH` env vars.** Coordinates of the shared Kopia
  repository injected by the Job mutating webhook. Defaults
  (`192.168.10.133` and `/mnt/BigTank/k8s/volsync-kopia-nfs`) match the
  reference cluster; override per deployment.
- **Leader election.** `coordination.k8s.io/leases` resource lock named
  `pvc-plumber.mitchross.github.io`.

### Preserved (unchanged from `1.7.0`)

- The `internal/kopia` Kopia client logic — same connect/listAll/check
  flow, same authoritative-vs-unknown semantics, same `/exists` JSON shape.
- The `/exists/{namespace}/{pvc-name}`, `/healthz`, `/readyz`, `/metrics`
  HTTP endpoints. Identical wire contract; existing v1.x callers (Kyverno
  `apiCall` rules, cluster-internal HTTP probes) continue to work without
  changes when `OPERATOR_MODE=false` or when the operator runs in dual
  mode.
- The `BACKEND_TYPE`, `S3_*`, `KOPIA_*`, `CACHE_TTL`, `RE_WARM_INTERVAL`,
  `LOG_LEVEL`, `PORT`, `HTTP_TIMEOUT` env vars.
- The cache pre-warm + periodic re-warm semantics for the Kopia backend.

---

## [1.7.0] — 2026-05-02

Last release of the v1 line. Stateless HTTP service.

### Summary of the v1 line

`pvc-plumber 1.x` is a stateless Go HTTP service that answers
`GET /exists/{namespace}/{pvc-name}` against an S3 bucket or a Kopia
repository mounted at `/repository`. No Kubernetes RBAC, no admission
webhooks, no controller-runtime — the consuming cluster wires the service
into PVC creation through Kyverno `apiCall` validate/mutate policies and
generate policies for the companion `ExternalSecret`, `ReplicationSource`,
`ReplicationDestination` resources.

### Highlights across the v1 line

- `1.7.0` — final v1 release. Companion to the operator rewrite preview.
- `1.6.0` — `/exists` singleflight deduplication; cache re-warm interval
  introduced.
- `1.5.x` — health-check tightening; reconciler-readiness gate fix for
  large NFS repos.
- `1.0.0` – `1.4.0` — early release line. S3 backend first; Kopia FS
  backend added in `1.3.0`.

For per-version detail of the v1 releases, see the GitHub release notes
and tags `v1.0.0` through `v1.7.0`.

---

[Unreleased]: https://github.com/mitchross/pvc-plumber/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/mitchross/pvc-plumber/compare/v1.7.0...v2.0.0
[1.7.0]: https://github.com/mitchross/pvc-plumber/releases/tag/v1.7.0
