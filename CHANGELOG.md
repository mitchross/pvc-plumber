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

## [3.1.0] — 2026-05-08

> **Bugfix release on top of v3.0.0.** Resolves the v3.0.0 cutover incident
> earlier today (2026-05-08) where pvc-plumber operator pods crashed with
> `CreateContainerConfigError` / `CrashLoopBackOff` because kubelet could
> not resolve `secretKeyRef` env vars while the `pvc-plumber-kopia` Secret
> was mid-update during ArgoCD sync waves. Also fixes task #30 — the
> reconciler error-loop on PVC names exceeding the 63-byte Kubernetes
> label-value limit. Image: `ghcr.io/mitchross/pvc-plumber:3.1.0`. Auto-
> promotes `:3.1`, `:3`, `:latest` via the existing release workflow.
>
> **No deployment surface changes from v3.0.0.** RBAC, webhook
> registration, and AppProject permissions are identical. The only
> manifest change in the consuming GitOps repo is the operator
> Deployment: drop the three `secretKeyRef` env vars (`KOPIA_PASSWORD`,
> `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) and add a Secret volume
> mount at `/var/secret/pvc-plumber-kopia` so kubelet renders each Secret
> key as a separate file. ESO-rendered Secret data is unchanged.

### Fixed

- **ES-race operator pod startup deadlock.** The v3.0.0 operator read its
  three kopia credentials (`KOPIA_PASSWORD`, `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`) via `secretKeyRef` env vars at pod start. If the
  `pvc-plumber-kopia` Secret was mid-update when the pod scheduled — the
  exact race that fires during ArgoCD sync waves whenever the underlying
  ExternalSecret manifest changes shape — kubelet's container env-var
  resolver failed and the pod entered `CreateContainerConfigError` →
  `CrashLoopBackOff`. With `failurePolicy: Fail` on the PVC validating
  webhook, that crash-loop denied every backup-labeled PVC creation
  cluster-wide. v3.1.0 mounts the Secret as a directory at
  `/var/secret/pvc-plumber-kopia` (kubelet writes each Secret key as a
  separate file) and the kopia client reads creds from disk on every
  subprocess invocation. The pod starts cleanly regardless of Secret render
  state — there's no kubelet-level `secretKeyRef` resolution to fail. If a
  cred file is missing or empty when a kopia call is needed, the client
  returns a typed `ErrCredentialsNotReady`; `Connect()` retries with
  exponential backoff up to a 60-second cap (configurable via
  `KOPIA_CONNECT_TIMEOUT`), and reconciler/admission paths re-queue
  through controller-runtime's normal backoff. A Secret update from ESO
  is observed on the next call without a pod restart.
- **Reconciler crash on PVC names exceeding 63 bytes.** The reconciler
  built a label selector `volsync.backup/pvc=<full-pvc-name>` for orphan
  cleanup, which Kubernetes rejected for label values over the 63-byte
  limit. The `prometheus-stack` namespace's `prometheus-kube-prometheus-
  stack-prometheus-db-prometheus-kube-prometheus-stack-prometheus-0` PVC
  (104 chars) and the alertmanager equivalent (102 chars) tripped this on
  every reconcile, error-looping the manager. Two layers of fix:
  - **Primary**: the system-namespace check now runs at the very TOP of
    `Reconcile()`, before any cleanup/Get/List call. The two prometheus-
    stack PVCs that triggered the original error-loop never reach the
    label-selector code in the first place — `prometheus-stack` is in the
    operator's `SystemNamespaces` exclusion set, and the reconciler short-
    circuits with no work done. Pre-v3.1.0 the system-namespace check ran
    AFTER the on-PVC-not-found cleanup() call, so an over-long PVC name in
    a system namespace still hit the broken selector.
  - **Defense-in-depth**: any PVC name longer than 63 bytes (regardless of
    namespace) is mapped to a deterministic 28-char `pvc-<sha256-prefix>`
    label value via `labelSafePVCRef`. Children are created with the
    hashed value and `cleanup()` selects with the same hash, so the
    selector always validates and uniquely identifies the source PVC.
    Existing children created against short-named PVCs keep their raw
    names — no migration required.

### Changed

- **`/readyz` semantic change (behavior change worth flagging).** Pre-
  v3.1.0 the operator's `/readyz` HTTP handler returned 200 once
  `Connect()` had succeeded at startup; the readiness probe was
  effectively a "did the process start" check. v3.1.0 makes `/readyz`
  invoke `kopia.Client.HealthCheck()`, which now actually executes
  `kopia repository status` (capped at a 5-second timeout). If the kopia
  connection has silently broken — creds rotated out from under us, on-
  disk session expired, S3 endpoint unreachable — the probe fails and the
  pod is marked not-Ready. Kubelet stops routing admission webhook traffic
  to a not-Ready pod, so this gates the `failurePolicy: Fail` PVC webhook
  against a half-broken operator. The deployment.yaml `readinessProbe`
  `timeoutSeconds: 15` already gives plenty of headroom for a cheap
  status call. Operators should expect the operator pod to flap between
  Ready and not-Ready during transient S3 incidents — that's the intended
  signal, not a regression.
- **Internal kopia client API.** `kopia.S3Config` no longer carries the
  three credential strings; they're loaded lazily through a
  `CredentialsSource` interface (`DirCredentialsSource` for v3.1.0+
  Secret-mount deployments, `StaticCredentialsSource` for the legacy v1.x
  HTTP-only deployment shape). `kopia.NewClient(cfg, creds, logger,
  Options{ConnectTimeout: …})` is the new constructor signature. The
  legacy `cmd/pvc-plumber` HTTP-only binary keeps building and runs
  unchanged on v1.x deployments — credentials still come from env vars
  via `StaticCredentialsSource`. Operator deployments use the dir source
  by default.

### Added

- **`KOPIA_CREDENTIALS_PATH` env var** (default
  `/var/secret/pvc-plumber-kopia`). When unset, the operator reads
  credentials from this directory; when explicitly set to empty, it falls
  back to env-var creds (legacy shape) so the v1.x HTTP-only deployment
  pattern keeps working.
- **`KOPIA_CONNECT_TIMEOUT` env var** (default `60s`). Caps the total time
  `Connect()` spends retrying on `ErrCredentialsNotReady` before returning
  an error and letting controller-runtime re-queue.
- **`ErrCredentialsNotReady` typed error.** Distinguishes "creds aren't
  rendered yet, retry" from "kopia repo is broken, fail" so the connect
  retry loop can be conservative without masking real outages.

### Migration guide

For any cluster running v3.0.0:

1. Bump the Deployment image to `:3.1.0`.
2. Drop the three `valueFrom.secretKeyRef` env vars (`KOPIA_PASSWORD`,
   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) from the operator
   container spec.
3. Add a Secret volume + volumeMount:

   ```yaml
   volumes:
     - name: kopia-credentials
       secret:
         secretName: pvc-plumber-kopia
         defaultMode: 0440
   ```

   ```yaml
   volumeMounts:
     - name: kopia-credentials
       mountPath: /var/secret/pvc-plumber-kopia
       readOnly: true
   ```

The ExternalSecret + rendered Secret are unchanged. ESO continues to
populate `KOPIA_PASSWORD`, `AWS_ACCESS_KEY_ID`, and
`AWS_SECRET_ACCESS_KEY` keys; kubelet writes them as separate files
under the mount directory. The mover-Job side (per-PVC `volsync-<pvc>`
Secret + ES) is unchanged — only the OPERATOR's own Secret moves to
the directory-mount pattern. Per-PVC mover Jobs continue to use
`secretKeyRef` env vars; they aren't subject to the same ArgoCD sync
race because the per-PVC ExternalSecrets are operator-rendered, not
Argo-rendered.

### Rollback

Pin the Deployment image to `:3.0.0`, restore the three `secretKeyRef`
env vars, drop the Secret volume + volumeMount. The on-disk credentials
the v3.1.0 build wrote are stateless — there's no data to migrate back.
Be aware that the v3.0.0 ES-race that prompted this release will
reappear on the next sync wave; the recommended rollback target is
`:2.1.1` if `:3.1.0` itself is found to regress something.

---

## [3.0.0] — 2026-05-08

> 🚨 **THIS IS A MAJOR BREAKING RELEASE.** The deployment surface changes
> substantially: the third admission webhook (`mutate-batch-v1-job`) is
> removed entirely, and the Kopia repository backend switches from
> filesystem (NFS-mounted at `/repository`) to S3 (RustFS or any
> S3-compatible store). Image: `ghcr.io/mitchross/pvc-plumber:3.0.0`.
>
> **Why this is breaking, in one paragraph.** The v2.x `JobMutator` injected
> an NFS volume into VolSync mover Jobs at admission time. VolSync's mover
> reconciler computes a "desired" Job spec WITHOUT the injected volume,
> sees the actual Job spec HAS it, tries to update (Jobs are immutable
> after creation), gets an immutable-field error, and falls back to
> `CreateOrUpdateDeleteOnImmutableErr` — which deletes the running Job and
> recreates it. The recreated Job goes through admission again, gets the
> volume injected, drift detected, delete + recreate. Tight loop. The
> 2026-05-08 cluster outage was caused by this race firing for seven
> backup-labeled PVCs simultaneously. The fix: stop injecting volumes at
> admission time, ship S3-backed Kopia instead. The whole class of
> admission-vs-reconciler races disappears because mover Jobs need no
> shared volume.

### Removed (BREAKING)

- 🚨 **`JobMutator` admission webhook deleted.** The third handler at
  `/mutate-batch-v1-job` is gone permanently. The operator no longer
  registers the route, the handler source files (`internal/webhook/job_mutate.go`
  and its test) are deleted, and the consuming cluster's
  `MutatingWebhookConfiguration` should drop the `mutate-job.pvc-plumber.io`
  entry. Cluster operators upgrading from v2.x to v3.0.0 MUST also delete
  the existing `mutate-job.pvc-plumber.io` webhook entry from
  `MutatingWebhookConfiguration` — leaving it in place against the v3
  operator means admission requests for `/mutate-batch-v1-job` will return
  `404 not found`, and with `failurePolicy: Ignore` (its v2 default) those
  admissions will silently succeed without injection. That's safe but
  cosmetically wrong; remove it.
- 🚨 **`NFS_SERVER` and `NFS_PATH` env vars removed** from the operator
  Deployment. They were JobMutator coordinates and have no successor.
  Setting them is harmless (the operator ignores them) but they should
  be removed from the Deployment manifest as part of the migration.
- 🚨 **`KOPIA_REPOSITORY_PATH` env var removed.** The operator no longer
  connects to a filesystem-backed Kopia repo, so there's no path to point
  it at. Replaced by the S3 connection env-var quartet (see Added below).
- 🚨 **`BACKEND_TYPE=kopia-fs` token renamed to `kopia-s3`.** Anything
  scraping Prometheus `pvc_plumber_backup_check_total{backend="…"}` or
  filtering logs by backend label needs the new token. The wire format
  of `/exists/{ns}/{pvc}` JSON also changes its `backend` field
  accordingly (`"backend": "kopia-s3"`).
- 🚨 **The legacy `repository` NFS volume + `/repository` mount on the
  operator Deployment are gone.** The operator pod no longer needs any
  shared volume — kopia talks to RustFS (or any S3 endpoint) over the
  network the same way VolSync mover Jobs do.

### Added

- **S3-backed Kopia repository for the operator's own client.** The
  operator's existence-check Kopia subprocess now invokes
  `kopia repository connect s3 --endpoint --bucket --access-key
  --secret-access-key --password [--disable-tls]`. New env vars:
  `KOPIA_S3_ENDPOINT`, `KOPIA_S3_BUCKET`, `KOPIA_S3_DISABLE_TLS`,
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `KOPIA_PASSWORD`. The
  operator-side credentials are reused 1:1 for the per-PVC kopia-credentials
  Secret the reconciler creates, so secret material has a single source of
  truth.
- **Configurable per-PVC ExternalSecret rendering** (resolves v2 quirks
  #1, #2, #3 from `MIGRATION-v1-to-v2.md` § 5). New env vars on the
  operator Deployment, all defaulted to the reference cluster's 1Password
  Connect layout:
  - `EXTERNAL_SECRETS_STORE_NAME` (default `1password`)
  - `EXTERNAL_SECRETS_VAULT_KEY` (default `rustfs`)
  - `EXTERNAL_SECRETS_KOPIA_PASSWORD_PROPERTY` (default `kopia_password`)
  - `EXTERNAL_SECRETS_S3_ACCESS_KEY_PROPERTY` (default `k8s-admin-access-key`)
  - `EXTERNAL_SECRETS_S3_SECRET_KEY_PROPERTY` (default `k8s-admin-secret-key`)
- **One-time v2 → v3 ExternalSecret migration helper.** The reconciler
  is normally `Get-or-Create` (no drift correction), but v3.0.0 adds a
  single drift-correct path: when an existing `volsync-<pvc>` ES still
  carries the legacy `KOPIA_REPOSITORY: filesystem:///repository` template,
  the reconciler **deletes and recreates** it in the new S3 shape on the
  next reconcile. This is the ONLY drift correction the reconciler
  performs; once every legacy ES has been recycled (one reconcile cycle
  per labeled PVC) the migration is a no-op and stays that way. After the
  recycle, ESO refreshes the rendered Secret with the new env vars
  (KOPIA_REPOSITORY=s3://…, KOPIA_S3_ENDPOINT, KOPIA_S3_BUCKET,
  KOPIA_S3_DISABLE_TLS, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
  KOPIA_PASSWORD); on VolSync mover Jobs' next scheduled run they pick
  up the new Secret values and connect to S3 instead of the legacy NFS
  share.

### Changed

- **Per-PVC `volsync-<pvc>` ExternalSecret schema** carries three
  remoteRefs now (`KOPIA_PASSWORD`, `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`) instead of one, plus four template `data`
  entries (`KOPIA_REPOSITORY=s3://<bucket>`, `KOPIA_S3_ENDPOINT`,
  `KOPIA_S3_BUCKET`, `KOPIA_S3_DISABLE_TLS`). Anyone consuming the
  rendered `volsync-<pvc>` Secret outside of VolSync mover Jobs needs to
  read the new keys. The ES `target.deletionPolicy` is now explicitly
  `Retain` so a deleted ES doesn't drag the rendered Secret down with it
  while a mover Job is mid-flight.
- **Cluster manifest defaults** (in `infrastructure/controllers/pvc-plumber/`
  in the consuming GitOps repo) updated for the S3 deployment shape: NFS
  volume + mount removed, NFS env vars removed, S3 env vars added pulling
  from the operator's own `pvc-plumber-kopia` Secret, the Deployment image
  bumped to `:3.0.0`, the `mutate-job.pvc-plumber.io` webhook entry
  deleted from `webhooks.yaml`, and the operator's own `pvc-plumber-kopia`
  ExternalSecret extended with `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY` mapped from the `rustfs` 1Password item's
  `k8s-admin-access-key` / `k8s-admin-secret-key` properties.

### Migration guide

For any cluster running v2.x, do these three things in one PR:

1. Bump the Deployment image to `:3.0.0`. Drop `NFS_SERVER`, `NFS_PATH`,
   `KOPIA_REPOSITORY_PATH`, `BACKEND_TYPE=kopia-fs`. Add `BACKEND_TYPE=kopia-s3`,
   `KOPIA_S3_ENDPOINT`, `KOPIA_S3_BUCKET`, `KOPIA_S3_DISABLE_TLS`,
   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`. Drop the `repository`
   NFS volume + `/repository` mount.
2. Extend the operator's own `pvc-plumber-kopia` ExternalSecret to pull
   `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the same secret
   store item the kopia password lives in.
3. Delete the `mutate-job.pvc-plumber.io` webhook from your
   `MutatingWebhookConfiguration`. Keep the two PVC webhooks
   (`mutate-pvc.pvc-plumber.io`, `validate-pvc.pvc-plumber.io`) and the
   `validate-pvc-exempt.pvc-plumber.io` entry untouched — those still
   work the same way.

After the operator is healthy on `:3.0.0`, every existing
`volsync-<pvc>` ExternalSecret will be recycled by the migration helper
on the next per-PVC reconcile (typically within one minute of pod
readiness). The rendered `volsync-<pvc>` Secret will contain the new
S3 keys; VolSync's next mover Job run will use them automatically.

There is no kopia repository data migration. The Kopia snapshots stay
where they are — if your repo was on NFS and you point the new operator
at an S3 bucket, the snapshots are not in that bucket. To carry over
existing snapshots, run `kopia repository sync-to s3 --endpoint=…` from
a machine that has the NFS share mounted, BEFORE switching the operator
over. For this cluster, the cutover skipped that step (recent volsync
backups had been broken since 2026-05-08 anyway, and DR drills had
already validated alternate recovery paths).

### Rollback

Pin the Deployment image to `:2.1.1`, restore the NFS volume + mount,
restore the NFS env vars, restore the `BACKEND_TYPE=kopia-fs` env var,
restore `KOPIA_REPOSITORY_PATH=/repository`, re-add the
`mutate-job.pvc-plumber.io` webhook (but with the
`pvc-plumber.io/emergency-disabled: 2026-05-08` objectSelector to keep
it dormant), and accept that backup-labeled PVCs created from this
point on will get the v3 ES shape recycled into the v2 shape only on
PVC recreate (the operator does not currently downgrade-migrate ESes —
that's not on the roadmap because the v2 shape is gone permanently
once you cut over).

---

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

[Unreleased]: https://github.com/mitchross/pvc-plumber/compare/v3.1.0...HEAD
[3.1.0]: https://github.com/mitchross/pvc-plumber/compare/v3.0.0...v3.1.0
[3.0.0]: https://github.com/mitchross/pvc-plumber/compare/v2.1.1...v3.0.0
[2.1.1]: https://github.com/mitchross/pvc-plumber/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/mitchross/pvc-plumber/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/mitchross/pvc-plumber/compare/v1.7.0...v2.0.0
[1.7.0]: https://github.com/mitchross/pvc-plumber/releases/tag/v1.7.0
