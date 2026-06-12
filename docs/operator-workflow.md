# Operator Workflow 🧠

How pvc-plumber thinks, per PVC event. It is a permissive reconciler — see
the [safety model](safety-model.md) for what bounds its writes.

## Responsibility boundary

```mermaid
flowchart LR
    PP["🚰 pvc-plumber<br/>owns RS/RD wiring + /audit"]
    VS["🚚 VolSync + Kopia<br/>own the bytes"]
    GIT["📂 Git<br/>owns the PVC manifest<br/>incl. dataSourceRef"]

    GIT -->|PVC events| PP
    PP -->|creates / repairs| VS

    classDef own fill:#dbeafe,stroke:#2563eb,color:#1e3a8a;
    classDef data fill:#dcfce7,stroke:#16a34a,color:#14532d;
    classDef git fill:#fef3c7,stroke:#92400e,color:#451a03;
    class PP own;
    class VS data;
    class GIT git;
```

pvc-plumber never moves data and never mutates your PVC spec. It reconciles
the VolSync objects that *describe* the data movement, and reports truth.

## The reconcile flow

For each PVC event, in order:

```mermaid
flowchart TD
    A[PVC event] --> Q1{"namespace managed?<br/>pvc-plumber.io/managed-namespace=true"}
    Q1 -->|no| S1[["skipped-namespace-not-managed ⚪"]]
    Q1 -->|yes| Q2{"backup-exempt=true?"}
    Q2 -->|yes| S2[["skipped-exempt ⚪"]]
    Q2 -->|no| Q3{"opt-in fuses?<br/>enabled + manage-volsync"}
    Q3 -->|no| S3[["skipped-not-opted-in ⚪"]]
    Q3 -->|yes| Q4{"tier valid?<br/>hourly|daily|weekly|manual"}
    Q4 -->|no| S4[["needs-human-review 🛑"]]
    Q4 -->|yes| OWN{"who owns existing RS/RD?"}
    OWN -->|"managed-by=pvc-plumber"| REC["reconcile to desired<br/>(or already-matches)"]
    OWN -->|"inline / Git-owned"| HANDS[["audit-only — never patch"]]
    OWN -->|"none"| CREATE["create RS + RD"]
    OWN -->|"mixed / partial"| S5[["needs-human-review 🛑"]]
    REC --> AUD[/record verdict in /audit/]
    CREATE --> AUD
    HANDS --> AUD

    classDef skip fill:#fef9c3,stroke:#ca8a04,color:#713f12;
    classDef stop fill:#fee2e2,stroke:#dc2626,color:#7f1d1d;
    classDef act fill:#dbeafe,stroke:#2563eb,color:#1e3a8a;
    class S1,S2,S3,HANDS skip;
    class S4,S5 stop;
    class REC,CREATE,AUD act;
```

## What it creates

| Resource | Name | Purpose |
|---|---|---|
| `ReplicationSource` | `<pvc>` | the backup schedule (minute derived from `hash(ns/pvc)` — no thundering herd) |
| `ReplicationDestination` | `<pvc>-dst` | the restore capability (`trigger.manual: restore-once`) |

Both carry `app.kubernetes.io/managed-by: pvc-plumber`. The operator writes
**only** resources with that label; anything else is audit-only.

## Restore-on-recreate

The operator does **not** inject `dataSourceRef`. Git must carry it:

```yaml
spec:
  dataSourceRef:
    apiGroup: volsync.backube
    kind: ReplicationDestination
    name: <pvc-name>-dst
```

```mermaid
flowchart LR
    DEL[PVC deleted /<br/>cluster rebuilt] --> GIT[GitOps recreates PVC]
    GIT --> DSR{dataSourceRef?}
    DSR -->|yes| POP[VolSync populator<br/>restores from RD latestImage]
    POP --> OK(["PVC Bound — with data ✅"])
    DSR -->|no| EMPTY(["PVC Bound EMPTY ⚠️"])

    classDef good fill:#dcfce7,stroke:#16a34a,color:#14532d;
    classDef bad fill:#fee2e2,stroke:#dc2626,color:#7f1d1d;
    class POP,OK good;
    class EMPTY bad;
```

Without that reference, a recreated PVC comes back empty even if a backup
exists. `/audit` and the reference deployment's CI both watch for the gap.

## Exclusions

- CNPG database PVCs use native Barman/S3 — never generic-migrated.
- Disposable data (caches, brokers, rebuildable analytics) is
  `backup-exempt: "true"` + a mandatory reason annotation.

## Related docs

- [Safety model](safety-model.md)
- [`/audit` API](audit-api.md)
