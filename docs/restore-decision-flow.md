# PVC Restore Decision Flow

This service answers one admission-time question:

> Should this PVC restore from backup, start fresh, or wait because backup truth is unknown?

The answer is intentionally tri-state. `exists: false` no longer means "safe" by itself. A safe fresh create must also be `authoritative: true` with `decision: fresh`.

## One Screen Model

```mermaid
flowchart LR
    A[PVC create request] --> B[Kyverno calls pvc-plumber]
    B --> C{Can pvc-plumber prove backup truth?}

    C -->|Backup exists| D[decision: restore<br/>authoritative: true<br/>HTTP 200]
    C -->|No backup exists| E[decision: fresh<br/>authoritative: true<br/>HTTP 200]
    C -->|Backend error, timeout, parse error| F[decision: unknown<br/>authoritative: false<br/>HTTP 503]

    D --> G[Kyverno adds dataSourceRef]
    G --> H[VolSync populates PVC]
    H --> I[App starts with restored data]

    E --> J[Kyverno leaves PVC unchanged]
    J --> K[StorageClass creates empty volume]
    K --> L[New app starts fresh]

    F --> M[Kyverno denies admission]
    M --> N[ArgoCD retries after backend is fixed]

    classDef restore fill:#d9fbe5,stroke:#16803c,color:#0b3d1b;
    classDef fresh fill:#d9ecff,stroke:#1d5fa7,color:#0b2f57;
    classDef stop fill:#ffe1df,stroke:#b42318,color:#5f130d;
    class D,G,H,I restore;
    class E,J,K,L fresh;
    class F,M,N stop;
```

## Swimlane Flow

```mermaid
sequenceDiagram
    participant Git as Git / ArgoCD
    participant API as Kubernetes API
    participant Kyverno as Kyverno admission
    participant Plumber as pvc-plumber
    participant Kopia as Kopia repository
    participant VolSync as VolSync
    participant App as App pod

    Git->>API: Apply backup-labeled PVC
    API->>Kyverno: Admission review
    Kyverno->>Plumber: GET /exists/{namespace}/{pvc}
    Plumber->>Kopia: Query backup source

    alt Backup exists
        Kopia-->>Plumber: snapshots found
        Plumber-->>Kyverno: 200 decision=restore authoritative=true
        Kyverno-->>API: mutate PVC with dataSourceRef
        API->>VolSync: PVC waits for population
        VolSync-->>API: PVC bound after restore
        API-->>App: Pod can start
    else No backup exists
        Kopia-->>Plumber: empty snapshot list
        Plumber-->>Kyverno: 200 decision=fresh authoritative=true
        Kyverno-->>API: allow PVC unchanged
        API-->>App: Pod starts on new empty volume
    else Unknown
        Kopia-->>Plumber: error, timeout, or invalid response
        Plumber-->>Kyverno: 503 decision=unknown authoritative=false
        Kyverno-->>API: deny PVC creation
        API-->>Git: ArgoCD retries later
    end
```

## API Contract

| Decision | HTTP | `exists` | `authoritative` | Admission behavior |
|---|---:|---:|---:|---|
| `restore` | 200 | `true` | `true` | Add `dataSourceRef` and restore with VolSync |
| `fresh` | 200 | `false` | `true` | Create a normal empty PVC |
| `unknown` | 503 | `false` | `false` | Deny PVC creation and retry later |

## Example Responses

Restore:

```json
{
  "exists": true,
  "decision": "restore",
  "authoritative": true,
  "namespace": "karakeep",
  "pvc": "data-pvc",
  "backend": "kopia-fs",
  "source": "data-pvc-backup@karakeep:/data"
}
```

Fresh:

```json
{
  "exists": false,
  "decision": "fresh",
  "authoritative": true,
  "namespace": "new-app",
  "pvc": "data",
  "backend": "kopia-fs",
  "source": "data-backup@new-app:/data"
}
```

Unknown:

```json
{
  "exists": false,
  "decision": "unknown",
  "authoritative": false,
  "namespace": "paperless-ngx",
  "pvc": "media",
  "backend": "kopia-fs",
  "source": "media-backup@paperless-ngx:/data",
  "error": "failed to list snapshots: exit status 1"
}
```

## Operator Signals

| Signal | Meaning |
|---|---|
| `pvc_plumber_backup_check_total{decision="restore"}` | PVCs that found backups |
| `pvc_plumber_backup_check_total{decision="fresh"}` | PVCs that were proven new |
| `pvc_plumber_backup_check_total{decision="unknown"}` | PVCs that were blocked for safety |
| `pvc_plumber_requests_errors_total` | Backend or request errors |
| HTTP 503 from `/exists` | Kyverno should deny PVC creation |

