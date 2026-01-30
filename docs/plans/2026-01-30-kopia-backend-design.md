# Kopia Filesystem Backend Design

## Overview

Add a Kopia filesystem backend to pvc-plumber, enabling backup existence checks against VolSync Kopia repositories stored on NFS.

## Backend Selection

- `BACKEND_TYPE=s3` (default) - existing S3/MinIO backend
- `BACKEND_TYPE=kopia-fs` - new Kopia filesystem backend

Default remains `s3` for backwards compatibility.

## Kopia Integration Approach

**CLI-based**: Shell out to `kopia` binary already present in the container (dual-purpose pod).

**Connection model**:
- Connect once at startup: `kopia repository connect filesystem --path /repository`
- Creates `~/.config/kopia/repository.config` automatically
- Subsequent commands use this config implicitly

**Snapshot check**:
```bash
kopia snapshot list "{pvc}-backup@{namespace}" --json
```
- Returns `[]` if no snapshots (exit 0)
- Returns array of snapshot objects if exists (exit 0)
- Non-zero exit code indicates error

## Package Structure

```
internal/
├── backend/
│   └── types.go          # Shared CheckResult type
├── s3/
│   └── client.go         # S3 backend (refactored)
│   └── client_test.go
├── kopia/
│   └── client.go         # NEW: Kopia filesystem backend
│   └── client_test.go
├── config/
│   └── config.go         # Updated with backend selection
├── handler/
│   └── handler.go        # BackendClient interface (renamed)
```

## Shared Types

`internal/backend/types.go`:
```go
package backend

type CheckResult struct {
    Exists    bool   `json:"exists"`
    Namespace string `json:"namespace"`
    Pvc       string `json:"pvc"`
    Backend   string `json:"backend"`
    Error     string `json:"error,omitempty"`
}
```

## Kopia Client

`internal/kopia/client.go`:
```go
type Client struct {
    repoPath  string
    logger    *slog.Logger
    connected bool
    executor  CommandExecutor  // For testing
}

type CommandExecutor interface {
    Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

func NewClient(repoPath string, logger *slog.Logger) (*Client, error)
func (c *Client) Connect(ctx context.Context) error
func (c *Client) CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult
```

## Configuration

New environment variables:

| Variable | Backend | Required | Default |
|----------|---------|----------|---------|
| `BACKEND_TYPE` | all | no | `s3` |
| `KOPIA_REPOSITORY_PATH` | kopia-fs | yes* | `/repository` |

*Required only when `BACKEND_TYPE=kopia-fs`

Validation:
- If `kopia-fs`, verify path exists at startup with `os.Stat()`
- Unknown backend type returns error

## Handler Changes

Rename interface:
```go
// Before
type S3Client interface {
    CheckBackupExists(ctx context.Context, namespace, pvc string) s3.CheckResult
}

// After
type BackendClient interface {
    CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult
}
```

## Main Application Flow

```go
var backend handler.BackendClient
switch cfg.BackendType {
case "s3":
    logger.Info("initializing s3 backend", "bucket", cfg.S3Bucket)
    client, err := s3.NewClient(...)
    backend = client
case "kopia-fs":
    logger.Info("initializing kopia-fs backend", "path", cfg.KopiaRepositoryPath)
    client, err := kopia.NewClient(cfg.KopiaRepositoryPath, logger)
    if err := client.Connect(ctx); err != nil {
        log.Fatal("failed to connect to kopia repository", "error", err)
    }
    logger.Info("connected to kopia repository")
    backend = client
}
```

## API Response Format

Updated response (both backends):
```json
{"exists": true, "namespace": "karakeep", "pvc": "data-pvc", "backend": "kopia-fs"}
```

Error response:
```json
{"exists": false, "namespace": "foo", "pvc": "bar", "error": "command failed"}
```

Breaking change: `keyCount` field removed (was unused by Kyverno).

## Testing Strategy

**Unit tests** (`internal/kopia/client_test.go`):
- Inject mock `CommandExecutor` to simulate kopia CLI
- Test cases: snapshots found, empty result, command error, connect success/failure

**Handler tests**:
- Already mock `BackendClient` interface
- Add cases verifying new response format

**Integration tests**:
- Skipped by default (`t.Skip("requires kopia repository")`)
- Manual verification against real repository

## Startup Sequence (kopia-fs)

1. Load config, validate `KOPIA_REPOSITORY_PATH` exists
2. Create kopia client
3. Run `kopia repository connect filesystem --path /repository`
4. If connect fails → exit (pod won't become ready)
5. Start HTTP server
6. `/readyz` returns ok if connected

## Graceful Shutdown

No special cleanup needed - kopia CLI doesn't hold persistent connections.

## Files Changed

| File | Change |
|------|--------|
| `internal/backend/types.go` | NEW |
| `internal/kopia/client.go` | NEW |
| `internal/kopia/client_test.go` | NEW |
| `internal/config/config.go` | Add BackendType, KopiaRepositoryPath |
| `internal/config/config_test.go` | Add new config tests |
| `internal/handler/handler.go` | Rename S3Client → BackendClient |
| `internal/handler/handler_test.go` | Update for new response format |
| `internal/s3/client.go` | Use backend.CheckResult, remove keyCount |
| `internal/s3/client_test.go` | Update for new result type |
| `cmd/pvc-plumber/main.go` | Backend factory, kopia connect |
