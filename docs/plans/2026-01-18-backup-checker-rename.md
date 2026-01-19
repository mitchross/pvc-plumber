# Backup-Checker Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rename pvc-plumber to backup-checker and add optional Prometheus metrics endpoint.

**Architecture:** The existing pvc-plumber implementation is complete and matches specs. This plan renames the project to "backup-checker" and adds the optional /metrics endpoint for Prometheus.

**Tech Stack:** Go 1.22+, standard library only (net/http, encoding/xml, log/slog), distroless Docker image.

---

## Pre-Implementation Decision

The current codebase is functionally complete. Two options:

**Option A: Keep as pvc-plumber** - No changes needed, existing implementation matches all required specs.

**Option B: Rename to backup-checker** - Follow tasks below to rename.

If proceeding with Option B, continue with the tasks below.

---

### Task 1: Rename main.go entry point

**Files:**
- Rename: `cmd/pvc-plumber/` to `cmd/backup-checker/`
- Modify: `cmd/backup-checker/main.go` - update log message

**Step 1: Rename directory**

```bash
git mv cmd/pvc-plumber cmd/backup-checker
```

**Step 2: Update log message in main.go**

Change line 46 from:
```go
logger.Info("starting pvc-plumber",
```
To:
```go
logger.Info("starting backup-checker",
```

**Step 3: Verify build compiles**

Run: `go build -o backup-checker ./cmd/backup-checker`
Expected: Binary compiles successfully

**Step 4: Commit**

```bash
git add cmd/
git commit -m "refactor: rename cmd/pvc-plumber to cmd/backup-checker"
```

---

### Task 2: Update go.mod module path

**Files:**
- Modify: `go.mod`

**Step 1: Update module path**

Change:
```go
module github.com/mitchross/pvc-pulmber
```
To:
```go
module github.com/mitchross/backup-checker
```

**Step 2: Update all import statements**

Files to update:
- `cmd/backup-checker/main.go`
- `internal/handler/handler.go`
- `internal/handler/handler_test.go`

Replace all occurrences of:
```go
github.com/mitchross/pvc-pulmber
```
With:
```go
github.com/mitchross/backup-checker
```

**Step 3: Run go mod tidy**

```bash
go mod tidy
```

**Step 4: Verify tests pass**

Run: `go test ./...`
Expected: All tests pass

**Step 5: Commit**

```bash
git add go.mod go.sum cmd/ internal/
git commit -m "refactor: update module path to backup-checker"
```

---

### Task 3: Update Dockerfile binary paths

**Files:**
- Modify: `Dockerfile`
- Modify: `Dockerfile.debug`

**Step 1: Update Dockerfile**

Change line 14:
```dockerfile
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o pvc-plumber ./cmd/pvc-plumber
```
To:
```dockerfile
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o backup-checker ./cmd/backup-checker
```

Change line 22:
```dockerfile
COPY --from=builder /build/pvc-plumber /pvc-plumber
```
To:
```dockerfile
COPY --from=builder /build/backup-checker /backup-checker
```

Change line 29:
```dockerfile
ENTRYPOINT ["/pvc-plumber"]
```
To:
```dockerfile
ENTRYPOINT ["/backup-checker"]
```

**Step 2: Update Dockerfile.debug similarly**

Apply same changes to Dockerfile.debug.

**Step 3: Verify Docker build**

Run: `docker build -t backup-checker:test .`
Expected: Image builds successfully

**Step 4: Commit**

```bash
git add Dockerfile Dockerfile.debug
git commit -m "refactor: update Dockerfiles for backup-checker binary"
```

---

### Task 4: Update Makefile

**Files:**
- Modify: `Makefile`

**Step 1: Update BINARY_NAME**

Change line 4:
```makefile
BINARY_NAME=pvc-plumber
```
To:
```makefile
BINARY_NAME=backup-checker
```

**Step 2: Update DOCKER_IMAGE**

Change line 5:
```makefile
DOCKER_IMAGE=ghcr.io/mitchross/pvc-plumber
```
To:
```makefile
DOCKER_IMAGE=ghcr.io/mitchross/backup-checker
```

**Step 3: Update build command path**

Change line 14:
```makefile
CGO_ENABLED=0 go build -a -installsuffix cgo $(LDFLAGS) -o $(BINARY_NAME) ./cmd/pvc-plumber
```
To:
```makefile
CGO_ENABLED=0 go build -a -installsuffix cgo $(LDFLAGS) -o $(BINARY_NAME) ./cmd/backup-checker
```

**Step 4: Verify make build works**

Run: `make build`
Expected: Binary named "backup-checker" is created

**Step 5: Commit**

```bash
git add Makefile
git commit -m "refactor: update Makefile for backup-checker"
```

---

### Task 5: Update .gitignore

**Files:**
- Modify: `.gitignore`

**Step 1: Update binary name in .gitignore**

Change line 2:
```
pvc-plumber
```
To:
```
backup-checker
```

**Step 2: Commit**

```bash
git add .gitignore
git commit -m "refactor: update .gitignore for backup-checker binary"
```

---

### Task 6: Update GitHub Actions workflows

**Files:**
- Modify: `.github/workflows/build.yaml`
- Modify: `.github/workflows/release.yaml`

**Step 1: Update build.yaml**

Change line 71:
```yaml
run: CGO_ENABLED=0 go build -v -o pvc-plumber ./cmd/pvc-plumber
```
To:
```yaml
run: CGO_ENABLED=0 go build -v -o backup-checker ./cmd/backup-checker
```

**Step 2: Verify build.yaml syntax**

Run: `cat .github/workflows/build.yaml | head -80`
Expected: Valid YAML with updated paths

**Step 3: Commit**

```bash
git add .github/workflows/
git commit -m "refactor: update GitHub Actions for backup-checker"
```

---

### Task 7: Update README.md

**Files:**
- Modify: `README.md`

**Step 1: Update all references**

Replace all occurrences of:
- `pvc-plumber` → `backup-checker`
- `pvc-pulmber` → `backup-checker` (fix typo in repo name)

Key sections to update:
- Title (line 1)
- Badge URLs (lines 3-4)
- Quick start Docker command (line 45-48)
- Kubernetes deployment image (line 71)
- All binary references

**Step 2: Verify README renders correctly**

Run: `cat README.md | head -50`
Expected: Updated project name throughout

**Step 3: Commit**

```bash
git add README.md
git commit -m "docs: update README for backup-checker"
```

---

### Task 8: Add optional Prometheus metrics endpoint (Optional)

**Files:**
- Modify: `internal/handler/handler.go`
- Modify: `internal/handler/handler_test.go`
- Modify: `cmd/backup-checker/main.go`

**Step 1: Write failing test for /metrics**

Add to `internal/handler/handler_test.go`:
```go
func TestHandleMetrics(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := New(nil, logger)

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	handler.HandleMetrics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if !strings.Contains(body, "backup_checker_requests_total") {
		t.Error("Expected metrics output to contain backup_checker_requests_total")
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./internal/handler -run TestHandleMetrics -v`
Expected: FAIL - HandleMetrics undefined

**Step 3: Add metrics types to handler**

Add to `internal/handler/handler.go` after imports:
```go
import (
	"fmt"
	"sync/atomic"
	// ... existing imports
)

type Handler struct {
	s3Client       *s3.Client
	logger         *slog.Logger
	requestsTotal  atomic.Int64
	requestsErrors atomic.Int64
}
```

**Step 4: Add HandleMetrics method**

Add to `internal/handler/handler.go`:
```go
func (h *Handler) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP backup_checker_requests_total Total number of backup check requests\n")
	fmt.Fprintf(w, "# TYPE backup_checker_requests_total counter\n")
	fmt.Fprintf(w, "backup_checker_requests_total %d\n", h.requestsTotal.Load())
	fmt.Fprintf(w, "# HELP backup_checker_requests_errors_total Total number of failed backup check requests\n")
	fmt.Fprintf(w, "# TYPE backup_checker_requests_errors_total counter\n")
	fmt.Fprintf(w, "backup_checker_requests_errors_total %d\n", h.requestsErrors.Load())
}
```

**Step 5: Update HandleExists to increment counters**

Add at the start of HandleExists:
```go
h.requestsTotal.Add(1)
```

Add when error occurs:
```go
if result.Error != "" {
	h.requestsErrors.Add(1)
}
```

**Step 6: Run test to verify it passes**

Run: `go test ./internal/handler -run TestHandleMetrics -v`
Expected: PASS

**Step 7: Register /metrics endpoint in main.go**

Add to `cmd/backup-checker/main.go`:
```go
mux.HandleFunc("/metrics", h.HandleMetrics)
```

**Step 8: Run all tests**

Run: `go test ./... -v`
Expected: All tests pass

**Step 9: Commit**

```bash
git add internal/handler/ cmd/backup-checker/
git commit -m "feat: add /metrics endpoint for Prometheus"
```

---

### Task 9: Final verification

**Step 1: Run full test suite**

Run: `make test`
Expected: All tests pass with >80% coverage

**Step 2: Run linter**

Run: `make lint`
Expected: No linting errors

**Step 3: Build Docker image**

Run: `make docker-build`
Expected: Image builds successfully

**Step 4: Test Docker image**

Run:
```bash
docker run -d --name backup-checker-test \
  -e S3_ENDPOINT=http://example.com \
  -e S3_BUCKET=test-bucket \
  -p 8080:8080 \
  backup-checker:latest
curl http://localhost:8080/healthz
docker stop backup-checker-test && docker rm backup-checker-test
```
Expected: `{"status":"ok"}`

**Step 5: Commit any remaining changes**

```bash
git status
# If any uncommitted changes, commit them
```

---

## Summary

After completing all tasks:
- Project renamed from pvc-plumber to backup-checker
- All paths and references updated
- Optional /metrics endpoint added
- Tests pass with >80% coverage
- Docker image builds and runs correctly
