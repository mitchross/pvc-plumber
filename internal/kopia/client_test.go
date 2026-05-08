package kopia

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"testing"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// testS3Config is the canonical S3 connection input used by every test.
// Centralized so a future reshape of S3Config (added field, renamed knob)
// is a single-line change instead of N table edits.
func testS3Config() S3Config {
	return S3Config{
		Endpoint:   "http://192.168.10.133:30293",
		Bucket:     "volsync-kopia",
		AccessKey:  "test-access-key",
		SecretKey:  "test-secret-key",
		Password:   "testpass",
		DisableTLS: true,
	}
}

// mockExecutor implements CommandExecutor for testing. lastArgs / lastName
// captures the most recent call so connect-flag assertions don't need a
// dedicated wrapper.
type mockExecutor struct {
	output   []byte
	err      error
	lastName string
	lastArgs []string
}

func (m *mockExecutor) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	m.lastName = name
	m.lastArgs = append([]string(nil), args...)
	return m.output, m.err
}

func TestNewClient(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	client := NewClient(testS3Config(), logger)

	if client == nil {
		t.Fatal("NewClient returned nil")
	}
	if client.cfg.Bucket != "volsync-kopia" {
		t.Errorf("cfg.Bucket = %v, want volsync-kopia", client.cfg.Bucket)
	}
	if client.connected {
		t.Error("client should not be connected initially")
	}
}

func TestConnect_Success(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{
		output: []byte("Connected to repository."),
		err:    nil,
	}

	client := NewClientWithExecutor(testS3Config(), logger, mock)

	err := client.Connect(context.Background())
	if err != nil {
		t.Errorf("Connect() error = %v, want nil", err)
	}

	if !client.connected {
		t.Error("client should be connected after successful Connect()")
	}

	if !client.IsConnected() {
		t.Error("IsConnected() should return true")
	}

	// Pin the kopia CLI invocation shape — both `kopia` itself and the
	// `repository connect s3` sub-args plus the --disable-tls flag the
	// reference cluster needs for in-cluster RustFS over HTTP.
	if mock.lastName != "kopia" {
		t.Errorf("executor command = %q, want kopia", mock.lastName)
	}
	wantPrefix := []string{"repository", "connect", "s3"}
	if len(mock.lastArgs) < len(wantPrefix) {
		t.Fatalf("executor args = %v, too short", mock.lastArgs)
	}
	for i, w := range wantPrefix {
		if mock.lastArgs[i] != w {
			t.Errorf("executor args[%d] = %q, want %q", i, mock.lastArgs[i], w)
		}
	}
	if !slices.Contains(mock.lastArgs, "--endpoint") || !slices.Contains(mock.lastArgs, "--bucket") {
		t.Errorf("executor args missing --endpoint/--bucket; got %v", mock.lastArgs)
	}
	if !slices.Contains(mock.lastArgs, "--disable-tls") {
		t.Errorf("executor args missing --disable-tls (DisableTLS=true should set it); got %v", mock.lastArgs)
	}
	// Regression: the legacy filesystem connect form must NEVER appear.
	for _, banned := range []string{"filesystem", "--path"} {
		if slices.Contains(mock.lastArgs, banned) {
			t.Errorf("executor args contains legacy filesystem flag %q; got %v", banned, mock.lastArgs)
		}
	}
}

// TestConnect_NoDisableTLSWhenFalse pins the inverse: when DisableTLS is
// false (production-shaped HTTPS RustFS or any TLS-terminated endpoint),
// `--disable-tls` must NOT be passed.
func TestConnect_NoDisableTLSWhenFalse(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := testS3Config()
	cfg.DisableTLS = false
	mock := &mockExecutor{output: []byte("Connected to repository."), err: nil}
	client := NewClientWithExecutor(cfg, logger, mock)

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if slices.Contains(mock.lastArgs, "--disable-tls") {
		t.Errorf("executor args should NOT contain --disable-tls when DisableTLS=false; got %v", mock.lastArgs)
	}
}

func TestConnect_Failure(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{
		output: []byte("repository not found"),
		err:    errors.New("exit status 1"),
	}

	client := NewClientWithExecutor(testS3Config(), logger, mock)

	err := client.Connect(context.Background())
	if err == nil {
		t.Error("Connect() should have returned an error")
	}

	if client.connected {
		t.Error("client should not be connected after failed Connect()")
	}
}

func TestCheckBackupExists_Found(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Mock kopia snapshot list output with snapshots
	snapshotJSON := `[
		{
			"id": "abc123",
			"source": {"host": "test-pvc-backup", "userName": "karakeep", "path": "/data"},
			"startTime": "2024-01-15T10:00:00Z"
		}
	]`

	mock := &mockExecutor{
		output: []byte(snapshotJSON),
		err:    nil,
	}

	client := NewClientWithExecutor(testS3Config(), logger, mock)

	result := client.CheckBackupExists(context.Background(), "karakeep", "test-pvc")

	if !result.Exists {
		t.Error("Exists should be true")
	}
	if result.Decision != backend.DecisionRestore {
		t.Errorf("Decision = %v, want %v", result.Decision, backend.DecisionRestore)
	}
	if !result.Authoritative {
		t.Error("Authoritative should be true")
	}
	if result.Source != "test-pvc-backup@karakeep:/data" {
		t.Errorf("Source = %v, want test-pvc-backup@karakeep:/data", result.Source)
	}
	if result.Namespace != "karakeep" {
		t.Errorf("Namespace = %v, want karakeep", result.Namespace)
	}
	if result.Pvc != "test-pvc" {
		t.Errorf("Pvc = %v, want test-pvc", result.Pvc)
	}
	if result.Backend != backend.TypeKopiaS3 {
		t.Errorf("Backend = %v, want %s", result.Backend, backend.TypeKopiaS3)
	}
	if result.Error != "" {
		t.Errorf("Error = %v, want empty", result.Error)
	}
}

func TestCheckBackupExists_NotFound(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{
		output: []byte("[]"),
		err:    nil,
	}

	client := NewClientWithExecutor(testS3Config(), logger, mock)

	result := client.CheckBackupExists(context.Background(), "foo", "bar")

	if result.Exists {
		t.Error("Exists should be false")
	}
	if result.Decision != backend.DecisionFresh {
		t.Errorf("Decision = %v, want %v", result.Decision, backend.DecisionFresh)
	}
	if !result.Authoritative {
		t.Error("Authoritative should be true")
	}
	if result.Source != "bar-backup@foo:/data" {
		t.Errorf("Source = %v, want bar-backup@foo:/data", result.Source)
	}
	if result.Backend != backend.TypeKopiaS3 {
		t.Errorf("Backend = %v, want %s", result.Backend, backend.TypeKopiaS3)
	}
}

func TestCheckBackupExists_CommandError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{
		output: nil,
		err:    errors.New("command failed"),
	}

	client := NewClientWithExecutor(testS3Config(), logger, mock)

	result := client.CheckBackupExists(context.Background(), "test-ns", "test-pvc")

	if result.Exists {
		t.Error("Exists should be false on error")
	}
	if result.Decision != backend.DecisionUnknown {
		t.Errorf("Decision = %v, want %v", result.Decision, backend.DecisionUnknown)
	}
	if result.Authoritative {
		t.Error("Authoritative should be false on error")
	}
	if result.Error == "" {
		t.Error("Error should not be empty on command failure")
	}
	if result.Backend != backend.TypeKopiaS3 {
		t.Errorf("Backend = %v, want %s", result.Backend, backend.TypeKopiaS3)
	}
}

func TestCheckBackupExists_InvalidJSON(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{
		output: []byte("not valid json"),
		err:    nil,
	}

	client := NewClientWithExecutor(testS3Config(), logger, mock)

	result := client.CheckBackupExists(context.Background(), "test-ns", "test-pvc")

	if result.Exists {
		t.Error("Exists should be false on JSON parse error")
	}
	if result.Decision != backend.DecisionUnknown {
		t.Errorf("Decision = %v, want %v", result.Decision, backend.DecisionUnknown)
	}
	if result.Authoritative {
		t.Error("Authoritative should be false on JSON parse error")
	}
	if result.Error == "" {
		t.Error("Error should not be empty on JSON parse failure")
	}
}

func TestHealthCheck_NotConnected(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	client := NewClient(testS3Config(), logger)

	err := client.HealthCheck(context.Background())
	if err == nil {
		t.Error("HealthCheck should fail when client is not connected")
	}
}

// TestHealthCheck_Success verifies that once Connect() has marked the client
// connected, the readiness probe returns nil. v3.0.0 dropped the
// os.Stat(repoPath) check that made sense for filesystem-backed kopia repos
// — there is no local mount to stat anymore.
func TestHealthCheck_Success(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{output: []byte("Connected to repository."), err: nil}
	client := NewClientWithExecutor(testS3Config(), logger, mock)

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}

	if err := client.HealthCheck(context.Background()); err != nil {
		t.Errorf("HealthCheck() error = %v, want nil", err)
	}
}

// TestHealthCheck_DoesNotInvokeKopia is a regression test against the
// pre-1.5 behavior where HealthCheck spawned `kopia repository status` on
// every probe and frequently exceeded the kubelet probe timeout.
func TestHealthCheck_DoesNotInvokeKopia(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Connect first with a benign mock, then swap in a panicking executor.
	connectMock := &mockExecutor{output: []byte("Connected to repository."), err: nil}
	client := NewClientWithExecutor(testS3Config(), logger, connectMock)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}

	// This executor fails the test if any command is run against it.
	client.executor = &panickingExecutor{t: t}

	if err := client.HealthCheck(context.Background()); err != nil {
		t.Errorf("HealthCheck() error = %v, want nil", err)
	}
}

// panickingExecutor fails the test if any command is run against it.
type panickingExecutor struct {
	t *testing.T
}

func (p *panickingExecutor) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	p.t.Errorf("executor.Run should not be called on the readiness path; got %s %v", name, args)
	return nil, errors.New("unexpected subprocess invocation")
}

func TestCheckBackupExists_Integration(t *testing.T) {
	// Skip in CI - this test requires a real kopia repository
	t.Skip("Integration test - requires kopia repository")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	client := NewClient(testS3Config(), logger)

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	result := client.CheckBackupExists(context.Background(), "test-ns", "test-pvc")
	t.Logf("Result: exists=%v, namespace=%s, pvc=%s, backend=%s, error=%s",
		result.Exists, result.Namespace, result.Pvc, result.Backend, result.Error)
}
