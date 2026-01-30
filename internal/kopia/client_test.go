package kopia

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
)

const backendKopiaFS = "kopia-fs"

// mockExecutor implements CommandExecutor for testing
type mockExecutor struct {
	output []byte
	err    error
}

func (m *mockExecutor) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return m.output, m.err
}

func TestNewClient(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	client := NewClient("/test/path", logger)

	if client == nil {
		t.Fatal("NewClient returned nil")
	}
	if client.repoPath != "/test/path" {
		t.Errorf("repoPath = %v, want /test/path", client.repoPath)
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

	client := NewClientWithExecutor("/test/path", logger, mock)

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
}

func TestConnect_Failure(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{
		output: []byte("repository not found"),
		err:    errors.New("exit status 1"),
	}

	client := NewClientWithExecutor("/bad/path", logger, mock)

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

	client := NewClientWithExecutor("/repository", logger, mock)

	result := client.CheckBackupExists(context.Background(), "karakeep", "test-pvc")

	if !result.Exists {
		t.Error("Exists should be true")
	}
	if result.Namespace != "karakeep" {
		t.Errorf("Namespace = %v, want karakeep", result.Namespace)
	}
	if result.Pvc != "test-pvc" {
		t.Errorf("Pvc = %v, want test-pvc", result.Pvc)
	}
	if result.Backend != backendKopiaFS {
		t.Errorf("Backend = %v, want %s", result.Backend, backendKopiaFS)
	}
	if result.Error != "" {
		t.Errorf("Error = %v, want empty", result.Error)
	}
}

func TestCheckBackupExists_NotFound(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Mock kopia snapshot list output with empty array
	mock := &mockExecutor{
		output: []byte("[]"),
		err:    nil,
	}

	client := NewClientWithExecutor("/repository", logger, mock)

	result := client.CheckBackupExists(context.Background(), "foo", "bar")

	if result.Exists {
		t.Error("Exists should be false")
	}
	if result.Namespace != "foo" {
		t.Errorf("Namespace = %v, want foo", result.Namespace)
	}
	if result.Pvc != "bar" {
		t.Errorf("Pvc = %v, want bar", result.Pvc)
	}
	if result.Backend != backendKopiaFS {
		t.Errorf("Backend = %v, want %s", result.Backend, backendKopiaFS)
	}
	if result.Error != "" {
		t.Errorf("Error = %v, want empty", result.Error)
	}
}

func TestCheckBackupExists_CommandError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{
		output: nil,
		err:    errors.New("command failed"),
	}

	client := NewClientWithExecutor("/repository", logger, mock)

	result := client.CheckBackupExists(context.Background(), "test-ns", "test-pvc")

	if result.Exists {
		t.Error("Exists should be false on error")
	}
	if result.Error == "" {
		t.Error("Error should not be empty on command failure")
	}
	if result.Namespace != "test-ns" {
		t.Errorf("Namespace = %v, want test-ns", result.Namespace)
	}
	if result.Pvc != "test-pvc" {
		t.Errorf("Pvc = %v, want test-pvc", result.Pvc)
	}
	if result.Backend != backendKopiaFS {
		t.Errorf("Backend = %v, want %s", result.Backend, backendKopiaFS)
	}
}

func TestCheckBackupExists_InvalidJSON(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{
		output: []byte("not valid json"),
		err:    nil,
	}

	client := NewClientWithExecutor("/repository", logger, mock)

	result := client.CheckBackupExists(context.Background(), "test-ns", "test-pvc")

	if result.Exists {
		t.Error("Exists should be false on JSON parse error")
	}
	if result.Error == "" {
		t.Error("Error should not be empty on JSON parse failure")
	}
}

func TestCheckBackupExists_Integration(t *testing.T) {
	// Skip in CI - this test requires a real kopia repository
	t.Skip("Integration test - requires kopia repository")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	client := NewClient("/repository", logger)

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	result := client.CheckBackupExists(context.Background(), "test-ns", "test-pvc")
	t.Logf("Result: exists=%v, namespace=%s, pvc=%s, backend=%s, error=%s",
		result.Exists, result.Namespace, result.Pvc, result.Backend, result.Error)
}
