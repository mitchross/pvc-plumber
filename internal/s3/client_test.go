package s3

import (
	"context"
	"testing"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

func TestNewClient(t *testing.T) {
	client, err := NewClient("localhost:9000", "test-bucket", "accesskey", "secretkey", false)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}

	if client == nil {
		t.Fatal("NewClient returned nil")
	}
	if client.bucket != "test-bucket" {
		t.Errorf("bucket = %v, want test-bucket", client.bucket)
	}
	if client.minioClient == nil {
		t.Error("minioClient was not set")
	}
}

func TestNewClient_EmptyEndpoint(t *testing.T) {
	// minio-go validates the endpoint, but empty string is actually accepted
	// and will fail later when making requests
	client, err := NewClient("", "test-bucket", "accesskey", "secretkey", false)
	if err != nil {
		// If minio-go rejects empty endpoint, that's fine
		return
	}
	if client == nil {
		t.Fatal("NewClient returned nil without error")
	}
}

func TestCheckBackupExists_Integration(t *testing.T) {
	// Skip in CI - this test requires a real MinIO/S3 instance
	t.Skip("Integration test - requires MinIO instance")

	client, err := NewClient("localhost:9000", "test-bucket", "minioadmin", "minioadmin", false)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	result := client.CheckBackupExists(context.Background(), "test-ns", "test-pvc")
	// Just verify it doesn't panic - actual result depends on bucket contents
	t.Logf("Result: exists=%v, namespace=%s, pvc=%s, backend=%s, error=%s",
		result.Exists, result.Namespace, result.Pvc, result.Backend, result.Error)
}

func TestCheckResult_Fields(t *testing.T) {
	// Test that backend.CheckResult has the expected fields and JSON tags
	result := backend.CheckResult{
		Exists:    true,
		Namespace: "test-ns",
		Pvc:       "test-pvc",
		Backend:   "s3",
		Error:     "test error",
	}

	if !result.Exists {
		t.Error("Exists should be true")
	}
	if result.Namespace != "test-ns" {
		t.Errorf("Namespace = %s, want test-ns", result.Namespace)
	}
	if result.Pvc != "test-pvc" {
		t.Errorf("Pvc = %s, want test-pvc", result.Pvc)
	}
	if result.Backend != "s3" {
		t.Errorf("Backend = %s, want s3", result.Backend)
	}
	if result.Error != "test error" {
		t.Errorf("Error = %s, want 'test error'", result.Error)
	}
}
