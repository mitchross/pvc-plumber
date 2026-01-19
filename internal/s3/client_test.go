package s3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	httpClient := &http.Client{Timeout: 5 * time.Second}
	client := NewClient("http://localhost:9000", "test-bucket", httpClient)

	if client == nil {
		t.Fatal("NewClient returned nil")
	}
	if client.endpoint != "http://localhost:9000" {
		t.Errorf("endpoint = %v, want http://localhost:9000", client.endpoint)
	}
	if client.bucket != "test-bucket" {
		t.Errorf("bucket = %v, want test-bucket", client.bucket)
	}
	if client.httpClient != httpClient {
		t.Error("httpClient was not set correctly")
	}
}

func TestCheckBackupExists(t *testing.T) {
	tests := []struct {
		name           string
		namespace      string
		pvc            string
		responseStatus int
		responseBody   string
		wantExists     bool
		wantKeyCount   int
		wantError      bool
	}{
		{
			name:           "backup exists",
			namespace:      "karakeep",
			pvc:            "data-pvc",
			responseStatus: http.StatusOK,
			responseBody: `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>volsync-backup</Name>
  <Prefix>karakeep/data-pvc/</Prefix>
  <KeyCount>1</KeyCount>
  <MaxKeys>1</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>karakeep/data-pvc/config</Key>
    <LastModified>2026-01-10T01:46:03.000Z</LastModified>
    <Size>155</Size>
  </Contents>
</ListBucketResult>`,
			wantExists:   true,
			wantKeyCount: 1,
			wantError:    false,
		},
		{
			name:           "no backup",
			namespace:      "test-ns",
			pvc:            "test-pvc",
			responseStatus: http.StatusOK,
			responseBody: `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>volsync-backup</Name>
  <Prefix>test-ns/test-pvc/</Prefix>
  <KeyCount>0</KeyCount>
  <MaxKeys>1</MaxKeys>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>`,
			wantExists:   false,
			wantKeyCount: 0,
			wantError:    false,
		},
		{
			name:           "S3 error response",
			namespace:      "error-ns",
			pvc:            "error-pvc",
			responseStatus: http.StatusInternalServerError,
			responseBody:   `<Error><Code>InternalError</Code></Error>`,
			wantExists:     false,
			wantKeyCount:   0,
			wantError:      true,
		},
		{
			name:           "invalid XML",
			namespace:      "invalid-ns",
			pvc:            "invalid-pvc",
			responseStatus: http.StatusOK,
			responseBody:   `not valid xml`,
			wantExists:     false,
			wantKeyCount:   0,
			wantError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.responseStatus)
				_, _ = w.Write([]byte(tt.responseBody))
			}))
			defer server.Close()

			client := NewClient(server.URL, "test-bucket", &http.Client{Timeout: 5 * time.Second})
			result := client.CheckBackupExists(context.Background(), tt.namespace, tt.pvc)

			if result.Exists != tt.wantExists {
				t.Errorf("Exists = %v, want %v", result.Exists, tt.wantExists)
			}
			if result.KeyCount != tt.wantKeyCount {
				t.Errorf("KeyCount = %v, want %v", result.KeyCount, tt.wantKeyCount)
			}
			if tt.wantError && result.Error == "" {
				t.Errorf("Expected error but got none")
			}
			if !tt.wantError && result.Error != "" {
				t.Errorf("Unexpected error: %v", result.Error)
			}
		})
	}
}

func TestCheckBackupExists_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "test-bucket", &http.Client{Timeout: 10 * time.Millisecond})
	ctx := context.Background()
	result := client.CheckBackupExists(ctx, "test", "pvc")

	if result.Exists {
		t.Error("Expected exists=false on timeout")
	}
	if result.Error == "" {
		t.Error("Expected error on timeout")
	}
}

func TestCheckBackupExists_ContextCanceled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "test-bucket", &http.Client{Timeout: 5 * time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	result := client.CheckBackupExists(ctx, "test", "pvc")

	if result.Exists {
		t.Error("Expected exists=false on canceled context")
	}
	if result.Error == "" {
		t.Error("Expected error on canceled context")
	}
}
