package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mitchross/pvc-pulmber/internal/s3"
)

func TestHandleExists(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	tests := []struct {
		name         string
		path         string
		mockResult   s3.CheckResult
		wantStatus   int
		wantExists   bool
		wantKeyCount int
		wantError    bool
	}{
		{
			name: "backup exists",
			path: "/exists/karakeep/data-pvc",
			mockResult: s3.CheckResult{
				Exists:   true,
				KeyCount: 1,
			},
			wantStatus:   http.StatusOK,
			wantExists:   true,
			wantKeyCount: 1,
			wantError:    false,
		},
		{
			name: "no backup",
			path: "/exists/test-ns/test-pvc",
			mockResult: s3.CheckResult{
				Exists:   false,
				KeyCount: 0,
			},
			wantStatus:   http.StatusOK,
			wantExists:   false,
			wantKeyCount: 0,
			wantError:    false,
		},
		{
			name: "S3 error",
			path: "/exists/error-ns/error-pvc",
			mockResult: s3.CheckResult{
				Exists:   false,
				KeyCount: 0,
				Error:    "S3 connection failed",
			},
			wantStatus:   http.StatusOK,
			wantExists:   false,
			wantKeyCount: 0,
			wantError:    true,
		},
		{
			name:       "invalid path - no pvc",
			path:       "/exists/namespace-only",
			wantStatus: http.StatusBadRequest,
			wantExists: false,
			wantError:  true,
		},
		{
			name:       "invalid path - empty",
			path:       "/exists/",
			wantStatus: http.StatusBadRequest,
			wantExists: false,
			wantError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock S3 client
			s3Client := &s3.Client{}
			if tt.wantStatus == http.StatusOK {
				// For valid paths, we'll replace CheckBackupExists
				// We need to create handler with mock that returns our result
				// Let's use a test server approach
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					if tt.mockResult.Exists {
						_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <KeyCount>1</KeyCount>
</ListBucketResult>`))
					} else if tt.mockResult.Error != "" {
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = w.Write([]byte(`error`))
					} else {
						_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <KeyCount>0</KeyCount>
</ListBucketResult>`))
					}
				}))
				defer server.Close()

				if tt.mockResult.Error != "" {
					server.Close()
					// Create a dead server for error case
					server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusInternalServerError)
					}))
					defer server.Close()
				}

				s3Client = s3.NewClient(server.URL, "test-bucket", &http.Client{Timeout: 5 * time.Second})
			}

			handler := New(s3Client, logger)

			req := httptest.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()

			handler.HandleExists(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("Status = %v, want %v", w.Code, tt.wantStatus)
			}

			var response s3.CheckResult
			if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
				t.Fatalf("Failed to decode response: %v", err)
			}

			if response.Exists != tt.wantExists {
				t.Errorf("Exists = %v, want %v", response.Exists, tt.wantExists)
			}

			if tt.wantStatus == http.StatusOK && response.KeyCount != tt.wantKeyCount {
				t.Errorf("KeyCount = %v, want %v", response.KeyCount, tt.wantKeyCount)
			}

			if tt.wantError && response.Error == "" {
				t.Errorf("Expected error but got none")
			}
		})
	}
}

func TestHandleHealthz(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := New(nil, logger)

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	handler.HandleHealthz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusOK)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["status"] != "ok" {
		t.Errorf("Status = %v, want ok", response["status"])
	}
}

func TestHandleReadyz(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := New(nil, logger)

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()

	handler.HandleReadyz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusOK)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["status"] != "ok" {
		t.Errorf("Status = %v, want ok", response["status"])
	}
}

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

	// Check for Prometheus format
	if !strings.Contains(body, "# HELP") {
		t.Error("Expected metrics output to contain # HELP comments")
	}
	if !strings.Contains(body, "# TYPE") {
		t.Error("Expected metrics output to contain # TYPE comments")
	}
	if !strings.Contains(body, "pvc_plumber_requests_total") {
		t.Error("Expected metrics output to contain pvc_plumber_requests_total")
	}
	if !strings.Contains(body, "pvc_plumber_requests_errors_total") {
		t.Error("Expected metrics output to contain pvc_plumber_requests_errors_total")
	}

	// Check content type
	contentType := w.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") {
		t.Errorf("Content-Type = %v, want text/plain", contentType)
	}
}

func TestMetricsCounters(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Create a mock server that returns success
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <KeyCount>1</KeyCount>
</ListBucketResult>`))
	}))
	defer server.Close()

	s3Client := s3.NewClient(server.URL, "test-bucket", &http.Client{Timeout: 5 * time.Second})
	handler := New(s3Client, logger)

	// Make a request to /exists
	req := httptest.NewRequest("GET", "/exists/test-ns/test-pvc", nil)
	w := httptest.NewRecorder()
	handler.HandleExists(w, req)

	// Check metrics
	metricsReq := httptest.NewRequest("GET", "/metrics", nil)
	metricsW := httptest.NewRecorder()
	handler.HandleMetrics(metricsW, metricsReq)

	body := metricsW.Body.String()
	if !strings.Contains(body, "pvc_plumber_requests_total 1") {
		t.Errorf("Expected requests_total to be 1, got: %s", body)
	}
}
