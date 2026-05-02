package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// Test fixture constants. Centralized so goconst stops complaining about
// repeated literals and so renames are one-line changes.
const (
	testNamespace = "test-ns"
	testPVC       = "test-pvc"
)

// mockBackendClient implements BackendClient interface for testing
type mockBackendClient struct {
	result backend.CheckResult
}

func (m *mockBackendClient) CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult {
	return m.result
}

type deadlineCapturingBackend struct {
	hasDeadline bool
}

func (m *deadlineCapturingBackend) CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult {
	_, m.hasDeadline = ctx.Deadline()
	return backend.CheckResult{
		Exists:        false,
		Decision:      backend.DecisionFresh,
		Authoritative: true,
		Namespace:     namespace,
		Pvc:           pvc,
		Backend:       backend.TypeKopiaFS,
	}
}

func TestHandleExists(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	tests := []struct {
		name              string
		path              string
		mockResult        backend.CheckResult
		wantStatus        int
		wantExists        bool
		wantDecision      string
		wantAuthoritative bool
		wantBackend       string
		wantError         bool
	}{
		{
			name: "backup exists",
			path: "/exists/karakeep/data-pvc",
			mockResult: backend.CheckResult{
				Exists:        true,
				Decision:      backend.DecisionRestore,
				Authoritative: true,
				Namespace:     "karakeep",
				Pvc:           "data-pvc",
				Backend:       "s3",
			},
			wantStatus:        http.StatusOK,
			wantExists:        true,
			wantDecision:      backend.DecisionRestore,
			wantAuthoritative: true,
			wantBackend:       "s3",
			wantError:         false,
		},
		{
			name: "no backup",
			path: "/exists/" + testNamespace + "/" + testPVC,
			mockResult: backend.CheckResult{
				Exists:        false,
				Decision:      backend.DecisionFresh,
				Authoritative: true,
				Namespace:     testNamespace,
				Pvc:           testPVC,
				Backend:       backend.TypeKopiaFS,
			},
			wantStatus:        http.StatusOK,
			wantExists:        false,
			wantDecision:      backend.DecisionFresh,
			wantAuthoritative: true,
			wantBackend:       backend.TypeKopiaFS,
			wantError:         false,
		},
		{
			name: "backend error",
			path: "/exists/error-ns/error-pvc",
			mockResult: backend.CheckResult{
				Exists:        false,
				Decision:      backend.DecisionUnknown,
				Authoritative: false,
				Namespace:     "error-ns",
				Pvc:           "error-pvc",
				Backend:       "s3",
				Error:         "connection failed",
			},
			wantStatus:        http.StatusServiceUnavailable,
			wantExists:        false,
			wantDecision:      backend.DecisionUnknown,
			wantAuthoritative: false,
			wantBackend:       "s3",
			wantError:         true,
		},
		{
			name:              "invalid path - no pvc",
			path:              "/exists/namespace-only",
			wantStatus:        http.StatusBadRequest,
			wantExists:        false,
			wantDecision:      backend.DecisionUnknown,
			wantAuthoritative: false,
			wantError:         true,
		},
		{
			name:              "invalid path - empty",
			path:              "/exists/",
			wantStatus:        http.StatusBadRequest,
			wantExists:        false,
			wantDecision:      backend.DecisionUnknown,
			wantAuthoritative: false,
			wantError:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockBackendClient{result: tt.mockResult}
			handler := New(mock, logger)

			req := httptest.NewRequestWithContext(context.Background(), "GET", tt.path, nil)
			w := httptest.NewRecorder()

			handler.HandleExists(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("Status = %v, want %v", w.Code, tt.wantStatus)
			}

			var response backend.CheckResult
			if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
				t.Fatalf("Failed to decode response: %v", err)
			}

			if response.Exists != tt.wantExists {
				t.Errorf("Exists = %v, want %v", response.Exists, tt.wantExists)
			}

			if response.Decision != tt.wantDecision {
				t.Errorf("Decision = %v, want %v", response.Decision, tt.wantDecision)
			}

			if response.Authoritative != tt.wantAuthoritative {
				t.Errorf("Authoritative = %v, want %v", response.Authoritative, tt.wantAuthoritative)
			}

			if tt.wantStatus == http.StatusOK && response.Backend != tt.wantBackend {
				t.Errorf("Backend = %v, want %v", response.Backend, tt.wantBackend)
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

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/healthz", nil)
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

func TestHandleExistsAppliesRequestTimeout(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	mock := &deadlineCapturingBackend{}
	handler := New(mock, logger)
	handler.SetRequestTimeout(50 * time.Millisecond)

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/exists/"+testNamespace+"/"+testPVC, nil)
	w := httptest.NewRecorder()

	handler.HandleExists(w, req)

	if !mock.hasDeadline {
		t.Fatal("backend context should have a deadline")
	}
}

func TestHandleReadyz(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := New(nil, logger)

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/readyz", nil)
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

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/metrics", nil)
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

	mock := &mockBackendClient{result: backend.CheckResult{
		Exists:        true,
		Decision:      backend.DecisionRestore,
		Authoritative: true,
		Namespace:     testNamespace,
		Pvc:           testPVC,
		Backend:       "s3",
	}}
	handler := New(mock, logger)

	// Make a request to /exists
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/exists/"+testNamespace+"/"+testPVC, nil)
	w := httptest.NewRecorder()
	handler.HandleExists(w, req)

	// Check metrics
	metricsReq := httptest.NewRequestWithContext(context.Background(), "GET", "/metrics", nil)
	metricsW := httptest.NewRecorder()
	handler.HandleMetrics(metricsW, metricsReq)

	body := metricsW.Body.String()
	if !strings.Contains(body, "pvc_plumber_requests_total 1") {
		t.Errorf("Expected requests_total to be 1, got: %s", body)
	}
	if !strings.Contains(body, `pvc_plumber_backup_check_total{backend="s3",decision="restore"} 1`) {
		t.Errorf("Expected labeled restore counter, got: %s", body)
	}
}

func TestMetricsErrorCounter(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockBackendClient{result: backend.CheckResult{
		Exists:        false,
		Decision:      backend.DecisionUnknown,
		Authoritative: false,
		Namespace:     testNamespace,
		Pvc:           testPVC,
		Backend:       "s3",
		Error:         "connection failed",
	}}
	handler := New(mock, logger)

	// Make a request to /exists that will result in error
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/exists/"+testNamespace+"/"+testPVC, nil)
	w := httptest.NewRecorder()
	handler.HandleExists(w, req)

	// Check metrics
	metricsReq := httptest.NewRequestWithContext(context.Background(), "GET", "/metrics", nil)
	metricsW := httptest.NewRecorder()
	handler.HandleMetrics(metricsW, metricsReq)

	body := metricsW.Body.String()
	if !strings.Contains(body, "pvc_plumber_requests_errors_total 1") {
		t.Errorf("Expected errors_total to be 1, got: %s", body)
	}
	if !strings.Contains(body, `pvc_plumber_backup_check_total{backend="s3",decision="unknown"} 1`) {
		t.Errorf("Expected labeled unknown counter, got: %s", body)
	}
}
