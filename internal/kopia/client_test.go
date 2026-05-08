package kopia

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

const (
	// testPassword is the canonical test kopia password used across the
	// credentials-source and connect tests.
	testPassword  = "testpass"
	testAccessKey = "test-access-key"
	testSecretKey = "test-secret-key"

	// kopia CLI subcommand literals appearing in test-side argv assertions
	// — promoted to constants because goconst (lint) flags >=3 occurrences
	// across the file.
	cliRepository = "repository"
)

// testS3Config is the canonical S3 connection input used by every test.
// Centralized so a future reshape of S3Config (added field, renamed knob)
// is a single-line change instead of N table edits.
//
// v3.1.0: credentials are no longer part of S3Config; testCreds() supplies
// them via a StaticCredentialsSource the constructor takes separately.
func testS3Config() S3Config {
	return S3Config{
		Endpoint:   "http://192.168.10.133:30293",
		Bucket:     "volsync-kopia",
		DisableTLS: true,
	}
}

// testCreds returns a StaticCredentialsSource that always loads cleanly,
// so the existing connect / list tests don't have to worry about cred
// readiness — just like the pre-v3.1.0 client took creds via constructor.
func testCreds() CredentialsSource {
	return NewStaticCredentialsSource(testPassword, testAccessKey, testSecretKey)
}

// mockExecutor implements CommandExecutor for testing. lastArgs / lastName
// captures the most recent call so connect-flag assertions don't need a
// dedicated wrapper.
type mockExecutor struct {
	output   []byte
	err      error
	lastName string
	lastArgs []string

	// callCount counts how many times Run was invoked across the whole
	// test lifetime. Used by HealthCheck tests to assert subprocess
	// invocation behavior.
	callCount atomic.Int64
}

func (m *mockExecutor) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	m.callCount.Add(1)
	m.lastName = name
	m.lastArgs = append([]string(nil), args...)
	return m.output, m.err
}

func TestNewClient(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	client := NewClient(testS3Config(), testCreds(), logger, Options{})

	if client == nil {
		t.Fatal("NewClient returned nil")
	}
	if client.cfg.Bucket != "volsync-kopia" {
		t.Errorf("cfg.Bucket = %v, want volsync-kopia", client.cfg.Bucket)
	}
	if client.connected {
		t.Error("client should not be connected initially")
	}
	if client.connectTimeout != 60*time.Second {
		t.Errorf("connectTimeout default = %v, want 60s", client.connectTimeout)
	}
}

// TestNewClient_OptionsConnectTimeoutOverride pins the override path of the
// Options struct: a non-zero value replaces the 60s default.
func TestNewClient_OptionsConnectTimeoutOverride(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client := NewClient(testS3Config(), testCreds(), logger, Options{ConnectTimeout: 5 * time.Second})
	if client.connectTimeout != 5*time.Second {
		t.Errorf("connectTimeout = %v, want 5s", client.connectTimeout)
	}
}

func TestConnect_Success(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{
		output: []byte("Connected to repository."),
		err:    nil,
	}

	client := NewClientWithExecutor(testS3Config(), testCreds(), logger, mock, Options{})

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
	wantPrefix := []string{cliRepository, "connect", "s3"}
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
	// Pin that the lazy-loaded creds end up on the kopia argv.
	if !slices.Contains(mock.lastArgs, testPassword) {
		t.Errorf("executor args missing kopia password; got %v", mock.lastArgs)
	}
	if !slices.Contains(mock.lastArgs, testAccessKey) {
		t.Errorf("executor args missing access key; got %v", mock.lastArgs)
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
	client := NewClientWithExecutor(cfg, testCreds(), logger, mock, Options{})

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

	client := NewClientWithExecutor(testS3Config(), testCreds(), logger, mock, Options{})

	err := client.Connect(context.Background())
	if err == nil {
		t.Error("Connect() should have returned an error")
	}

	if client.connected {
		t.Error("client should not be connected after failed Connect()")
	}
}

// TestConnect_RetriesOnCredentialsNotReady pins the v3.1.0 backoff loop:
// when the credentials source returns ErrCredentialsNotReady on the first
// few calls and then succeeds, Connect() should keep retrying until the
// creds appear and finally invoke the kopia subprocess.
func TestConnect_RetriesOnCredentialsNotReady(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	creds := &flakyCreds{readyAfter: 3, ready: testCreds()}
	mock := &mockExecutor{output: []byte("Connected to repository."), err: nil}

	// Tight timeout because the test exercises rapid retries; 250ms initial
	// backoff doubles to 500ms and 1s — well within 5s budget.
	client := NewClientWithExecutor(testS3Config(), creds, logger, mock, Options{ConnectTimeout: 5 * time.Second})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v, want nil after creds-not-ready retries", err)
	}
	if !client.IsConnected() {
		t.Error("client should be connected after retried success")
	}
	if creds.calls != 3 {
		t.Errorf("creds.Load() call count = %d, want 3 (2 not-ready + 1 ready)", creds.calls)
	}
	// Exactly one kopia subprocess call — the not-ready iterations skip
	// the executor entirely.
	if got := mock.callCount.Load(); got != 1 {
		t.Errorf("executor invocations = %d, want 1", got)
	}
}

// TestConnect_TimesOutOnPersistentNotReady pins the deadline guard: when
// the creds NEVER become ready, Connect must return an error within roughly
// the configured ConnectTimeout rather than spinning forever.
func TestConnect_TimesOutOnPersistentNotReady(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	creds := &flakyCreds{readyAfter: 9999} // never becomes ready
	mock := &mockExecutor{output: []byte("Connected to repository."), err: nil}

	timeout := 600 * time.Millisecond
	client := NewClientWithExecutor(testS3Config(), creds, logger, mock, Options{ConnectTimeout: timeout})

	start := time.Now()
	err := client.Connect(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Connect() must return an error when credentials never become ready")
	}
	if !errors.Is(err, ErrCredentialsNotReady) {
		t.Errorf("error chain should include ErrCredentialsNotReady; got %v", err)
	}
	if elapsed < timeout {
		t.Errorf("Connect returned in %v, want at least the configured ConnectTimeout %v", elapsed, timeout)
	}
	// Generous upper bound to keep the test fast even on a slow CI box.
	if elapsed > 4*timeout {
		t.Errorf("Connect took %v, much longer than 4× ConnectTimeout %v — backoff loop runaway?", elapsed, timeout)
	}
	if client.IsConnected() {
		t.Error("client must not be marked connected after timeout")
	}
	if got := mock.callCount.Load(); got != 0 {
		t.Errorf("executor must not be invoked when creds never ready; got %d calls", got)
	}
}

// TestConnect_HardErrorPropagates pins that any error class OTHER than
// ErrCredentialsNotReady from the creds source short-circuits the retry
// loop. We don't want a permanent misconfiguration (e.g. an invalid path
// type) to be silently retried for a full minute.
func TestConnect_HardErrorPropagates(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	creds := &errorCreds{err: errors.New("credentials malformed")}
	mock := &mockExecutor{}
	client := NewClientWithExecutor(testS3Config(), creds, logger, mock, Options{ConnectTimeout: 60 * time.Second})

	err := client.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect() should have returned an error")
	}
	if errors.Is(err, ErrCredentialsNotReady) {
		t.Errorf("hard creds error must NOT be wrapped as ErrCredentialsNotReady; got %v", err)
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

	client := NewClientWithExecutor(testS3Config(), testCreds(), logger, mock, Options{})

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

	client := NewClientWithExecutor(testS3Config(), testCreds(), logger, mock, Options{})

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

	client := NewClientWithExecutor(testS3Config(), testCreds(), logger, mock, Options{})

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

	client := NewClientWithExecutor(testS3Config(), testCreds(), logger, mock, Options{})

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

	client := NewClient(testS3Config(), testCreds(), logger, Options{})

	err := client.HealthCheck(context.Background())
	if err == nil {
		t.Error("HealthCheck should fail when client is not connected")
	}
}

// TestHealthCheck_Success verifies that once Connect() has marked the client
// connected AND `kopia repository status` succeeds, the readiness probe
// returns nil. v3.1.0 made HealthCheck genuinely probe the repo via
// `kopia repository status` so /readyz reflects current usability rather
// than just the startup connect outcome.
func TestHealthCheck_Success(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{output: []byte("Connected to repository."), err: nil}
	client := NewClientWithExecutor(testS3Config(), testCreds(), logger, mock, Options{})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}

	// HealthCheck spawns `kopia repository status` — the same mock returns
	// success on every call so the probe sees a healthy repo.
	if err := client.HealthCheck(context.Background()); err != nil {
		t.Errorf("HealthCheck() error = %v, want nil", err)
	}
	if got := mock.lastArgs; len(got) < 2 || got[0] != cliRepository || got[1] != "status" {
		t.Errorf("HealthCheck must invoke `kopia repository status`; got args %v", got)
	}
}

// TestHealthCheck_StatusFailureFailsReadiness pins the v3.1.0 behavior
// change: when `kopia repository status` errors out, HealthCheck returns a
// non-nil error so the kubelet marks the pod not-Ready and stops routing
// admission webhook traffic. This is what gates failurePolicy=Fail PVC
// admission against an operator pod whose kopia connection has silently
// broken (creds rotated, on-disk session expired, S3 endpoint unreachable).
func TestHealthCheck_StatusFailureFailsReadiness(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{output: []byte("ok"), err: nil}
	client := NewClientWithExecutor(testS3Config(), testCreds(), logger, mock, Options{})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}

	// Swap in a status-failing executor — Connect already succeeded, so
	// the connected flag is true; the failure must come from the actual
	// `kopia repository status` invocation, not the gate.
	statusFail := &mockExecutor{output: []byte("not connected"), err: errors.New("exit status 1")}
	client.executor = statusFail

	if err := client.HealthCheck(context.Background()); err == nil {
		t.Error("HealthCheck() must fail when `kopia repository status` errors out")
	}
}

func TestCheckBackupExists_Integration(t *testing.T) {
	// Skip in CI - this test requires a real kopia repository
	t.Skip("Integration test - requires kopia repository")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	client := NewClient(testS3Config(), testCreds(), logger, Options{})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	result := client.CheckBackupExists(context.Background(), "test-ns", "test-pvc")
	t.Logf("Result: exists=%v, namespace=%s, pvc=%s, backend=%s, error=%s",
		result.Exists, result.Namespace, result.Pvc, result.Backend, result.Error)
}

// TestDirCredentialsSource_LoadsAllThreeFiles pins the directory-Secret
// loading shape: each Secret key becomes a separate file, the loader reads
// them, and trailing whitespace is trimmed (kubectl create secret leaves a
// trailing newline on each value).
func TestDirCredentialsSource_LoadsAllThreeFiles(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "KOPIA_PASSWORD"), "secret-pass\n")
	mustWrite(t, filepath.Join(dir, "AWS_ACCESS_KEY_ID"), "ak-123\n")
	mustWrite(t, filepath.Join(dir, "AWS_SECRET_ACCESS_KEY"), "sk-456\n")

	src := NewDirCredentialsSource(dir)
	got, err := src.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Password != "secret-pass" {
		t.Errorf("Password = %q, want %q", got.Password, "secret-pass")
	}
	if got.AccessKey != "ak-123" {
		t.Errorf("AccessKey = %q, want %q", got.AccessKey, "ak-123")
	}
	if got.SecretKey != "sk-456" {
		t.Errorf("SecretKey = %q, want %q", got.SecretKey, "sk-456")
	}
}

// TestDirCredentialsSource_MissingFileIsNotReady pins the headline guarantee
// of the v3.1.0 ES-race fix: a missing Secret file is reported as
// ErrCredentialsNotReady (NOT a generic error), so the retry loop in
// Connect() backs off rather than crashing the pod.
func TestDirCredentialsSource_MissingFileIsNotReady(t *testing.T) {
	dir := t.TempDir()
	// Only write 2 of 3 files.
	mustWrite(t, filepath.Join(dir, "KOPIA_PASSWORD"), "p\n")
	mustWrite(t, filepath.Join(dir, "AWS_ACCESS_KEY_ID"), "ak\n")
	// AWS_SECRET_ACCESS_KEY missing.

	src := NewDirCredentialsSource(dir)
	_, err := src.Load()
	if err == nil {
		t.Fatal("Load() must error when a credential file is missing")
	}
	if !errors.Is(err, ErrCredentialsNotReady) {
		t.Errorf("missing file error must wrap ErrCredentialsNotReady; got %v", err)
	}
}

// TestDirCredentialsSource_EmptyFileIsNotReady pins that a zero-byte file
// (kubelet wrote the file but the Secret key was empty) is treated the
// same as a missing file — both surface as not-ready, both retry.
func TestDirCredentialsSource_EmptyFileIsNotReady(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "KOPIA_PASSWORD"), "")
	mustWrite(t, filepath.Join(dir, "AWS_ACCESS_KEY_ID"), "ak")
	mustWrite(t, filepath.Join(dir, "AWS_SECRET_ACCESS_KEY"), "sk")

	src := NewDirCredentialsSource(dir)
	_, err := src.Load()
	if err == nil {
		t.Fatal("Load() must error on empty credential file")
	}
	if !errors.Is(err, ErrCredentialsNotReady) {
		t.Errorf("empty file error must wrap ErrCredentialsNotReady; got %v", err)
	}
}

// TestDirCredentialsSource_EmptyDirIsNotReady pins the empty-config guard:
// an unset Dir surfaces as not-ready so an admin who fat-fingers
// KOPIA_CREDENTIALS_PATH=' ' doesn't get a crash, just a retry.
func TestDirCredentialsSource_EmptyDirIsNotReady(t *testing.T) {
	src := NewDirCredentialsSource("")
	_, err := src.Load()
	if err == nil {
		t.Fatal("Load() must error when Dir is empty")
	}
	if !errors.Is(err, ErrCredentialsNotReady) {
		t.Errorf("empty Dir error must wrap ErrCredentialsNotReady; got %v", err)
	}
}

// TestStaticCredentialsSource_HappyPath pins the legacy compatibility path
// — the v1.x HTTP-only deployment shape supplies creds via env vars, the
// loader trivially returns them.
func TestStaticCredentialsSource_HappyPath(t *testing.T) {
	src := NewStaticCredentialsSource("p", "ak", "sk")
	got, err := src.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got != (Creds{Password: "p", AccessKey: "ak", SecretKey: "sk"}) {
		t.Errorf("Load() returned %+v, want full creds", got)
	}
}

// TestStaticCredentialsSource_EmptyFieldIsNotReady ensures even the legacy
// path surfaces as not-ready when env vars weren't populated, so the retry
// loop in Connect() applies uniformly.
func TestStaticCredentialsSource_EmptyFieldIsNotReady(t *testing.T) {
	cases := []struct {
		name      string
		password  string
		accessKey string
		secretKey string
	}{
		{"missing password", "", "ak", "sk"},
		{"missing access key", "p", "", "sk"},
		{"missing secret key", "p", "ak", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := NewStaticCredentialsSource(tc.password, tc.accessKey, tc.secretKey)
			_, err := src.Load()
			if err == nil {
				t.Fatal("expected error on missing field")
			}
			if !errors.Is(err, ErrCredentialsNotReady) {
				t.Errorf("error must wrap ErrCredentialsNotReady; got %v", err)
			}
		})
	}
}

// TestConnect_DirCredentialsSource_AppearsLate is the integration-y
// scenario: the cred files don't exist yet at Connect() time but appear
// shortly after, simulating ESO catching up after an ArgoCD sync wave.
func TestConnect_DirCredentialsSource_AppearsLate(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	dir := t.TempDir()

	// Start the connect loop without the files; write them after a short
	// delay so we know the retry path is what unblocks Connect.
	src := NewDirCredentialsSource(dir)
	mock := &mockExecutor{output: []byte("Connected"), err: nil}
	client := NewClientWithExecutor(testS3Config(), src, logger, mock, Options{ConnectTimeout: 5 * time.Second})

	go func() {
		time.Sleep(400 * time.Millisecond)
		mustWrite(t, filepath.Join(dir, "KOPIA_PASSWORD"), "p\n")
		mustWrite(t, filepath.Join(dir, "AWS_ACCESS_KEY_ID"), "ak\n")
		mustWrite(t, filepath.Join(dir, "AWS_SECRET_ACCESS_KEY"), "sk\n")
	}()

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v, want creds-appear-late path to succeed", err)
	}
	if !client.IsConnected() {
		t.Error("client should be connected after creds appeared")
	}
}

// flakyCreds returns ErrCredentialsNotReady for the first `readyAfter-1`
// calls, then delegates to `ready` for subsequent calls. Used to exercise
// the v3.1.0 retry loop without timer-based fakes.
type flakyCreds struct {
	readyAfter int // call index (1-based) at which Load() finally succeeds
	calls      int
	ready      CredentialsSource
}

func (f *flakyCreds) Load() (Creds, error) {
	f.calls++
	if f.calls < f.readyAfter {
		return Creds{}, ErrCredentialsNotReady
	}
	if f.ready == nil {
		return Creds{}, ErrCredentialsNotReady
	}
	return f.ready.Load()
}

// errorCreds always returns a non-not-ready error, exercising the
// hard-error-propagates-immediately branch of Connect().
type errorCreds struct {
	err error
}

func (e *errorCreds) Load() (Creds, error) {
	return Creds{}, e.err
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
