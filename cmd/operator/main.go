// Command operator is the controller-runtime entrypoint for pvc-plumber v2.
//
// Phase 3: this binary now wires the PVC reconciler and three admission
// webhook handlers (PVCMutator, PVCValidator, JobMutator) onto a real
// controller-runtime manager, sharing one in-process backend + cache
// instance with the legacy read-only HTTP server. The OPERATOR_MODE=true
// feature flag is the rollout gate — when unset, the binary behaves like
// the legacy cmd/pvc-plumber server (HTTP-only, no manager) so we can
// deploy this image as a drop-in before flipping the cluster onto the
// admission webhooks.
//
// Lifecycle: signal.NotifyContext drives a single cancellation context.
// When SIGTERM/SIGINT lands, the HTTP server, cache re-warm loop, and
// manager all shut down through that same ctx via errgroup; whichever
// subsystem exits first cancels the rest.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/mitchross/pvc-plumber/internal/config"
	"github.com/mitchross/pvc-plumber/internal/controller"
	pvcwebhook "github.com/mitchross/pvc-plumber/internal/webhook"
)

// defaultSystemNamespaces is the canonical 9-entry exclusion list. This
// MUST stay in sync with the namespaceSelector in
// `infrastructure/controllers/pvc-plumber/webhooks.yaml` — any namespace
// listed there as `NotIn` for the webhook configurations must also be
// excluded from the reconciler, otherwise the operator could try to
// reconcile a PVC the webhook never gates and end up creating ES/RS/RD
// objects in places that should be off-limits (e.g. cert-manager creating
// a backup-labeled PVC for its own internal use would deadlock).
//
// The design doc lists 5 entries and the legacy Kyverno YAML lists 3. The
// 9-entry list is the authoritative one because admission deadlock
// recovery (the original 2026-04-08 incident) requires that pvc-plumber
// itself, plus every controller it depends on at startup, can create PVCs
// even when the webhook is failurePolicy=Fail.
var defaultSystemNamespaces = []string{
	"kube-system",
	"volsync-system",
	"kyverno",
	"argocd",
	"longhorn-system",
	"snapshot-controller",
	"cert-manager",
	"external-secrets",
	"1passwordconnect",
}

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr          string
		probeAddr            string
		webhookPort          int
		webhookCertDir       string
		enableLeaderElection bool
		leaderElectionID     string
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8081",
		"The address the controller-runtime metrics endpoint binds to. Distinct from the HTTP server's /metrics on cfg.Port.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8082",
		"The address the manager's healthz/readyz probe endpoint binds to. Distinct from the HTTP server's probes on cfg.Port.")
	flag.IntVar(&webhookPort, "webhook-port", 9443,
		"The TCP port the webhook server listens on for TLS admission requests.")
	flag.StringVar(&webhookCertDir, "webhook-cert-dir",
		"/tmp/k8s-webhook-server/serving-certs",
		"Directory containing tls.crt + tls.key for the webhook server. cert-manager mounts this in production.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager. Required for replicas>1; safe to leave on for replicas=1.")
	flag.StringVar(&leaderElectionID, "leader-election-id",
		"pvc-plumber.mitchross.github.io",
		"Resource lock name for leader election. Stable; do not rename without coordinating a clean cutover.")

	zapOpts := zap.Options{Development: false}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	// slog drives the legacy HTTP server (cmd/pvc-plumber/main.go uses it);
	// reuse the same JSON format and level resolution so logs look identical
	// whether the operator binary or the legacy binary is running.
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}
	slogger := buildSlogger(cfg.LogLevel)
	slog.SetDefault(slogger)

	// Operator-specific env. The defaults below match the production cluster
	// (NFS_SERVER, NFS_PATH) and the canonical 9-entry exclusion list.
	nfsServer := envOr("NFS_SERVER", "192.168.10.133")
	nfsPath := envOr("NFS_PATH", "/mnt/BigTank/k8s/volsync-kopia-nfs")
	sysNs := parseSystemNamespaces(os.Getenv("SYSTEM_NAMESPACES"))
	operatorMode := os.Getenv("OPERATOR_MODE") == "true"

	slogger.Info("starting pvc-plumber operator",
		"backend", cfg.BackendType,
		"port", cfg.Port,
		"log_level", cfg.LogLevel,
		"operator_mode", operatorMode,
		"nfs_server", nfsServer,
		"nfs_path", nfsPath,
		"system_namespaces", systemNamespacesForLog(sysNs),
	)

	// signal.NotifyContext gives us a single ctx whose cancellation
	// propagates to BOTH the manager and the HTTP server. Using
	// ctrl.SetupSignalHandler() *and* signal.Notify in the same process
	// would race for SIGTERM and only one of them would clean up properly.
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Backend + cache: shared between HTTP server and webhook handlers so
	// there's exactly one Kopia connection and one cache. This is the
	// "shared kopia client" the conductor's brief asked for; the cached
	// wrapper satisfies the webhook package's narrow `kopiaClient`
	// interface (CheckBackupExists only).
	bundle, err := buildBackend(rootCtx, cfg, slogger)
	if err != nil {
		slogger.Error("backend init failed", "error", err)
		os.Exit(1)
	}

	httpSrv := newHTTPServer(cfg, bundle, slogger)

	// errgroup collects errors from any subsystem. ctx derives from
	// rootCtx; if any goroutine returns non-nil, ctx cancels and the rest
	// shut down. mgr.Start respects that ctx; http.Server respects it via
	// the shutdown goroutine pattern below.
	g, gctx := errgroup.WithContext(rootCtx)

	// 1. HTTP server (always on — even when OPERATOR_MODE=false this
	//    binary keeps serving the legacy /exists/ surface, which is how
	//    the cluster does the rolling cutover).
	g.Go(func() error {
		slogger.Info("http server starting", "addr", httpSrv.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		slogger.Info("http server shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
		return nil
	})

	// 2. Cache re-warm loop (kopia-fs only). Identical cadence to the
	//    legacy binary; ctx cancellation stops it within one tick.
	if bundle.kopia != nil && cfg.ReWarmInterval > 0 {
		g.Go(func() error {
			runCacheReWarmLoop(gctx, bundle.kopia, bundle.cached, cfg.ReWarmInterval, slogger)
			return nil
		})
	}

	// 3. controller-runtime manager — only when OPERATOR_MODE=true. This
	//    is the rollout gate: a deployment can ship the operator image
	//    with OPERATOR_MODE unset to verify the legacy HTTP path stays
	//    healthy, then flip the env var to enable webhooks + reconciler
	//    without a separate image.
	if operatorMode {
		g.Go(func() error {
			return runManager(
				gctx,
				slogger,
				bundle,
				sysNs,
				nfsServer,
				nfsPath,
				metricsAddr, probeAddr,
				webhookPort, webhookCertDir,
				enableLeaderElection, leaderElectionID,
			)
		})
	} else {
		slogger.Info("OPERATOR_MODE=false; manager + webhooks DISABLED, http server only")
	}

	if err := g.Wait(); err != nil {
		slogger.Error("operator exiting with error", "error", err)
		os.Exit(1)
	}
	slogger.Info("operator stopped cleanly")
}

// runManager builds the controller-runtime manager, registers the PVC
// reconciler, wires the three admission webhook handlers against the
// shared backend, and blocks on mgr.Start until ctx cancels. Errors are
// returned so the parent errgroup can drive the unified shutdown.
//
// Note: the manager's metrics + probe addresses are distinct from the
// legacy HTTP server's port (cfg.Port) — running both on the same port
// would clash. Operators see the controller-runtime metrics on
// metricsAddr and the legacy /metrics on cfg.Port.
func runManager(
	ctx context.Context,
	slogger *slog.Logger,
	bundle *backendBundle,
	sysNs map[string]struct{},
	nfsServer, nfsPath string,
	metricsAddr, probeAddr string,
	webhookPort int, webhookCertDir string,
	enableLeaderElection bool, leaderElectionID string,
) error {
	slogger.Info("manager starting",
		"metrics", metricsAddr,
		"probes", probeAddr,
		"webhook_port", webhookPort,
		"leader_election", enableLeaderElection,
	)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), manager.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		WebhookServer: webhook.NewServer(webhook.Options{
			Port:    webhookPort,
			CertDir: webhookCertDir,
		}),
		LeaderElection:   enableLeaderElection,
		LeaderElectionID: leaderElectionID,
	})
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}

	// PVC reconciler. The reconciler intentionally does NOT take a Kopia
	// client — it only manages ES/RS/RD lifecycle and trusts admission
	// webhooks for the actual restore decisions.
	if err := (&controller.PVCReconciler{
		Client:           mgr.GetClient(),
		SystemNamespaces: sysNs,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup PVCReconciler: %w", err)
	}

	// Admission webhook handlers. The decoder is built from the manager's
	// scheme so it knows how to deserialize core/v1 PVC and batch/v1 Job
	// payloads. All three handlers receive the same SystemNamespaces /
	// shared cached backend, keeping behaviour identical between
	// reconcile and admission paths.
	decoder := admission.NewDecoder(mgr.GetScheme())
	hookSrv := mgr.GetWebhookServer()
	hookSrv.Register("/mutate-v1-pvc", &webhook.Admission{
		Handler: &pvcwebhook.PVCMutator{
			Decoder:          decoder,
			Kopia:            bundle.cached,
			SystemNamespaces: sysNs,
		},
	})
	hookSrv.Register("/validate-v1-pvc", &webhook.Admission{
		Handler: &pvcwebhook.PVCValidator{
			Decoder:          decoder,
			Kopia:            bundle.cached,
			SystemNamespaces: sysNs,
		},
	})
	hookSrv.Register("/mutate-batch-v1-job", &webhook.Admission{
		Handler: &pvcwebhook.JobMutator{
			Decoder:   decoder,
			NFSServer: nfsServer,
			NFSPath:   nfsPath,
		},
	})

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("add healthz: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("add readyz: %w", err)
	}

	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("manager run: %w", err)
	}
	return nil
}

// envOr returns the env var value when set non-empty, otherwise fallback.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// parseSystemNamespaces converts a comma-separated env value into a set.
// Empty / whitespace-only entries are dropped. When the input is empty the
// 9-entry defaultSystemNamespaces is used so a misconfigured Deployment
// still excludes the deadlock-critical namespaces.
func parseSystemNamespaces(raw string) map[string]struct{} {
	out := make(map[string]struct{})
	source := defaultSystemNamespaces
	if strings.TrimSpace(raw) != "" {
		source = strings.Split(raw, ",")
	}
	for _, ns := range source {
		if trimmed := strings.TrimSpace(ns); trimmed != "" {
			out[trimmed] = struct{}{}
		}
	}
	return out
}

// systemNamespacesForLog flattens the set back to a sorted-ish slice for
// human-readable startup logging. Map iteration is unordered, but for a
// boot log line we don't care about determinism.
func systemNamespacesForLog(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for ns := range set {
		out = append(out, ns)
	}
	return out
}

// buildSlogger mirrors cmd/pvc-plumber/main.go's logger construction so
// log output looks identical between the legacy and operator binaries.
func buildSlogger(level string) *slog.Logger {
	var lv slog.Level
	switch level {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lv}))
}
