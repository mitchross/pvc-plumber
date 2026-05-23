// Command operator is the controller-runtime entrypoint for pvc-plumber.
//
// As of v3.0.0 this binary wires the PVC reconciler and TWO admission
// webhook handlers (PVCMutator, PVCValidator) onto a real controller-runtime
// manager, sharing one in-process backend + cache instance with the legacy
// read-only HTTP server. The third handler (JobMutator) was removed in
// v3.0.0 — see CHANGELOG. The OPERATOR_MODE=true feature flag is the rollout
// gate — when unset, the binary behaves like the legacy cmd/pvc-plumber
// server (HTTP-only, no manager) so we can deploy this image as a drop-in
// before flipping the cluster onto the admission webhooks.
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
	"github.com/mitchross/pvc-plumber/internal/v4/auditclient"
	"github.com/mitchross/pvc-plumber/internal/v4/runtimeconfig"
	pvcwebhook "github.com/mitchross/pvc-plumber/internal/webhook"
)

// defaultSystemNamespaces is the canonical, deadlock-prevention namespace set
// that pvc-plumber must NOT process under any circumstance. These entries are
// always seeded into the SystemNamespaces set at startup; the
// SYSTEM_NAMESPACES env var only ADDS to this list (see parseSystemNamespaces),
// never replaces it. Drift between this list and the namespaceSelector NotIn
// list in `infrastructure/controllers/pvc-plumber/webhooks.yaml` is the actual
// cluster-safety bug — the env override is intentionally additive so an
// operator can add e.g. `staging-infra` without losing kube-system.
//
// Background: the 9 entries reflect the admission deadlock recovery path
// (origin: 2026-04-08 incident). pvc-plumber itself, plus every controller
// it depends on at startup (cert-manager, external-secrets, the secret store,
// snapshot-controller, longhorn, argocd, kyverno, kube-system controllers),
// must be able to create PVCs while the webhook is failurePolicy=Fail.
// Shrinking this list can wedge the cluster on bootstrap. The design doc and
// legacy Kyverno YAML have shorter lists — both are stale; this is the canon.
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

// scheme is the runtime scheme the manager + admission decoder use.
// Phase 1 had a `setupLog` here too but the operator now logs through slog
// for both the legacy HTTP path and the manager startup, so the named
// controller-runtime logger has no callers.
var scheme = runtime.NewScheme()

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

	// Phase 2.5d: resolve runtime mode (PVC_PLUMBER_MODE → audit default)
	// BEFORE config.Load() so that audit-mode startup can skip backend
	// validation. The audit-mode binary must NOT depend on RustFS / Kopia
	// / S3 / credential env vars to come up — config.LoadWithOptions
	// (SkipBackend=true) is the seam.
	runtimeCfg, runtimeErr := runtimeconfig.Load()

	// slog drives the legacy HTTP server (cmd/pvc-plumber/main.go uses it);
	// reuse the same JSON format and level resolution so logs look identical
	// whether the operator binary or the legacy binary is running.
	cfg, err := config.LoadWithOptions(config.LoadOptions{
		SkipBackend: !runtimeCfg.WritesAllowed(),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}
	slogger := buildSlogger(cfg.LogLevel)
	slog.SetDefault(slogger)

	// Operator-specific env. v3.0.0 removed NFS_SERVER / NFS_PATH along with
	// the JobMutator that consumed them — the operator no longer reaches into
	// any shared filesystem mount, S3-backed Kopia handles everything.
	sysNs := parseSystemNamespaces(os.Getenv("SYSTEM_NAMESPACES"))
	operatorMode := os.Getenv("OPERATOR_MODE") == "true"

	// Now that the slogger is up, surface any runtimeconfig warning + the
	// banner. The Banner() goes first so it's the LITERAL first audit
	// signal — log scrapers expect to see it before any other line in
	// audit-mode pods.
	if runtimeErr != nil {
		slogger.Warn("runtime config load returned a warning",
			"error", runtimeErr,
			"env_key", runtimeconfig.EnvKey,
			"raw_value", runtimeCfg.RawModeValue,
			"effective_mode", runtimeCfg.Mode.String(),
			"mode_source", runtimeCfg.ModeSource.String(),
		)
	}
	slogger.Info(runtimeCfg.Banner())

	slogger.Info("starting pvc-plumber operator",
		"backend", cfg.BackendType,
		"port", cfg.Port,
		"log_level", cfg.LogLevel,
		"operator_mode", operatorMode,
		"pvc_plumber_mode", runtimeCfg.Mode.String(),
		"mode_source", runtimeCfg.ModeSource.String(),
		"writes_allowed", runtimeCfg.WritesAllowed(),
		"webhooks_will_register", runtimeCfg.WebhookRegistrationAllowed(),
		"system_namespaces", systemNamespacesForLog(sysNs),
	)

	// signal.NotifyContext gives us a single ctx whose cancellation
	// propagates to BOTH the manager and the HTTP server. Using
	// ctrl.SetupSignalHandler() *and* signal.Notify in the same process
	// would race for SIGTERM and only one of them would clean up properly.
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Backend + cache + HTTP server are conditional. In audit mode the
	// binary must NOT initialize Kopia / S3 / kopia-credentials, must NOT
	// connect to RustFS, and must NOT serve the legacy /exists endpoint —
	// the audit-mode promise is "binary starts cleanly even when the
	// backup infrastructure is unreachable." Phase 2.5d of the v4 PRD.
	//
	// Anything that depends on `bundle` (HTTP server, cache re-warm loop,
	// webhook handlers in runManager) is gated on the same condition.
	var bundle *backendBundle
	if runtimeCfg.WritesAllowed() {
		bundle, err = buildBackend(rootCtx, cfg, slogger)
		if err != nil {
			slogger.Error("backend init failed", "error", err)
			os.Exit(1)
		}
	} else {
		slogger.Info("audit mode: skipping backend init (Kopia / S3 / credentials)",
			"mode", runtimeCfg.Mode.String())
	}

	// errgroup collects errors from any subsystem. ctx derives from
	// rootCtx; if any goroutine returns non-nil, ctx cancels and the rest
	// shut down. mgr.Start respects that ctx; http.Server respects it via
	// the shutdown goroutine pattern below.
	g, gctx := errgroup.WithContext(rootCtx)

	// 1. Legacy HTTP server (/exists, /metrics) — only when a backend is
	//    initialized. In audit mode this is intentionally OFF; the
	//    controller-runtime manager still serves its own /metrics and
	//    healthz on :8081 / :8082 so observability isn't lost.
	if bundle != nil {
		httpSrv := newHTTPServer(cfg, bundle, slogger)
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

		// 2. Cache re-warm loop (kopia-s3 only). Identical cadence to the
		//    legacy binary; ctx cancellation stops it within one tick.
		if bundle.kopia != nil && cfg.ReWarmInterval > 0 {
			g.Go(func() error {
				runCacheReWarmLoop(gctx, bundle.kopia, bundle.cached, cfg.ReWarmInterval, slogger)
				return nil
			})
		}
	} else {
		slogger.Info("audit mode: HTTP server + cache re-warm loop NOT started")
	}

	// 3. controller-runtime manager — only when OPERATOR_MODE=true. This
	//    is the rollout gate: a deployment can ship the operator image
	//    with OPERATOR_MODE unset to verify the legacy HTTP path stays
	//    healthy, then flip the env var to enable webhooks + reconciler
	//    without a separate image. In audit mode the manager runs but
	//    its webhook handlers + write paths are no-ops via the
	//    auditclient wrapper and the runtimeCfg gating.
	if operatorMode {
		g.Go(func() error {
			return runManager(
				gctx,
				slogger,
				bundle,
				cfg,
				runtimeCfg,
				sysNs,
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
// reconciler, wires the two PVC admission webhook handlers against the
// shared backend, and blocks on mgr.Start until ctx cancels. Errors are
// returned so the parent errgroup can drive the unified shutdown.
//
// v3.0.0: the third handler (JobMutator at /mutate-batch-v1-job) is gone
// permanently. With the operator's Kopia repo on S3, mover Jobs no longer
// need a shared volume injected at admission time — the volume-injection
// approach was incompatible with VolSync's CreateOrUpdateDeleteOnImmutableErr
// reconciler and caused a cluster-wide backup outage on 2026-05-08.
//
// Note: the manager's metrics + probe addresses are distinct from the
// legacy HTTP server's port (cfg.Port) — running both on the same port
// would clash. Operators see the controller-runtime metrics on
// metricsAddr and the legacy /metrics on cfg.Port.
func runManager(
	ctx context.Context,
	slogger *slog.Logger,
	bundle *backendBundle,
	cfg *config.Config,
	runtimeCfg runtimeconfig.Config,
	sysNs map[string]struct{},
	metricsAddr, probeAddr string,
	webhookPort int, webhookCertDir string,
	enableLeaderElection bool, leaderElectionID string,
) error {
	// Phase 2.5: leader election uses a coordination.k8s.io/v1 Lease that
	// is written by a path SEPARATE from mgr.GetClient(). The auditclient
	// wrapper cannot gate those writes, so we disable leader election
	// outright in audit mode to honor the "no cluster writes" contract.
	// Audit-mode deployments are typically single-replica anyway; HA + leader
	// election are a Phase 8 concern when the operator goes to enforce/strict.
	if !runtimeCfg.WritesAllowed() && enableLeaderElection {
		slogger.Info("audit mode: forcing --leader-elect=false to keep Lease writes off the cluster",
			"mode", runtimeCfg.Mode.String())
		enableLeaderElection = false
	}

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
	// webhooks for the actual restore decisions. The ExternalSecretConfig
	// is plumbed through so the per-PVC `volsync-<pvc>` ES the reconciler
	// renders points at the right ClusterSecretStore / vault item /
	// property names for this deployment (defaults pin to the reference
	// cluster's 1Password Connect layout — see internal/config).
	//
	// Phase 2.5: wrap mgr.GetClient() with auditclient so every write
	// the reconciler attempts is gated by runtimeCfg.Mode. In audit mode
	// the wrapper logs "would-write" and returns nil without touching the
	// cluster — see internal/v4/auditclient.
	reconcilerClient := auditclient.New(mgr.GetClient(), runtimeCfg.Mode, slogger)
	if err := (&controller.PVCReconciler{
		Client:           reconcilerClient,
		SystemNamespaces: sysNs,
		ExternalSecret: controller.ExternalSecretConfig{
			SecretStoreName:       cfg.ExternalSecretsStoreName,
			VaultKey:              cfg.ExternalSecretsVaultKey,
			KopiaPasswordProperty: cfg.ExternalSecretsKopiaPasswordProperty,
			S3AccessKeyProperty:   cfg.ExternalSecretsS3AccessKeyProperty,
			S3SecretKeyProperty:   cfg.ExternalSecretsS3SecretKeyProperty,
			S3Endpoint:            cfg.KopiaS3Endpoint,
			S3Bucket:              cfg.KopiaS3Bucket,
			S3DisableTLS:          cfg.KopiaS3DisableTLS,
		},
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup PVCReconciler: %w", err)
	}

	// Admission webhook handlers. The decoder is built from the manager's
	// scheme so it knows how to deserialize core/v1 PVC payloads. Both
	// handlers receive the same SystemNamespaces / shared cached backend,
	// keeping behavior identical between reconcile and admission paths.
	//
	// Phase 2.5: skip registration entirely in audit mode. The binary is
	// safe to deploy without a MutatingWebhookConfiguration /
	// ValidatingWebhookConfiguration; if one is accidentally created
	// against this audit-mode binary, the webhook server returns 404 for
	// the unregistered routes so admission requests simply fall through
	// to "not handled by this webhook" (kube-apiserver behavior depends on
	// failurePolicy — but the binary itself contributes zero denials).
	if runtimeCfg.WebhookRegistrationAllowed() {
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
		slogger.Info("admission webhooks registered",
			"mode", runtimeCfg.Mode.String(),
			"paths", []string{"/mutate-v1-pvc", "/validate-v1-pvc"})
	} else {
		slogger.Info("admission webhooks NOT registered (audit mode)",
			"mode", runtimeCfg.Mode.String())
	}

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

// parseSystemNamespaces builds the SystemNamespaces set: ALWAYS the 9
// defaults, PLUS any extras supplied via the SYSTEM_NAMESPACES env var
// (comma-separated). Whitespace-only entries are dropped. The defaults are
// load-bearing for cluster safety (see defaultSystemNamespaces) and must
// never be lost — the env var is purely additive so an operator can extend
// the exclusion list with site-local namespaces (e.g. `staging-infra`)
// without accidentally re-enabling reconciliation in kube-system.
func parseSystemNamespaces(raw string) map[string]struct{} {
	out := make(map[string]struct{}, len(defaultSystemNamespaces)+8)
	// Always seed the deadlock-prevention defaults first.
	for _, ns := range defaultSystemNamespaces {
		out[ns] = struct{}{}
	}
	if strings.TrimSpace(raw) == "" {
		return out
	}
	for _, ns := range strings.Split(raw, ",") {
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
