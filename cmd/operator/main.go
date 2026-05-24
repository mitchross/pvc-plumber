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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/mitchross/pvc-plumber/internal/config"
	"github.com/mitchross/pvc-plumber/internal/controller"
	"github.com/mitchross/pvc-plumber/internal/v4/auditclient"
	"github.com/mitchross/pvc-plumber/internal/v4/mode"
	"github.com/mitchross/pvc-plumber/internal/v4/naming"
	"github.com/mitchross/pvc-plumber/internal/v4/runtimeconfig"
	pvcwebhook "github.com/mitchross/pvc-plumber/internal/webhook"
)

// Reconciler-selection sentinels. Exported as constants (lowercase OK
// since they don't leave package main) so wiring tests can assert the
// exact string runManager would pick for a given mode without standing
// up a manager.
//
// Patch 6.7-wire renamed reconcilerKindV4Audit → reconcilerKindV4 to
// match the new routing: permissive joins audit on the v4 reconciler
// path, so a name that pins the kind to one mode is misleading.
const (
	reconcilerKindV3 = "v3"
	reconcilerKindV4 = "v4"
)

// reconcilerKindFor is the single source of truth for "which reconciler
// runs in this mode." Audit AND permissive route to the v4 reconciler
// + executor pair (the executor's Mode-gated short-circuit keeps audit
// observe-only); enforce and strict are rejected at startup by
// validateMode and never reach this predicate, but if they ever do
// they fall to v3 as a defensive default.
//
// Tested in main_test.go so a future mode addition forces an explicit
// decision.
func reconcilerKindFor(m mode.Mode) string {
	if runsV4Reconciler(m) {
		return reconcilerKindV4
	}
	return reconcilerKindV3
}

// runsV4Reconciler reports whether the operator binary routes the
// given mode to the v4 reconciler. Audit and permissive both run v4
// (their executor behavior diverges via the reconciler's Mode field —
// audit short-circuits, permissive applies the planner's ops with
// ownership and GVK safety rails). Patch 6.7-wire is the patch that
// added permissive to this set.
//
// Enforce and strict return false here, but in practice they never
// reach this predicate — validateMode aborts the binary at startup
// for those modes. The false return is the defensive fallback in
// case validateMode is ever bypassed.
func runsV4Reconciler(m mode.Mode) bool {
	return m == mode.Audit || m == mode.Permissive
}

// needsBackend reports whether the operator binary must initialize
// the Kopia/S3 backend bundle (legacy v3 reconciler dependency). The
// v4 reconciler/executor only manages VolSync RS/RD via the embedded
// controller-runtime client; it does not inspect Kopia, S3, or
// RustFS, so v4-routed modes (audit + permissive) skip backend init
// entirely. Currently the inverse of runsV4Reconciler — they would
// diverge if a future v4 component needs a backend hook, hence the
// separate predicate.
func needsBackend(m mode.Mode) bool {
	return !runsV4Reconciler(m)
}

// validateMode fails loudly for modes the operator binary cannot
// honor today. Enforce and strict are reserved for Phase 8
// admission/restore semantics; routing them to v3 would resurrect
// the chart-era reconciler (forbidden by the Patch 6.7-wire scope),
// and routing them to v4 with permissive-equivalent writes would
// silently downgrade their stronger contract. The correct behavior
// is fail-fast: crash the pod at startup with an unambiguous error
// so the operator who set PVC_PLUMBER_MODE=enforce/strict sees
// "this mode is not implemented yet" rather than a permissive-style
// reconcile log.
//
// Returns nil for audit and permissive (the two supported v4-routed
// modes) and an error wrapping the mode string otherwise.
func validateMode(m mode.Mode) error {
	switch m {
	case mode.Audit, mode.Permissive:
		return nil
	case mode.Enforce, mode.Strict:
		return fmt.Errorf("PVC_PLUMBER_MODE=%s is not supported in v4 yet; enforce/strict are reserved for Phase 8 admission/restore semantics", m.String())
	default:
		return fmt.Errorf("PVC_PLUMBER_MODE=%s is not a recognized mode", m.String())
	}
}

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
	// BEFORE config.Load() so that v4-routed modes can skip backend
	// validation. The v4 binary must NOT depend on RustFS / Kopia / S3 /
	// credential env vars to come up — config.LoadWithOptions
	// (SkipBackend=true) is the seam.
	runtimeCfg, runtimeErr := runtimeconfig.Load()

	// Patch 6.7-wire: fail-fast on modes the binary cannot honor today.
	// Enforce and strict are Phase 8 (admission/restore semantics) and
	// have no in-binary behavior yet; crashing here with a clear error
	// beats silently routing them to v3 chart-era (forbidden) or to
	// permissive-equivalent writes (a hidden contract downgrade). See
	// validateMode for the full reasoning.
	if err := validateMode(runtimeCfg.Mode); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	// Patch 6.8a: in permissive mode, fail-fast when any of the six
	// PVC_PLUMBER_DEFAULT_* env vars are missing or set to a value the
	// v4 builder can't safely render (empty string, UID/GID/FSGroup =
	// 0). The alternative — silently emitting RS/RD with empty
	// snapshot class or root-owned mover Pods — would surface as a
	// failed first backup in the karakeep canary that's hard to
	// diagnose. Audit-mode binaries skip this check entirely (the
	// executor short-circuits, so default values never reach the
	// cluster). See runtimeconfig.RequireV4WriteDefaults for the
	// complete contract.
	if err := runtimeconfig.RequireV4WriteDefaults(runtimeCfg); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	// slog drives the legacy HTTP server (cmd/pvc-plumber/main.go uses it);
	// reuse the same JSON format and level resolution so logs look identical
	// whether the operator binary or the legacy binary is running.
	//
	// SkipBackend gate: needsBackend now drives this, not WritesAllowed.
	// The two differ for permissive — WritesAllowed=true (cluster writes
	// happen via the executor) but needsBackend=false (no Kopia/S3
	// inspection in the v4 path). Decoupling them is the core of Patch
	// 6.7-wire.
	cfg, err := config.LoadWithOptions(config.LoadOptions{
		SkipBackend: !needsBackend(runtimeCfg.Mode),
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

	// Backend + cache + HTTP server are conditional. In any v4-routed
	// mode (audit + permissive as of Patch 6.7-wire) the binary must NOT
	// initialize Kopia / S3 / kopia-credentials, must NOT connect to
	// RustFS, and must NOT serve the legacy /exists endpoint — the v4
	// promise is "binary starts cleanly even when the backup
	// infrastructure is unreachable." Phase 2.5d of the v4 PRD,
	// extended to permissive in Patch 6.7-wire.
	//
	// Anything that depends on `bundle` (HTTP server, cache re-warm loop,
	// webhook handlers in runManager) is gated on the same condition.
	var bundle *backendBundle
	if needsBackend(runtimeCfg.Mode) {
		bundle, err = buildBackend(rootCtx, cfg, slogger)
		if err != nil {
			slogger.Error("backend init failed", "error", err)
			os.Exit(1)
		}
	} else {
		slogger.Info("v4 mode: skipping backend init (Kopia / S3 / credentials)",
			"mode", runtimeCfg.Mode.String())
	}

	// In any v4-routed mode (audit + permissive) the V4AuditReconciler
	// (registered inside runManager) and the /audit HTTP handler
	// (mounted on auditSrv below) share one in-memory parity Store.
	// Constructed up here so both subsystems receive the same instance —
	// the reconciler writes parity entries, the handler serves them.
	// Modes that don't run v4 leave the store nil and no v4 HTTP
	// subsystem is wired.
	//
	// The Store's operatorMode field is set from runtimeCfg.Mode so the
	// /audit JSON response correctly reports operator_mode="permissive"
	// when the binary is in permissive mode.
	var auditStore *controller.Store
	if runsV4Reconciler(runtimeCfg.Mode) {
		auditStore = controller.NewStore(
			runtimeCfg.Mode.String(),
			naming.StrategyBareDst.String(),
			naming.DefaultRepoSecretName,
		)
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
		slogger.Info("v4 mode: legacy HTTP server (/exists) + cache re-warm loop NOT started",
			"mode", runtimeCfg.Mode.String())
	}

	// v4 HTTP server. Backend-independent: serves only /audit (parity
	// report), /healthz, /readyz. No /exists, no /metrics (controller-
	// runtime exposes its own /metrics on metricsAddr). Bound to
	// cfg.Port — same socket the legacy server uses for non-v4 modes,
	// so liveness/readiness probes don't have to know which mode the
	// pod is running in. Mounted whenever runsV4Reconciler(mode) is
	// true (audit + permissive today).
	if auditStore != nil {
		auditSrv := newAuditHTTPServer(cfg, auditStore, slogger)
		g.Go(func() error {
			slogger.Info("audit http server starting", "addr", auditSrv.Addr)
			if err := auditSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("audit http server: %w", err)
			}
			return nil
		})
		g.Go(func() error {
			<-gctx.Done()
			slogger.Info("audit http server shutting down")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := auditSrv.Shutdown(shutdownCtx); err != nil {
				return fmt.Errorf("audit http shutdown: %w", err)
			}
			return nil
		})
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
				auditStore,
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
	auditStore *controller.Store,
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

	// Phase 2.5: wrap mgr.GetClient() with auditclient so every write
	// the reconciler attempts is gated by runtimeCfg.Mode. In audit mode
	// the wrapper logs "would-write" and returns nil without touching the
	// cluster — see internal/v4/auditclient.
	reconcilerClient := auditclient.New(mgr.GetClient(), runtimeCfg.Mode, slogger)

	// Reconciler selection. Audit and permissive both run V4AuditReconciler
	// (the executor's Mode-gated short-circuit keeps audit observe-only) —
	// the v3 PVCReconciler is not registered for either, so even an
	// unintended Get on the audit-wrapped client can't trigger v3
	// ensure/update paths. Enforce and strict are rejected by
	// validateMode at startup and never reach this switch; the v3 path
	// is retained in source for future Phase 8 admission/restore
	// semantics. reconcilerKindFor is the single source of truth for
	// this mapping — see top of file + main_test.go.
	switch reconcilerKindFor(runtimeCfg.Mode) {
	case reconcilerKindV4:
		if auditStore == nil {
			return fmt.Errorf("v4-routed mode %q requires a non-nil auditStore; main() must construct one", runtimeCfg.Mode.String())
		}
		// Patch 6.8a: builder/executor defaults arrive via runtimeCfg.
		// In permissive mode RequireV4WriteDefaults (called in main())
		// has already validated that every field is set + non-zero;
		// here we trust the validator and pass through. In audit mode
		// the fields may be zero / nil and the executor short-circuits
		// anyway, so the pass-through is harmless.
		v4rec := newV4Reconciler(reconcilerClient, auditStore, sysNs, runtimeCfg)
		if err := v4rec.SetupWithManager(mgr); err != nil {
			return fmt.Errorf("setup V4AuditReconciler: %w", err)
		}
		slogger.Info("v4 reconciler registered (v3 reconciler NOT registered)",
			"mode", runtimeCfg.Mode.String(),
			"naming_strategy", naming.StrategyBareDst.String(),
			"default_repo_secret", naming.DefaultRepoSecretName,
			"system_namespaces", len(sysNs),
			"default_snapshot_class", runtimeCfg.DefaultSnapshotClass,
			"default_cache_capacity", runtimeCfg.DefaultCacheCapacity,
			"default_storage_class", runtimeCfg.DefaultStorageClass,
			"default_uid", int64OrZero(runtimeCfg.DefaultUID),
			"default_gid", int64OrZero(runtimeCfg.DefaultGID),
			"default_fsgroup", int64OrZero(runtimeCfg.DefaultFSGroup),
		)

	default:
		// PVC reconciler (v3). The reconciler intentionally does NOT
		// take a Kopia client — it only manages ES/RS/RD lifecycle and
		// trusts admission webhooks for the actual restore decisions.
		// The ExternalSecretConfig is plumbed through so the per-PVC
		// `volsync-<pvc>` ES the reconciler renders points at the
		// right ClusterSecretStore / vault item / property names for
		// this deployment (defaults pin to the reference cluster's
		// 1Password Connect layout — see internal/config).
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
		slogger.Info("v3 PVC reconciler registered",
			"mode", runtimeCfg.Mode.String(),
		)
	}

	// Admission webhook handlers. The decoder is built from the manager's
	// scheme so it knows how to deserialize core/v1 PVC payloads. Both
	// handlers receive the same SystemNamespaces / shared cached backend,
	// keeping behavior identical between reconcile and admission paths.
	//
	// Patch 6.7-wire: skip registration entirely for any v4-routed mode
	// (audit + permissive). The binary is safe to deploy without a
	// MutatingWebhookConfiguration / ValidatingWebhookConfiguration; if
	// one is accidentally created against this binary, the webhook
	// server returns 404 for the unregistered routes so admission
	// requests simply fall through to "not handled by this webhook"
	// (kube-apiserver behavior depends on failurePolicy — but the
	// binary itself contributes zero denials).
	//
	// Today this branch is effectively dead because validateMode
	// rejects enforce/strict at startup and the surviving modes
	// (audit + permissive) are both v4-routed. The condition stays
	// defensive: when Phase 8 lands and enforce/strict gain admission
	// semantics, this is the seam they'll re-engage.
	if !runsV4Reconciler(runtimeCfg.Mode) && runtimeCfg.WebhookRegistrationAllowed() {
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
		slogger.Info("admission webhooks NOT registered (v4-routed mode)",
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

// newV4Reconciler is the factory for V4AuditReconciler used by
// runManager. Extracted out of the inline construction site so the
// defaults-wiring contract (Patch 6.8a) is independently unit-testable
// without standing up a controller-runtime manager: a test can call
// newV4Reconciler with a runtimeconfig.Config and assert the
// reconciler's fields match the config's. Trust-but-verify for the
// permissive cutover.
//
// Pre-conditions when called from runManager:
//   - mode is one of {audit, permissive} (validateMode has rejected
//     enforce/strict)
//   - if mode is permissive, RequireV4WriteDefaults has succeeded
//     (every default field is set + UID/GID/FSGroup > 0)
//
// In audit mode the defaults may be nil/zero and that's fine —
// V4AuditReconciler's executor short-circuits, so the zero values
// never reach the cluster.
func newV4Reconciler(
	c client.Client,
	store *controller.Store,
	sysNs map[string]struct{},
	runtimeCfg runtimeconfig.Config,
) *controller.V4AuditReconciler {
	return &controller.V4AuditReconciler{
		Client:               c,
		Store:                store,
		NamingStrategy:       naming.StrategyBareDst,
		DefaultRepoSecret:    naming.DefaultRepoSecretName,
		SystemNamespaces:     sysNs,
		OperatorMode:         runtimeCfg.Mode.String(),
		Mode:                 runtimeCfg.Mode,
		DefaultSnapshotClass: runtimeCfg.DefaultSnapshotClass,
		DefaultCacheCapacity: runtimeCfg.DefaultCacheCapacity,
		DefaultStorageClass:  runtimeCfg.DefaultStorageClass,
		DefaultUID:           int64OrZero(runtimeCfg.DefaultUID),
		DefaultGID:           int64OrZero(runtimeCfg.DefaultGID),
		DefaultFSGroup:       int64OrZero(runtimeCfg.DefaultFSGroup),
	}
}

// int64OrZero dereferences a *int64, returning 0 when the pointer is
// nil. Used to bridge runtimeconfig's "explicit zero vs unset"
// distinction (Patch 6.8a) into the reconciler's plain int64 fields.
// Validation that the value is non-zero in permissive mode happens
// upstream in runtimeconfig.RequireV4WriteDefaults, not here.
func int64OrZero(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
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
