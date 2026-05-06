// Command operator is the controller-runtime entrypoint for pvc-plumber v2.
//
// Phase 1 scaffold: this binary stands up a controller-runtime manager with
// the PVC reconciler stub registered, three webhook routes wired to stub
// handlers, leader election, healthz/readyz, and a metrics endpoint. The
// existing read-only HTTP server in cmd/pvc-plumber/main.go is unchanged;
// Phase 3 will wire the two together (env reads, Kopia client sharing,
// optional in-process co-location).
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/mitchross/pvc-plumber/internal/controller"
	pvcwebhook "github.com/mitchross/pvc-plumber/internal/webhook"
)

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
		"The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8082",
		"The address the probe endpoint binds to.")
	flag.IntVar(&webhookPort, "webhook-port", 9443,
		"The port the webhook server listens on.")
	flag.StringVar(&webhookCertDir, "webhook-cert-dir",
		"/tmp/k8s-webhook-server/serving-certs",
		"The directory containing the TLS certs for the webhook server.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&leaderElectionID, "leader-election-id",
		"pvc-plumber.mitchross.github.io",
		"The name of the resource lock used for leader election.")

	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
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
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Phase 1: register the PVC reconciler stub. The struct gains real
	// dependencies (Kopia client, NFS coordinates, system-namespace list)
	// in Phase 3.
	if err := (&controller.PVCReconciler{
		Client: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up controller", "controller", "PersistentVolumeClaim")
		os.Exit(1)
	}

	// Phase 1: register webhook routes against stub handlers so the webhook
	// server has a real surface for cert-manager + admissionregistration
	// integration tests in later phases. Paths match the design doc.
	hookServer := mgr.GetWebhookServer()
	hookServer.Register("/mutate-v1-pvc", &webhook.Admission{Handler: &pvcwebhook.PVCMutator{}})
	hookServer.Register("/validate-v1-pvc", &webhook.Admission{Handler: &pvcwebhook.PVCValidator{}})
	hookServer.Register("/mutate-batch-v1-job", &webhook.Admission{Handler: &pvcwebhook.JobMutator{}})

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager",
		"metrics", metricsAddr,
		"probes", probeAddr,
		"webhook-port", webhookPort,
		"leader-election", enableLeaderElection,
	)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
