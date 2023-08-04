package main

import (
	"flag"
	"os"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	"go.uber.org/zap/zapcore"
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/vshn/statefulset-resize-controller/controllers"
	//+kubebuilder:scaffold:imports
)

//go:generate go run sigs.k8s.io/controller-tools/cmd/controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./..."
//go:generate go run sigs.k8s.io/controller-tools/cmd/controller-gen rbac:roleName=controller-manager paths="./..."

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	//+kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var syncContainerImage string
	var syncClusterRole string
	var inplaceResize bool
	var inplaceLabelName string
	var logLevel int
	flag.StringVar(&syncContainerImage, "sync-image", "instrumentisto/rsync-ssh", "A container image containing rsync, used to move data.")
	flag.StringVar(&syncClusterRole, "sync-cluster-role", "", "ClusterRole to use for the sync jobs."+
		"For example, this can be used to allow the sync job to run as root on a cluster with PSPs enabled by providing the name of a ClusterRole which allows usage of a privileged PSP.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&inplaceResize, "inplace", false, "Enable in-place update of PVCs. "+
		"If the underlying storage supports direct resizing of the PVCs this should be used.")
	flag.StringVar(&inplaceLabelName, "inplaceLabelName", "sts-resize.vshn.net/resize-inplace", "If inplace resize is enable the sts needs to have this label with value \"true\" in order to be handled.")
	flag.IntVar(&logLevel, "log-level", 0, "Set the log level.")
	flag.Parse()

	opts := zap.Options{
		Development: true,
		Level:       zapcore.Level(logLevel * -1),
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		MetricsBindAddress:     metricsAddr,
		Port:                   9443,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "d8b76942.vshn.net",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	var stsController controllers.StatefulSetController = &controllers.StatefulSetReconciler{
		Client:             mgr.GetClient(),
		Scheme:             mgr.GetScheme(),
		Recorder:           mgr.GetEventRecorderFor("statefulset-resize-controller"),
		SyncContainerImage: syncContainerImage,
		SyncClusterRole:    syncClusterRole,
		RequeueAfter:       10 * time.Second,
	}

	if inplaceResize {
		stsController = &controllers.InplaceReconciler{
			Client:       mgr.GetClient(),
			Scheme:       mgr.GetScheme(),
			Recorder:     mgr.GetEventRecorderFor("statefulset-resize-controller"),
			RequeueAfter: 10 * time.Second,
			LabelName:    inplaceLabelName,
		}
	}

	if err = stsController.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "StatefulSet")
		os.Exit(1)
	}
	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
