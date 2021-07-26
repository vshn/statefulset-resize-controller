package controllers

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// StatefulSetReconciler reconciles a StatefulSet object
type StatefulSetReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	SyncContainerImage string
}

//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=statefulsets/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=apps,resources=statefulsets/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaim,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get;update;patch

// Reconcile is the main work loop, reacting to changes in statefulsets and initiating resizing of StatefulSets.
func (r *StatefulSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("statefulset", req.NamespacedName)

	sts, err := r.fetchStatefulSet(ctx, req.NamespacedName)
	if err != nil {
		l.Error(err, "Unable to fetch StatefulSet")
		return ctrl.Result{}, err
	}
	if !sts.Resizing() || sts.Failed() {
		return ctrl.Result{}, nil
	}

	done, err := r.resizeStatefulSet(ctx, sts)
	if err != nil {
		l.Error(err, "Unable to resize StatefulSet")
		return ctrl.Result{}, err
	}
	if !done {
		return ctrl.Result{
			RequeueAfter: 2 * time.Second,
		}, nil
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StatefulSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.StatefulSet{}).
		Complete(r)
}
