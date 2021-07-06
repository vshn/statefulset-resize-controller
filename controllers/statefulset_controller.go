package controllers

import (
	"context"
	"errors"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// StatefulSetReconciler reconciles a StatefulSet object
type StatefulSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const stateAnnotation = "sts-resize.appuio.ch/state"
const (
	stateScaledown = "scaledown"
	stateBackup    = "backup"
	stateResize    = "resize"
)

//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=statefulsets/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=apps,resources=statefulsets/finalizers,verbs=update

func (r *StatefulSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("statefulset", req.NamespacedName)

	sts := appsv1.StatefulSet{}
	if err := r.Get(ctx, req.NamespacedName, &sts); err != nil {
		l.Error(err, "unable to fetch statefulset")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// TODO(glrf) handle edgecase of starting up sts? (Probably is fine as they should never start up with wrong size?)
	// NOTE(glrf) This will get _all_ PVCs that belonged to the sts. Even the ones not used anymore (i.e. if scaled up and down)
	pvcs := corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, &pvcs, client.InNamespace(req.Namespace), client.MatchingLabels(sts.Spec.Selector.MatchLabels)); err != nil {
		l.Error(err, "unable to list pvcs")
		return ctrl.Result{}, err
	}
	rps := filterResizablePVCs(ctx, sts, pvcs.Items)

	// Check if we are resizing this StS (have a state)
	// If we do
	state, ok := sts.Annotations[stateAnnotation]
	if !ok && len(rps) > 0 {
		// There are resizable PVCs.
		state = stateScaledown
		sts.Annotations[stateAnnotation] = state
	}

	// TODO(glrf) We shoud somehow fallthrough if we can continue
	var err error
	switch state {
	case stateScaledown:
		sts, err = r.ScaleDown(ctx, sts)
	case stateBackup:
		sts, err = r.Backup(ctx, sts, rps)
	case stateResize:
		sts, err = r.Resize(ctx, sts, pvcs.Items)
	default:
	}
	// TODO(glrf) Handle StS update

	return ctrl.Result{}, err
}

// FilterResizablePVCs filters out the PVCs that do not match the request of the statefulset
// TODO(glrf) try to break this
func filterResizablePVCs(ctx context.Context, sts appsv1.StatefulSet, pvcs []corev1.PersistentVolumeClaim) []corev1.PersistentVolumeClaim {
	return nil
}

// ScaleDown will scale the StatefulSet and requeue the request with a backoff.
// When the StS sucessfully scaled down to 0, it will advance to the next state `backup`
func (r *StatefulSetReconciler) ScaleDown(ctx context.Context, sts appsv1.StatefulSet) (appsv1.StatefulSet, error) {
	// TODO(glrf) store the original number of replicas.
	return sts, errors.New("not implemented")
}

// Backup will create a copy of all provided pvcs.
// When all pvcs are backed up successfully, it will advance to the next state `resize`
func (r *StatefulSetReconciler) Backup(ctx context.Context, sts appsv1.StatefulSet, pvcs []corev1.PersistentVolumeClaim) (appsv1.StatefulSet, error) {
	return sts, errors.New("not implemented")
}

// Resize will recreate all PVCs with the new size and copy the content of its backup to the new PVCs.
// When all pvcs are recreated and their contents restored, it will scale up the statfulset back to it original replicas
func (r *StatefulSetReconciler) Resize(ctx context.Context, sts appsv1.StatefulSet, pvcs []corev1.PersistentVolumeClaim) (appsv1.StatefulSet, error) {
	return sts, errors.New("not implemented")
}

// SetupWithManager sets up the controller with the Manager.
func (r *StatefulSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// TODO(glrf) Add mode to only watch sts with specific labels.
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.StatefulSet{}).
		Complete(r)
}
