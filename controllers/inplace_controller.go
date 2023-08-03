package controllers

import (
	"context"
	"time"

	"github.com/vshn/statefulset-resize-controller/statefulset"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// StatefulSetController is an interface for various implementations
// of the StatefulSet controller.
type StatefulSetController interface {
	SetupWithManager(ctrl.Manager) error
}

// InplaceReconciler reconciles a StatefulSet object
// It will resize the PVCs according to the sts template.
type InplaceReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	RequeueAfter time.Duration
	LabelName    string
}

// Reconcile is the main work loop, reacting to changes in statefulsets and initiating resizing of StatefulSets.
func (r *InplaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("statefulset", req.NamespacedName)
	ctx = log.IntoContext(ctx, l)

	sts := &appsv1.StatefulSet{}
	err := r.Client.Get(ctx, req.NamespacedName, sts)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	l.V(1).Info("Checking label for sts", "labelName", r.LabelName)
	if sts.GetLabels()[r.LabelName] != "true" {
		l.V(1).Info("Label not found, skipping sts")
		return ctrl.Result{}, nil
	}

	l.Info("Found sts with label", "labelName", r.LabelName)

	stsEntity, err := statefulset.NewEntity(sts)
	if err != nil {
		return ctrl.Result{}, err
	}

	stsEntity.Pvcs, err = fetchResizablePVCs(ctx, r.Client, *stsEntity)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(stsEntity.Pvcs) == 0 {
		l.Info("All PVCs have the right size")
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, resizePVCsInplace(ctx, r.Client, stsEntity.Pvcs)
}

// SetupWithManager sets up the controller with the Manager.
func (r *InplaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.StatefulSet{}).
		Complete(r)
}
