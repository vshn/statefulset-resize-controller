package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
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
}

const sizeAnnotation = "sts-resize.appuio.ch/size"

const pvcAnnotation = "sts-resize.appuio.ch/pvcs"

var errInProgress = errors.New("in progress")

//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=statefulsets/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=apps,resources=statefulsets/finalizers,verbs=update

func (r *StatefulSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("statefulset", req.NamespacedName)

	old := appsv1.StatefulSet{}
	if err := r.Get(ctx, req.NamespacedName, &old); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	sts := *old.DeepCopy()

	// TODO(glrf) REFACTOR THIS MESS
	var err error
	pvcs := []pvcInfo{}
	pvcsRaw := sts.Annotations[pvcAnnotation]
	if pvcsRaw == "" {
		pvcs, err = getResizablePVCs(ctx, r, sts)
		if err != nil {
			l.V(0).Error(err, "Unable to fetch PVCs")
			return ctrl.Result{}, err
		}
		pvcsRaw, err := json.Marshal(pvcs)
		if err != nil {
			return ctrl.Result{}, err
		}
		sts.Annotations[pvcAnnotation] = string(pvcsRaw)
	} else {
		if err := json.Unmarshal([]byte(pvcsRaw), &pvcs); err != nil {
			return ctrl.Result{}, err
		}
	}
	if len(pvcs) == 0 {
		return ctrl.Result{}, nil
	}

	l.V(0).Info("resize")
	res := ctrl.Result{}
	sts, err = r.resize(ctx, sts, pvcs)
	if errors.Is(err, errInProgress) {
		l.V(0).Error(err, "In Progress")
		res = ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}
	} else if err != nil {
		l.V(0).Error(err, "Unable to resize PVCs")
		return ctrl.Result{}, err
	} else {
		delete(sts.Annotations, pvcAnnotation)
		delete(sts.Annotations, replicasAnnotation)
		delete(sts.Annotations, scalupAnnotation)
	}

	if !reflect.DeepEqual(sts.Annotations, old.Annotations) || !reflect.DeepEqual(sts.Spec, old.Spec) {
		err := r.Client.Update(ctx, &sts)
		if err != nil {
			l.V(0).Error(err, "Unable to update StatefulSet")
			return ctrl.Result{}, err
		}
	}
	return res, nil
}

func (r *StatefulSetReconciler) resize(ctx context.Context, sts appsv1.StatefulSet, pvcs []pvcInfo) (appsv1.StatefulSet, error) {
	sts, err := scaleDown(sts)
	if err != nil {
		return sts, err
	}
	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	// TODO(glrf) THIS IS BROKEN! The pvc do no have to be completed as the same time. This will loop as restorePVC
	// is not idepotent.
	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	done := true
	for _, pvc := range pvcs {
		l := log.FromContext(ctx).WithValues("pvc", pvc.Name)
		err = r.resizePVC(ctx, pvc)
		if errors.Is(err, errInProgress) {
			l.Info("inpro")
			done = false
		} else if err != nil {
			return sts, err
		}
	}
	if !done {
		return sts, errInProgress
	}

	return scaleUp(sts)
}

// SetupWithManager sets up the controller with the Manager.
func (r *StatefulSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// TODO(glrf) Add mode to only watch sts with specific labels.
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.StatefulSet{}).
		Complete(r)
}
