package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	SyncContainerImage string
}

// Label for failed sts resizing that need human interaction
const failedLabel = "sts-resize.appuio.ch/failed"

// Annotation key in which the initial state of the pvcs is stored in
const pvcAnnotation = "sts-resize.appuio.ch/pvcs"

// Error to return if reconciliation is running as planed but the caller needs to backoff and retry later
var errInProgress = errors.New("in progress")

// Error requiring manual recovery
var errCritical = errors.New("critical")

// newErrCritical returns a new critical error.
// The issue should be descriptive enough that Ops knows what is wrong
func newErrCritical(issue string) error {
	return fmt.Errorf("%w: %s", errCritical, issue)
}

// Unrecoverable error.
// Will cause the reconciliation to stop, mark the StatefulSet as aborted and scale back
var errAbort = errors.New("Abort")

// newErrAbort returns a new unrecoverable error.
// The issue should be descriptive enough that Ops knows what is wrong
func newErrAbort(issue string) error {
	return fmt.Errorf("%w: %s", errAbort, issue)
}

//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=statefulsets/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=apps,resources=statefulsets/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaim,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get;update;patch

// Reconcile is the main work loop, reacting to changes in statefulsets and initiating resizing of StatefulSets
func (r *StatefulSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx).WithValues("statefulset", req.NamespacedName)

	old := appsv1.StatefulSet{}
	if err := r.Get(ctx, req.NamespacedName, &old); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	sts := *old.DeepCopy()

	pvcs, err := r.getPVCInfo(ctx, sts)
	if err != nil {
		l.Error(err, "Failed to get information of PVCs")
		return ctrl.Result{}, err
	}
	if len(pvcs) == 0 {
		return ctrl.Result{}, nil
	}
	if sts.Labels != nil && sts.Labels[failedLabel] == "true" {
		// This Sts needs human interaction, we cannot fix this
		return ctrl.Result{}, nil
	}

	res := ctrl.Result{}
	sts, err = r.resize(ctx, sts, pvcs)
	switch {
	case errors.Is(err, errInProgress):
		// Resizing is in progresss, backing off
		res = ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}
	case errors.Is(err, errAbort):
		// Resizing failed and connot be recovered automatically
		// We can abort and scale up
		// We potentially ignore a critical error here, but as we already notify ops there is not much else to do
		sts, _ = scaleUp(sts)
		fallthrough
	case errors.Is(err, errCritical):
		// Something went very wrong.
		r.Recorder.Event(&sts, "Warning", "ErrorResize", err.Error())
		l.Error(err, "Unable to resize PVCs and cannot recover")
		if sts.Labels == nil {
			sts.Labels = map[string]string{}
		}
		sts.Labels[failedLabel] = "true"
	case err == nil:
		// Cleanup annotation with PVCInfo so we do not try to resize again
		r.Recorder.Event(&sts, "Normal", "ResizeComplete", "Successfully resized StatefulSet")
		l.Info("Successfully resized StatefulSet")
		delete(sts.Annotations, pvcAnnotation)
	default:
		l.Error(err, "Unable to resize PVCs")
		return ctrl.Result{}, err
	}

	if !reflect.DeepEqual(sts.Annotations, old.Annotations) || !reflect.DeepEqual(sts.Spec, old.Spec) {
		err := r.Client.Update(ctx, &sts)
		if err != nil {
			l.Error(err, "Unable to update StatefulSet")
			return ctrl.Result{}, err
		}
	}
	return res, nil
}

func (r *StatefulSetReconciler) getPVCInfo(ctx context.Context, sts appsv1.StatefulSet) ([]pvcInfo, error) {
	pis := []pvcInfo{}
	if sts.Annotations[pvcAnnotation] != "" {
		if err := json.Unmarshal([]byte(sts.Annotations[pvcAnnotation]), &pis); err != nil {
			return nil, newErrCritical(fmt.Sprintf("Annotation %s malformed", pvcAnnotation))
		}
		return pis, nil
	}
	pis, err := getResizablePVCs(ctx, r, sts)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(pis)
	if err != nil {
		return nil, err
	}
	sts.Annotations[pvcAnnotation] = string(data)

	return pis, nil
}

func (r *StatefulSetReconciler) resize(ctx context.Context, sts appsv1.StatefulSet, pvcs []pvcInfo) (appsv1.StatefulSet, error) {
	sts, err := scaleDown(sts)
	if err != nil {
		return sts, err
	}
	done := true
	for _, pvc := range pvcs {
		err = r.resizePVC(ctx, pvc)
		if errors.Is(err, errInProgress) {
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
	// TODO(glrf) Add mode to only watch sts with specific labels or NS?
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.StatefulSet{}).
		Complete(r)
}
