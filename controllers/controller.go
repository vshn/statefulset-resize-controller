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
const FailedLabel = "sts-resize.appuio.ch/failed"

// Annotation key in which the initial state of the pvcs is stored in
const PvcAnnotation = "sts-resize.appuio.ch/pvcs"

// Error to return if reconciliation is running as planed but the caller needs to backoff and retry later
var errInProgress = errors.New("in progress")

// Error requiring manual recovery
var errCritical = errors.New("critical")

// newErrCritical returns a new critical error.
// The issue should be descriptive enough that Ops knows what is wrong.
func newErrCritical(issue string) error {
	return fmt.Errorf("%w: %s", errCritical, issue)
}

// Unrecoverable error.
// Will cause the reconciliation to stop, mark the StatefulSet as aborted and scale back.
var errAbort = errors.New("Abort")

// newErrAbort returns a new unrecoverable error.
// The issue should be descriptive enough that Ops knows what is wrong.
func newErrAbort(issue string) error {
	return fmt.Errorf("%w: %s", errAbort, issue)
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

	// Fetch StatefulSet of the request
	old := appsv1.StatefulSet{}
	if err := r.Get(ctx, req.NamespacedName, &old); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	sts := *old.DeepCopy()

	// Get PVC information for all PVC that need to be resized
	pvcs, err := r.getResizablePVCInfo(ctx, sts)
	if err != nil {
		l.Error(err, "Failed to get information of PVCs")
		return ctrl.Result{}, err
	}
	if len(pvcs) == 0 {
		// There are no PVCs that need to be resized.
		// We do not need to handle this.
		return ctrl.Result{}, nil
	}
	if sts.Labels != nil && sts.Labels[FailedLabel] == "true" {
		// If this label is set the Sts needs human interaction, we cannot fix this.
		// Common example for that is if the data transfer failed repeatedly, or if someone or something messed with the
		// controller state and we are unable to recover.
		return ctrl.Result{}, nil
	}

	// Perform resizing of all relevant PVCs of the StatefulSet, this includes the save scale down and scale up of the StatefulSet.
	// This will not run through in one go but is meant to be called repeatedly until it succeeds.
	// The resizing is idempotent and will return an errInProgress if we need to wait for asynchronous actions to complete.
	res := ctrl.Result{}
	sts, err = r.resize(ctx, sts, pvcs)
	switch {
	case errors.Is(err, errInProgress):
		// Resizing is in progress, backing off.
		res = ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}
	case errors.Is(err, errAbort):
		// Resizing failed and cannot be recovered automatically.
		// We can abort and scale up.
		// We potentially ignore a critical error here, but as we already notify ops there is not much else to do.
		sts, _ = scaleUp(sts)
		fallthrough
	case errors.Is(err, errCritical):
		// Something went very wrong.
		r.Recorder.Event(&sts, "Warning", "ErrorResize", err.Error())
		l.Error(err, "Unable to resize PVCs and cannot recover")
		if sts.Labels == nil {
			sts.Labels = map[string]string{}
		}
		sts.Labels[FailedLabel] = "true"
	case err == nil:
		// We ran through successfully, so the resizing is complete.
		// Cleanup annotation with PVCInfo so we do not try to resize again.
		r.Recorder.Event(&sts, "Normal", "ResizeComplete", "Successfully resized StatefulSet")
		l.Info("Successfully resized StatefulSet")
		delete(sts.Annotations, PvcAnnotation)
	default:
		// Some potentially recoverable error. We will just back off exponentially and try again.
		l.Error(err, "Unable to resize PVCs")
		return ctrl.Result{}, err
	}

	// Apply possible changes from the resize function.
	if !reflect.DeepEqual(sts.Annotations, old.Annotations) || !reflect.DeepEqual(sts.Spec, old.Spec) {
		err := r.Client.Update(ctx, &sts)
		if err != nil {
			l.Error(err, "Unable to update StatefulSet")
			return ctrl.Result{}, err
		}
	}
	return res, nil
}

// getResizablePVCInfo returns a list of PVCs that do not satisfy the size requested as part of the template.
func (r *StatefulSetReconciler) getResizablePVCInfo(ctx context.Context, sts appsv1.StatefulSet) ([]pvcInfo, error) {
	// After the initial search for relevant PVCs, it will cache the result as an annotation.
	// This is not done for performance, but to keep record of resizing PVCs and to not potentially loose them when recreating them.
	pis := []pvcInfo{}
	if sts.Annotations[PvcAnnotation] != "" {
		if err := json.Unmarshal([]byte(sts.Annotations[PvcAnnotation]), &pis); err != nil {
			return nil, newErrCritical(fmt.Sprintf("Annotation %s malformed", PvcAnnotation))
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
	sts.Annotations[PvcAnnotation] = string(data)

	return pis, nil
}

// resize grows all specified PVCs to their target size. It will first safely scale down the StatefulSet and scale it up if it
// ran through successfully.
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.StatefulSet{}).
		Complete(r)
}
