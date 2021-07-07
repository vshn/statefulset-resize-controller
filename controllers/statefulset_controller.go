package controllers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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

const stateAnnotation = "sts-resize.appuio.ch/state"
const (
	stateScaledown = "scaledown"
	stateBackup    = "backup"
	stateResize    = "resize"
)
const replicasAnnotation = "sts-resize.appuio.ch/replicas"
const sizeAnnotation = "sts-resize.appuio.ch/size"

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
	// TODO(glrf) handle edgecase of starting up sts? (Probably is fine as they should never start up with wrong size?)
	// NOTE(glrf) This will get _all_ PVCs that belonged to the sts. Even the ones not used anymore (i.e. if scaled up and down)
	pvcs := corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, &pvcs, client.InNamespace(req.Namespace), client.MatchingLabels(sts.Spec.Selector.MatchLabels)); err != nil {
		l.Error(err, "unable to list pvcs")
		return ctrl.Result{}, err
	}
	rps := filterResizablePVCs(sts, pvcs.Items)

	// Check if we are resizing this StS (have a state)
	// If we do
	state, ok := sts.Annotations[stateAnnotation]
	if !ok && len(rps) > 0 {
		// There are resizable PVCs.
		state = stateScaledown
		r.Recorder.Event(&sts, "Normal", "ScaleDown", "Scaling down StatefulSet")
	}

	// TODO(glrf) We shoud somehow fallthrough if we can continue
	var err error
	switch state {
	case stateScaledown:
		sts, err = scaleDown(sts)
	case stateBackup:
		sts, err = r.backup(ctx, sts, rps)
	case stateResize:
		sts, err = r.resize(ctx, sts, pvcs.Items)
	default:
	}
	if err != nil && !errors.Is(err, errInProgress) {
		return ctrl.Result{}, err
	}
	res := ctrl.Result{}
	if errors.Is(err, errInProgress) {
		res = ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}
	}
	if !reflect.DeepEqual(sts.Annotations, old.Annotations) || !reflect.DeepEqual(sts.Spec, old.Spec) {
		err := r.Client.Update(ctx, &sts)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	return res, nil
}

// filterResizablePVCs filters out the PVCs that do not match the request of the statefulset
// It will also add the target size as an annotation sts-resize.appuio.ch/size
func filterResizablePVCs(sts appsv1.StatefulSet, pvcs []corev1.PersistentVolumeClaim) []corev1.PersistentVolumeClaim {
	// StS managed PVCs are created according to the VolumeClaimTemplate.
	// The name of the resulting PVC will be in the following format
	// <template.name>-<sts.name>-<ordinal-number>
	// This allows us to match the pvcs to the template

	var res []corev1.PersistentVolumeClaim

	for _, pvc := range pvcs {
		if pvc.Namespace != sts.Namespace {
			continue
		}
		for _, tpl := range sts.Spec.VolumeClaimTemplates {
			if !strings.HasPrefix(pvc.Name, tpl.Name) {
				continue
			}
			n := strings.TrimPrefix(pvc.Name, fmt.Sprintf("%s-", tpl.Name))
			if !strings.HasPrefix(n, sts.Name) {
				continue
			}
			n = strings.TrimPrefix(n, fmt.Sprintf("%s-", sts.Name))
			if _, err := strconv.Atoi(n); err != nil {
				continue
			}
			q := pvc.Spec.Resources.Requests[corev1.ResourceStorage]            // Necessary because pointer receiver
			if q.Cmp(tpl.Spec.Resources.Requests[corev1.ResourceStorage]) < 0 { // Returns -1 if q < requested size
				s := tpl.Spec.Resources.Requests[corev1.ResourceStorage]
				if pvc.Annotations == nil {
					pvc.Annotations = map[string]string{}
				}
				pvc.Annotations[sizeAnnotation] = s.String()
				res = append(res, pvc)
				break
			}
		}
	}

	return res
}

// scaleDown will scale the StatefulSet and requeue the request with a backoff.
// When the StS sucessfully scaled down to 0, it will advance to the next state `backup`
func scaleDown(sts appsv1.StatefulSet) (appsv1.StatefulSet, error) {
	if *sts.Spec.Replicas == 0 && sts.Status.Replicas == 0 {
		sts.Annotations[stateAnnotation] = stateBackup
		return sts, nil
	}
	sts.Annotations[stateAnnotation] = stateScaledown
	if sts.Annotations[replicasAnnotation] == "" {
		sts.Annotations[replicasAnnotation] = strconv.Itoa(int(*sts.Spec.Replicas))
	}
	z := int32(0)
	sts.Spec.Replicas = &z
	return sts, errInProgress
}

// Backup will create a copy of all provided pvcs.
// When all pvcs are backed up successfully, it will advance to the next state `resize`
func (r *StatefulSetReconciler) backup(ctx context.Context, sts appsv1.StatefulSet, pvcs []corev1.PersistentVolumeClaim) (appsv1.StatefulSet, error) {
	if *sts.Spec.Replicas != 0 || sts.Status.Replicas != 0 {
		// Fallback to last state
		sts.Annotations[stateAnnotation] = stateScaledown
		return sts, nil
	}
	// TODO(glrf) make backup
	return sts, errInProgress
}

// Resize will recreate all PVCs with the new size and copy the content of its backup to the new PVCs.
// When all pvcs are recreated and their contents restored, it will scale up the statfulset back to it original replicas
func (r *StatefulSetReconciler) resize(ctx context.Context, sts appsv1.StatefulSet, pvcs []corev1.PersistentVolumeClaim) (appsv1.StatefulSet, error) {
	return sts, errors.New("not implemented")
}

// SetupWithManager sets up the controller with the Manager.
func (r *StatefulSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// TODO(glrf) Add mode to only watch sts with specific labels.
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.StatefulSet{}).
		Complete(r)
}
