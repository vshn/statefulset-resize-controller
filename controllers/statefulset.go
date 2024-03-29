package controllers

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/vshn/statefulset-resize-controller/statefulset"
)

// CriticalError is an unrecoverable error.
type CriticalError struct {
	Err           error
	Event         string
	SaveToScaleUp bool
}

// Error implements the Error interface
func (err CriticalError) Error() string {
	return err.Err.Error()
}

// Unwrap is used to make it work with errors.Is, errors.As.
func (err *CriticalError) Unwrap() error {
	return err.Err
}

func isCritical(err error) *CriticalError {
	cerr := CriticalError{}
	if err != nil && errors.As(err, &cerr) {
		return &cerr
	}
	return nil
}

func (r StatefulSetReconciler) fetchStatefulSet(ctx context.Context, namespacedName types.NamespacedName) (*statefulset.Entity, error) {
	old := &appsv1.StatefulSet{}
	err := r.Get(ctx, namespacedName, old)
	if err != nil {
		return nil, err
	}
	sts, err := statefulset.NewEntity(old)
	if err != nil {
		return nil, err
	}

	if !sts.Resizing() {
		sts.Pvcs, err = fetchResizablePVCs(ctx, r.Client, *sts)
		return sts, err
	}
	return sts, nil
}

func (r StatefulSetReconciler) resizeStatefulSet(ctx context.Context, sts *statefulset.Entity) (bool, error) {
	stsv1, err := sts.StatefulSet()
	if err != nil {
		return false, err
	}
	l := log.FromContext(ctx).WithValues("statefulset", fmt.Sprintf("%s/%s", stsv1.Namespace, stsv1.Name))

	done := sts.PrepareScaleDown()
	if !done {
		return done, r.updateStatefulSet(ctx, sts, nil)
	}

	objs, err := r.createRbacObjs(ctx, sts)
	if err != nil {
		return false, err
	}

	sts.Pvcs, err = r.resizePVCs(context.WithValue(ctx, RbacObjCtxKey, objs), sts.Pvcs)
	if err != nil || len(sts.Pvcs) > 0 {
		return len(sts.Pvcs) == 0, r.updateStatefulSet(ctx, sts, err)
	}

	err = r.deleteRbacObjs(ctx, objs)
	if err != nil {
		l.Info("Failed to delete Job RBAC objects", "error", err)
	}

	done, err = sts.PrepareScaleUp()
	return done, r.updateStatefulSet(ctx, sts, err)
}

func (r StatefulSetReconciler) updateStatefulSet(ctx context.Context, si *statefulset.Entity, resizeErr error) error {
	sts, err := si.StatefulSet()
	if err != nil {
		return err
	}
	l := log.FromContext(ctx).WithValues("statefulset", fmt.Sprintf("%s/%s", sts.Namespace, sts.Name))
	if resizeErr != nil {
		l.Error(err, "failed to resize statefulset")
	}
	if cerr := isCritical(resizeErr); cerr != nil {
		si.SetFailed()
		if cerr.SaveToScaleUp {
			// If we fail here there is not much to do
			if _, err = si.PrepareScaleUp(); err != nil {
				l.Error(err, "failed to scale up statefulset")
			}
		}
		r.Recorder.Event(sts, "Warning", "ResizeFailed", cerr.Event)
	}
	if !reflect.DeepEqual(sts.Annotations, si.Old.Annotations) ||
		!reflect.DeepEqual(sts.Spec, si.Old.Spec) ||
		!reflect.DeepEqual(sts.Labels, si.Old.Labels) {
		return r.Client.Update(ctx, sts)
	}
	return nil
}
