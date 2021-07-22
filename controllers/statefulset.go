package controllers

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

func (r StatefulSetReconciler) fetchStatefulSet(ctx context.Context, namespacedName types.NamespacedName) (*statefulset.Info, error) {
	old := &appsv1.StatefulSet{}
	err := r.Get(ctx, namespacedName, old)
	if err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	sts, err := statefulset.NewInfo(old)
	if err != nil {
		return nil, err
	}

	if len(sts.Pvcs) == 0 {
		sts.Pvcs, err = r.fetchResizablePVCs(ctx, *sts)
		return sts, err
	}
	return sts, nil
}

func (r StatefulSetReconciler) resizeStatefulSet(ctx context.Context, sts *statefulset.Info) (bool, error) {
	done := sts.ScaleDown()
	if !done {
		return done, r.updateStatefulSet(ctx, sts, nil)
	}

	for i, pvc := range sts.Pvcs {
		pvc, d, err := r.resizePVC(ctx, pvc)
		sts.Pvcs[i] = pvc
		if err != nil {
			return false, r.updateStatefulSet(ctx, sts, err)
		}
		if !d {
			done = false
		}
	}
	if !done {
		return done, r.updateStatefulSet(ctx, sts, nil)
	}

	done, err := sts.ScaleUp()
	return done, r.updateStatefulSet(ctx, sts, err)
}

func (r StatefulSetReconciler) updateStatefulSet(ctx context.Context, si *statefulset.Info, err error) error {
	sts, e := si.Sts()
	if e != nil {
		return err
	}
	l := log.FromContext(ctx).WithValues("statefulset", fmt.Sprintf("%s/%s", sts.Namespace, sts.Name))
	if err != nil {
		l.Error(err, "failed to resize statefulset")
	}
	if cerr := isCritical(err); cerr != nil {
		si.SetFailed()
		if cerr.SaveToScaleUp {
			// If we fail here ther is not much to do
			_, err = si.ScaleUp()
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
