package controllers

import (
	"context"

	"github.com/vshn/statefulset-resize-controller/pvc"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *StatefulSetReconciler) restorePVC(ctx context.Context, pi pvc.Info) (pvc.Info, bool, error) {
	if pi.Restored {
		return pi, true, nil
	}
	done, err := r.resizeSource(ctx, pi)
	if err != nil || !done {
		return pi, done, err
	}

	// Copy data from backup to resized original
	done, err = r.copyPVC(ctx,
		client.ObjectKey{Name: pi.BackupName(), Namespace: pi.Namespace},
		client.ObjectKey{Name: pi.SourceName, Namespace: pi.Namespace})
	if err != nil || !done {
		return pi, done, err
	}

	pi.Restored = true
	return pi, true, nil
}

func (r *StatefulSetReconciler) resizeSource(ctx context.Context, pi pvc.Info) (bool, error) {
	found := corev1.PersistentVolumeClaim{}
	source := pi.GetResizedSource()

	err := r.Get(ctx, client.ObjectKeyFromObject(source), &found)
	if apierrors.IsNotFound(err) {
		// The PVC does not exist.
		// Let's recreate it with the target size
		return true, r.Create(ctx, source)
	}
	if err != nil {
		return false, err
	}
	// There still is a pvc.
	// Check if it it already large enough.
	// If not delete and receate it
	q := found.Spec.Resources.Requests[corev1.ResourceStorage]
	if q.Cmp(pi.TargetSize) < 0 {
		// The deletion might take a while to take effect.
		// Let's backoff to avoid a race condition.
		return false, r.Delete(ctx, &found)
	}
	return true, nil
}
