package controllers

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// restorePVC will grow the referenced PVC to the target size, by recreating it if necessary and restore the
// original data form the backup, created earlier using backupPVC.
// This function might not run through successfully in a single run but may return an `errInProgress`, signifying
// that the caller needs to retry later.
func (r *StatefulSetReconciler) restorePVC(ctx context.Context, pi pvcInfo) error {
	// Check if the backup we want to restore from actually exists
	backup := corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, client.ObjectKey{Name: pi.backupName(), Namespace: pi.Namespace}, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			// If its not present we are in an inconsitent state
			// We want to abort and scale back up
			return newErrAbort("backup pvc missing while trying to restore it")
		}
		return err
	}
	// Get original pvc and recreate it if it is too small
	original, err := r.getOrRecreatePVC(ctx, pi)
	if err != nil {
		return err
	}
	// Check if we already restored it
	if original.Annotations != nil && original.Annotations[DoneAnnotation] == "true" {
		return nil
	}
	// Copy data from backup to resized original
	err = r.copyPVC(ctx,
		client.ObjectKey{Name: pi.backupName(), Namespace: pi.Namespace},
		client.ObjectKey{Name: pi.Name, Namespace: pi.Namespace})
	if err != nil {
		return err
	}

	// We ran successfully. Let's mark it as done
	if original.Annotations == nil {
		// This should generally not happen, but let's better not panic if it does
		original.Annotations = map[string]string{}
	}
	original.Annotations[DoneAnnotation] = "true"
	return r.Update(ctx, original)
}

// getOrRecreatePVC will get the referenced PVC. If it is not large enough, it will recreate the PVC with updated
// resource requirements.
// This function might not run through successfully in a single run but may return an `errInProgress`, signifying
// that the caller needs to retry later.
func (r *StatefulSetReconciler) getOrRecreatePVC(ctx context.Context, pi pvcInfo) (*corev1.PersistentVolumeClaim, error) {
	found := corev1.PersistentVolumeClaim{}
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pi.Name,
			Namespace: pi.Namespace,
			Labels:    pi.Labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: pi.Spec.AccessModes,
			Resources: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceStorage: pi.TargetSize,
				},
			},
			StorageClassName: pi.Spec.StorageClassName,
			VolumeMode:       pi.Spec.VolumeMode,
		},
	}

	err := r.Get(ctx, client.ObjectKey{Name: pi.Name, Namespace: pi.Namespace}, &found)
	if err != nil && apierrors.IsNotFound(err) {
		// The PVC does not exist.
		// Let's recreate it with the target size
		if err := r.Create(ctx, &pvc); err != nil {
			return nil, err
		}
		return &pvc, nil
	} else if err != nil {
		return nil, err
	}
	// There still is a pvc.
	// Check if it it already large enough.
	// If not delete and receate it
	q := found.Spec.Resources.Requests[corev1.ResourceStorage]
	if q.Cmp(pi.TargetSize) < 0 {
		if err := r.Delete(ctx, &found); err != nil {
			return nil, err
		}
		// The delete might take a while to take effect.
		// Let's backoff to avoid a race condition.
		return nil, errInProgress
	}
	return &found, nil
}
