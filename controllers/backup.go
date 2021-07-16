package controllers

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *StatefulSetReconciler) backupPVC(ctx context.Context, pi pvcInfo) error {
	// Check if the original PVC still exists. If not there is a problem
	original := corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, client.ObjectKey{Name: pi.Name, Namespace: pi.Namespace}, &original); err != nil {
		if apierrors.IsNotFound(err) {
			// If its not present we are in an inconsitent state
			return newErrCritical("original pvc missing while trying to back it up")
		}
		return err
	}

	backup, err := r.getOrCreateBackup(ctx, pi)
	if err != nil {
		return err
	}
	if backup.Annotations != nil && backup.Annotations[DoneAnnotation] == "true" {
		// We ran successfully before
		return nil
	}
	q := backup.Spec.Resources.Requests[corev1.ResourceStorage]              // Necessary because pointer receiver
	if q.Cmp(original.Spec.Resources.Requests[corev1.ResourceStorage]) < 0 { // Returns -1 if q < size of original
		// That is not the correct PVC
		return newErrAbort(fmt.Sprintf("existing backup %s too small", backup.Name))
	}

	err = r.copyPVC(ctx,
		client.ObjectKey{Name: pi.Name, Namespace: pi.Namespace},
		client.ObjectKey{Name: pi.backupName(), Namespace: pi.Namespace})
	if err != nil {
		if errors.Is(err, errCritical) {
			// Critical errors in this stage can be aborted
			err := errors.Unwrap(err)
			return newErrAbort(err.Error())
		}
		return err
	}
	if backup.Annotations == nil {
		// This should generally not happen, but let's better not panic if it does
		backup.Annotations = map[string]string{}
	}
	// We ran successfully
	backup.Annotations[DoneAnnotation] = "true"
	return r.Update(ctx, backup)
}

func (r *StatefulSetReconciler) getOrCreateBackup(ctx context.Context, pi pvcInfo) (*corev1.PersistentVolumeClaim, error) {
	found := corev1.PersistentVolumeClaim{}
	backup := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pi.backupName(),
			Namespace: pi.Namespace,
			Labels: map[string]string{
				managedLabel: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      pi.Spec.AccessModes,
			Resources:        pi.Spec.Resources,
			StorageClassName: pi.Spec.StorageClassName,
			VolumeMode:       pi.Spec.VolumeMode,
		},
	}
	// Create backup destination if not exists
	if err := r.Get(ctx, client.ObjectKey{Name: pi.backupName(), Namespace: pi.Namespace}, &found); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		} else if apierrors.IsNotFound(err) {
			if err := r.Create(ctx, &backup); err != nil {
				return nil, err
			}
		}
	} else {
		// It already exists.
		backup = found
	}
	return &backup, nil
}
