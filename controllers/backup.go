package controllers

import (
	"context"
	"errors"
	"reflect"

	"github.com/vshn/statefulset-resize-controller/pvc"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r StatefulSetReconciler) backupPVC(ctx context.Context, pi pvc.Info) (pvc.Info, bool, error) {
	if pi.BackedUp {
		return pi, true, nil
	}

	err := r.createBackupIfNotExists(ctx, pi)

	done, err := r.copyPVC(ctx,
		client.ObjectKey{Name: pi.SourceName, Namespace: pi.Namespace},
		client.ObjectKey{Name: pi.BackupName(), Namespace: pi.Namespace})
	if err != nil || !done {
		return pi, done, err
	}

	pi.BackedUp = true

	return pi, true, nil
}

func (r StatefulSetReconciler) createBackupIfNotExists(ctx context.Context, pi pvc.Info) error {
	found := corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, client.ObjectKey{Name: pi.BackupName(), Namespace: pi.Namespace}, &found)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, pi.GetBackup())
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(found.Spec, pi.GetBackup().Spec) {
		return errors.New("exiting backup does not match requirements")
	}
	return err
}
