package controllers

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/vshn/statefulset-resize-controller/pvc"
	"github.com/vshn/statefulset-resize-controller/statefulset"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// getResizablePVCs fetches the information of all PVCs that are smaller than the request of the statefulset
func fetchResizablePVCs(ctx context.Context, cl client.Client, si statefulset.Entity) ([]pvc.Entity, error) {
	// NOTE(glrf) This will get _all_ PVCs that belonged to the sts. Even the ones not used anymore (i.e. if scaled up and down).
	sts, err := si.StatefulSet()
	if err != nil {
		return nil, err
	}
	pvcs := corev1.PersistentVolumeClaimList{}
	if err := cl.List(ctx, &pvcs, client.InNamespace(sts.Namespace), client.MatchingLabels(sts.Spec.Selector.MatchLabels)); err != nil {
		return nil, err
	}
	pis := filterResizablePVCs(ctx, *sts, pvcs.Items)
	return pis, nil
}

// filterResizablePVCs filters out the PVCs that do not match the request of the statefulset
func filterResizablePVCs(ctx context.Context, sts appsv1.StatefulSet, pvcs []corev1.PersistentVolumeClaim) []pvc.Entity {
	var res []pvc.Entity

	for _, p := range pvcs {
		if p.Namespace != sts.Namespace {
			continue
		}
		for _, tpl := range sts.Spec.VolumeClaimTemplates {
			if isPVCTooSmall(ctx, p, tpl, sts.Name) {
				res = append(res, pvc.NewEntity(p, tpl.Spec.Resources.Requests[corev1.ResourceStorage], tpl.Spec.StorageClassName))
				break
			}
		}
	}
	return res
}

func isPVCTooSmall(ctx context.Context, p, tpl corev1.PersistentVolumeClaim, stsName string) bool {
	//TODO Test this separately
	// StS managed PVCs are created according to the VolumeClaimTemplate.
	// The name of the resulting PVC will be in the following format:
	// <template.name>-<sts.name>-<ordinal-number>
	// This allows us to match the pvcs to the template.

	// Very spammy but could help in error cases
	l := log.FromContext(ctx).WithValues("Namespace", p.Namespace, "Pvc", p.Name, "StatefulSet", stsName, "Template", tpl.Name).V(2)
	if !strings.HasPrefix(p.Name, tpl.Name) {
		l.Info("pvc does not match the template")
		return false
	}
	n := strings.TrimPrefix(p.Name, fmt.Sprintf("%s-", tpl.Name))
	if !strings.HasPrefix(n, stsName) {
		l.Info("pvc does not match the StatefulSet")
		return false
	}

	n = strings.TrimPrefix(n, fmt.Sprintf("%s-", stsName))
	if _, err := strconv.Atoi(n); err != nil {
		l.Info("pvc does not end in a number")
		return false
	}
	return isGreaterStorageRequest(p, tpl)
}

func isGreaterStorageRequest(p, tpl corev1.PersistentVolumeClaim) bool {
	q := p.Spec.Resources.Requests[corev1.ResourceStorage]
	return q.Cmp(tpl.Spec.Resources.Requests[corev1.ResourceStorage]) < 0 // Returns -1 if q < requested size
}

func (r *StatefulSetReconciler) resizePVC(ctx context.Context, pi pvc.Entity) (pvc.Entity, bool, error) {
	pi, done, err := r.backupPVC(ctx, pi)
	if err != nil || !done {
		if errors.As(err, &CriticalError{}) {
			err = CriticalError{
				Err:           err,
				Event:         fmt.Sprintf("Failed to backup PVC %s", pi.SourceName),
				SaveToScaleUp: true,
			}
		}
		return pi, done, err
	}
	return r.restorePVC(ctx, pi)
}

func (r *StatefulSetReconciler) resizePVCs(ctx context.Context, oldPIs []pvc.Entity) ([]pvc.Entity, error) {
	pis := []pvc.Entity{}
	for i, pi := range oldPIs {
		pi, done, err := r.resizePVC(ctx, pi)
		if err != nil {
			pis = append(pis, oldPIs[i:]...)
			return pis, err
		}
		if !done {
			pis = append(pis, pi)
		}
	}
	return pis, nil
}

func resizePVCsInplace(ctx context.Context, cl client.Client, PVCs []pvc.Entity) error {
	l := log.FromContext(ctx)

	for _, pvc := range PVCs {
		l.Info("Updating PVC", "PVCName", pvc.SourceName)

		resizedPVC := pvc.GetResizedSource()
		resizedPVC.Spec.StorageClassName = pvc.SourceStorageClass
		resizedPVC.Spec.VolumeName = pvc.Spec.VolumeName

		err := cl.Update(ctx, resizedPVC)
		if err != nil {
			return err
		}
	}

	return nil
}
