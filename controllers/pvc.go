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
)

// getResizablePVCs fetches the information of all PVCs that are smaller than the request of the statefulset
func (r StatefulSetReconciler) fetchResizablePVCs(ctx context.Context, si statefulset.Entity) ([]pvc.Entity, error) {
	// NOTE(glrf) This will get _all_ PVCs that belonged to the sts. Even the ones not used anymore (i.e. if scaled up and down).
	sts, err := si.StatefulSet()
	if err != nil {
		return nil, err
	}
	pvcs := corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, &pvcs, client.InNamespace(sts.Namespace), client.MatchingLabels(sts.Spec.Selector.MatchLabels)); err != nil {
		return nil, err
	}
	pis := filterResizablePVCs(*sts, pvcs.Items)
	return pis, nil
}

// filterResizablePVCs filters out the PVCs that do not match the request of the statefulset
func filterResizablePVCs(sts appsv1.StatefulSet, pvcs []corev1.PersistentVolumeClaim) []pvc.Entity {
	// StS managed PVCs are created according to the VolumeClaimTemplate.
	// The name of the resulting PVC will be in the following format:
	// <template.name>-<sts.name>-<ordinal-number>
	// This allows us to match the pvcs to the template.

	var res []pvc.Entity

	for _, p := range pvcs {
		if p.Namespace != sts.Namespace {
			continue
		}
		for _, tpl := range sts.Spec.VolumeClaimTemplates {
			if isPVCTooSmall(p, tpl, sts.Name) {
				res = append(res, pvc.NewEntity(p, tpl.Spec.Resources.Requests[corev1.ResourceStorage]))
				break
			}
		}
	}
	return res
}

func isPVCTooSmall(p, tpl corev1.PersistentVolumeClaim, stsName string) bool {
	//TODO Test this separately
	if !strings.HasPrefix(p.Name, tpl.Name) {
		return false
	}
	n := strings.TrimPrefix(p.Name, fmt.Sprintf("%s-", tpl.Name))

	if !strings.HasPrefix(n, stsName) {
		return false
	}
	n = strings.TrimPrefix(n, fmt.Sprintf("%s-", stsName))

	if _, err := strconv.Atoi(n); err != nil {
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
