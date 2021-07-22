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
func (r StatefulSetReconciler) fetchResizablePVCs(ctx context.Context, si statefulset.Info) ([]pvc.Info, error) {
	// NOTE(glrf) This will get _all_ PVCs that belonged to the sts. Even the ones not used anymore (i.e. if scaled up and down).
	sts, err := si.Sts()
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
func filterResizablePVCs(sts appsv1.StatefulSet, pvcs []corev1.PersistentVolumeClaim) []pvc.Info {
	// StS managed PVCs are created according to the VolumeClaimTemplate.
	// The name of the resulting PVC will be in the following format:
	// <template.name>-<sts.name>-<ordinal-number>
	// This allows us to match the pvcs to the template.

	var res []pvc.Info

	for _, p := range pvcs {
		if p.Namespace != sts.Namespace {
			continue
		}
		for _, tpl := range sts.Spec.VolumeClaimTemplates {
			if !strings.HasPrefix(p.Name, tpl.Name) {
				continue
			}
			n := strings.TrimPrefix(p.Name, fmt.Sprintf("%s-", tpl.Name))
			if !strings.HasPrefix(n, sts.Name) {
				continue
			}
			n = strings.TrimPrefix(n, fmt.Sprintf("%s-", sts.Name))
			if _, err := strconv.Atoi(n); err != nil {
				continue
			}
			q := p.Spec.Resources.Requests[corev1.ResourceStorage]
			if q.Cmp(tpl.Spec.Resources.Requests[corev1.ResourceStorage]) < 0 { // Returns -1 if q < requested size
				res = append(res, pvc.NewInfo(p, tpl.Spec.Resources.Requests[corev1.ResourceStorage]))
				break
			}
		}
	}
	return res
}

func (r *StatefulSetReconciler) resizePVC(ctx context.Context, pi pvc.Info) (pvc.Info, bool, error) {
	pi, done, err := r.backupPVC(ctx, pi)
	if err != nil || !done {
		cerr := CriticalError{}
		if errors.As(err, &cerr) {
			fmt.Println("Got crit error")
			err = CriticalError{
				Err:           err,
				Event:         fmt.Sprintf("Failed to backup PVC %s", pi.SourceName),
				SaveToScaleUp: true,
			}
		}
		return pi, done, err
	}
	pi, done, err = r.restorePVC(ctx, pi)
	if err != nil || !done {
		return pi, done, err
	}
	return pi, true, nil
}
