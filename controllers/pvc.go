package controllers

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const managedLabel = "sts-resize.appuio.ch/managed"

const DoneAnnotation = "sts-resize.appuio.ch/done"

// pvcInfo describs a resizable PVC
type pvcInfo struct {
	Name       string
	Namespace  string
	Labels     map[string]string
	Spec       corev1.PersistentVolumeClaimSpec
	TargetSize resource.Quantity
}

func (pi pvcInfo) backupName() string {
	q := pi.Spec.Resources.Requests[corev1.ResourceStorage] // Necessary because pointer receiver
	return strings.ToLower(fmt.Sprintf("%s-backup-%s", pi.Name, q.String()))
}

// getResizablePVCs fetches the information of all PVCs that are smaller than the request of the statefulset
func getResizablePVCs(ctx context.Context, c client.Reader, sts appsv1.StatefulSet) ([]pvcInfo, error) {
	// NOTE(glrf) This will get _all_ PVCs that belonged to the sts. Even the ones not used anymore (i.e. if scaled up and down)
	pvcs := corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, &pvcs, client.InNamespace(sts.Namespace), client.MatchingLabels(sts.Spec.Selector.MatchLabels)); err != nil {
		return nil, err
	}
	pis := filterResizablePVCs(sts, pvcs.Items)
	return pis, nil
}

// filterResizablePVCs filters out the PVCs that do not match the request of the statefulset
func filterResizablePVCs(sts appsv1.StatefulSet, pvcs []corev1.PersistentVolumeClaim) []pvcInfo {
	// StS managed PVCs are created according to the VolumeClaimTemplate.
	// The name of the resulting PVC will be in the following format
	// <template.name>-<sts.name>-<ordinal-number>
	// This allows us to match the pvcs to the template

	var res []pvcInfo

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
				res = append(res, pvcInfo{
					Name:       pvc.Name,
					Namespace:  pvc.Namespace,
					Labels:     pvc.Labels,
					TargetSize: tpl.Spec.Resources.Requests[corev1.ResourceStorage],
					Spec:       pvc.Spec,
				})
				break
			}
		}
	}
	return res
}

// resizePVC is an idempotent function that will make sure the PVC in the pvcInfo will grow to the requested size
// This function might not run through successfully in a single run but may return an `errInProgress`, signifying
// that the caller needs to retry later.
func (r *StatefulSetReconciler) resizePVC(ctx context.Context, pi pvcInfo) error {
	if err := r.backupPVC(ctx, pi); err != nil {
		return err
	}
	if err := r.restorePVC(ctx, pi); err != nil {
		return err
	}
	return nil
}
