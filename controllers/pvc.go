package controllers

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const managedLabel = "sts-resize.appuio.ch/managed"

const doneAnnotation = "sts-resize.appuio.ch/done"

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

func (r *StatefulSetReconciler) backupPVC(ctx context.Context, pi pvcInfo) error {
	// Create backup destination if not exists
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
	if err := r.Get(ctx, client.ObjectKey{Name: pi.backupName(), Namespace: pi.Namespace}, &found); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		} else if apierrors.IsNotFound(err) {
			if err := r.Create(ctx, &backup); err != nil {
				return err
			}
		}
	} else {
		backup = found
		if backup.Annotations[doneAnnotation] == "true" {
			return nil
		}
	}
	// TODO(glrf) Do we need sanity checks? If someone creates a data-test-0-backup-4G that is only a gigabit large this will fail..

	err := r.copyPVC(ctx, client.ObjectKey{Name: pi.Name, Namespace: pi.Namespace}, client.ObjectKey{Name: pi.backupName(), Namespace: pi.Namespace})
	if err != nil {
		return err
	}
	backup.Annotations[doneAnnotation] = "true"
	return r.Update(ctx, &backup)
}

func (r *StatefulSetReconciler) restorePVC(ctx context.Context, pi pvcInfo) error {
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
	recreate := false // If we need to recreate the original PVC as it is too small
	err := r.Get(ctx, client.ObjectKey{Name: pi.Name, Namespace: pi.Namespace}, &found)
	if err != nil && apierrors.IsNotFound(err) {
		// The PVC does not exist. We probably crashed while recreating it.
		// Let's try again.
		recreate = true
	} else if err != nil {
		return err
	} else {
		pvc = found
		// There still is a pvc. Check if it it already large enough.
		// If not delete and receate it
		q := found.Spec.Resources.Requests[corev1.ResourceStorage]
		if q.Cmp(pi.TargetSize) < 0 {
			if err := r.Delete(ctx, &pvc); err != nil {
				return err
			}
			recreate = true
		} else {
			if pvc.Annotations[doneAnnotation] == "true" {
				return nil
			}
		}
	}

	if recreate {
		if err := r.Create(ctx, &pvc); err != nil {
			return err
		}
	}
	err = r.copyPVC(ctx, client.ObjectKey{Name: pi.backupName(), Namespace: pi.Namespace}, client.ObjectKey{Name: pi.Name, Namespace: pi.Namespace})
	if err != nil {
		return err
	}
	pvc.Annotations[doneAnnotation] = "true"
	return r.Update(ctx, &pvc)
}

func (r *StatefulSetReconciler) copyPVC(ctx context.Context, src client.ObjectKey, dst client.ObjectKey) error {
	if src.Namespace != dst.Namespace {
		return errors.New("unable to copy across namespaces")
	}
	name := fmt.Sprintf("sync-%s-to-%s", src.Name, dst.Name)
	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: src.Namespace,
			Labels: map[string]string{
				managedLabel: "true",
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "sync",
							Image:   "instrumentisto/rsync-ssh", // TODO(glrf) configurable
							Command: []string{"rsync", "-avhWHAX", "--no-compress", "--progress", "/src/", "/dst/"},
							VolumeMounts: []corev1.VolumeMount{
								{
									MountPath: "/src",
									Name:      "src",
								},
								{
									MountPath: "/dst",
									Name:      "dst",
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Volumes: []corev1.Volume{
						{
							Name: "src",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: src.Name,
									ReadOnly:  false,
								},
							},
						},
						{
							Name: "dst",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: dst.Name,
									ReadOnly:  false,
								},
							},
						},
					},
				},
			},
		},
	}
	fjob := batchv1.Job{}
	err := r.Client.Get(ctx, client.ObjectKeyFromObject(&job), &fjob)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	} else if apierrors.IsNotFound(err) {
		if err := r.Client.Create(ctx, &job); err != nil {
			return err
		}
	} else {
		// TODO(glrf) Do we need sanity checks? Mabye this is a different job?
		job = fjob
	}

	//TODO(glrf) Handle Failure!
	if job.Status.Succeeded > 0 {
		// We are done with this. Let's clean up the Job
		pol := metav1.DeletePropagationForeground
		return r.Client.Delete(ctx, &job, &client.DeleteOptions{
			PropagationPolicy: &pol,
		})
	}

	return errInProgress
}
