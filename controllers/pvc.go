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
	// Check if the original PVC still exists. If not there is a problem
	original := corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, client.ObjectKey{Name: pi.Name, Namespace: pi.Namespace}, &original); err != nil {
		if apierrors.IsNotFound(err) {
			// If its not present we are in an inconsitent state
			return newErrCritical("original pvc missing while trying to back it up")
		}
		return err
	}

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
			return err
		} else if apierrors.IsNotFound(err) {
			if err := r.Create(ctx, &backup); err != nil {
				return err
			}
		}
	} else {
		// It already exists.
		// We either are in progress of copying or we are done.
		backup = found
		if backup.Annotations[doneAnnotation] == "true" {
			// We ran successfully before
			return nil
		}
	}
	q := backup.Spec.Resources.Requests[corev1.ResourceStorage]              // Necessary because pointer receiver
	if q.Cmp(original.Spec.Resources.Requests[corev1.ResourceStorage]) < 0 { // Returns -1 if q < size of original
		return newErrAbort(fmt.Sprintf("existing backup %s too small", backup.Name))
	}

	err := r.copyPVC(ctx, client.ObjectKey{Name: pi.Name, Namespace: pi.Namespace}, client.ObjectKey{Name: pi.backupName(), Namespace: pi.Namespace})
	if errors.Is(err, errCritical) {
		// Critical errors in this stage can be aborted
		err := errors.Unwrap(err)
		return newErrAbort(err.Error())
	}
	if err != nil {
		return err
	}
	if backup.Annotations == nil {
		// This should generally not happen, but let's better not panic if it does
		backup.Annotations = map[string]string{}
	}
	// We ran successfully
	backup.Annotations[doneAnnotation] = "true"
	return r.Update(ctx, &backup)
}

func (r *StatefulSetReconciler) restorePVC(ctx context.Context, pi pvcInfo) error {
	// Check if the backup we want to restore from actually exists
	backupMissing := false
	backup := corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, client.ObjectKey{Name: pi.backupName(), Namespace: pi.Namespace}, &backup); err != nil {
		if !apierrors.IsNotFound(err) {
			// If its not present we are in an inconsitent state
			return err
		}
		backupMissing = true
	}
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
		if backupMissing {
			// backup and original is missing
			return newErrCritical(fmt.Sprintf("backup %s and original %s missing, state inconsitent", pi.backupName(), pi.Name))
		}
		// Let's recreate it with the target size
		if err := r.Create(ctx, &pvc); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		// There still is a pvc.
		if backupMissing {
			// backup is missing but the original is here. Abort
			return newErrAbort(fmt.Sprintf("backup %s missing, state inconsitent", pi.backupName()))
		}
		//Check if it it already large enough.
		// If not delete and receate it
		q := found.Spec.Resources.Requests[corev1.ResourceStorage]
		if q.Cmp(pi.TargetSize) < 0 {
			if err := r.Delete(ctx, &found); err != nil {
				return err
			}
			// The delete might take a while to take effect.
			// Let's backoff to avoid a race condition.
			return errInProgress
		}
		if found.Annotations[doneAnnotation] == "true" {
			return nil
		}
	}

	err = r.copyPVC(ctx, client.ObjectKey{Name: pi.backupName(), Namespace: pi.Namespace}, client.ObjectKey{Name: pi.Name, Namespace: pi.Namespace})
	if err != nil {
		return err
	}
	if pvc.Annotations == nil {
		// This should generally not happen, but let's better not panic if it does
		pvc.Annotations = map[string]string{}
	}
	pvc.Annotations[doneAnnotation] = "true"
	return r.Update(ctx, &pvc)
}

func (r *StatefulSetReconciler) copyPVC(ctx context.Context, src client.ObjectKey, dst client.ObjectKey) error {
	if src.Namespace != dst.Namespace {
		return newErrCritical("unable to copy across namespaces")
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
							Image:   r.SyncContainerImage,
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

	stat := getJobStatus(job)
	if stat == nil {
		// Job still running
		return errInProgress
	}
	switch *stat {
	case batchv1.JobComplete:
		// We are done with this. Let's clean up the Job
		// If we don't we won't be able to mount it in the next step
		pol := metav1.DeletePropagationForeground
		err := r.Client.Delete(ctx, &job, &client.DeleteOptions{
			PropagationPolicy: &pol,
		})
		return err
	case batchv1.JobFailed:
		return newErrCritical(fmt.Sprintf("job %s failed", job.Name))
	default:
		return newErrCritical(fmt.Sprintf("job %s in unknown state", job.Name))
	}
}

func getJobStatus(job batchv1.Job) *batchv1.JobConditionType {
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobComplete || cond.Type == batchv1.JobFailed {
			return &cond.Type
		}
	}
	return nil
}
