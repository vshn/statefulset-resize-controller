package controllers

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

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
