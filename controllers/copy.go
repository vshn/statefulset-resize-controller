package controllers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/vshn/statefulset-resize-controller/naming"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ManagedLabel is a label to mark resources to be managed by the controller
const ManagedLabel = "sts-resize.vshn.net/managed"

func (r *StatefulSetReconciler) copyPVC(ctx context.Context, src client.ObjectKey, dst client.ObjectKey) (bool, error) {
	if src.Namespace != dst.Namespace {
		return false, errors.New("unable to copy across namespaces")
	}

	job := newJob(src.Namespace, r.SyncContainerImage, src.Name, dst.Name)
	job, err := r.getOrCreateJob(ctx, job)
	if err != nil {
		return false, err
	}

	done, err := isJobDone(job)
	if err != nil {
		return done, err
	}
	if done {
		// Let's clean up the Job
		// If we don't we won't be able to mount it in the next step
		pol := metav1.DeletePropagationForeground
		err = r.Client.Delete(ctx, &job, &client.DeleteOptions{
			PropagationPolicy: &pol,
		})
	}
	return done, nil
}

func (r *StatefulSetReconciler) getOrCreateJob(ctx context.Context, job batchv1.Job) (batchv1.Job, error) {
	found := batchv1.Job{}
	err := r.Client.Get(ctx, client.ObjectKeyFromObject(&job), &found)
	if apierrors.IsNotFound(err) {
		return job, r.Client.Create(ctx, &job)
	}
	if err != nil {
		return job, err
	}
	return found, nil
}

func newJobName(src, dst string) string {
	maxNameLength := 27
	// The ignored errors are impossible
	src, _ = naming.ShortenName(src, maxNameLength)
	dst, _ = naming.ShortenName(dst, maxNameLength)
	return strings.ToLower(fmt.Sprintf("sync-%s-to-%s", src, dst))
}

func newJob(namespace, image, src, dst string) batchv1.Job {
	return batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      newJobName(src, dst),
			Namespace: namespace,
			Labels: map[string]string{
				ManagedLabel: "true",
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "sync",
							Image:   image,
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
									ClaimName: src,
									ReadOnly:  false,
								},
							},
						},
						{
							Name: "dst",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: dst,
									ReadOnly:  false,
								},
							},
						},
					},
				},
			},
		},
	}
}

func isJobDone(job batchv1.Job) (bool, error) {
	for _, cond := range job.Status.Conditions {
		if cond.Status != corev1.ConditionTrue {
			continue
		}
		if cond.Type == batchv1.JobComplete {
			return true, nil
		}
		if cond.Type == batchv1.JobFailed {
			return true, CriticalError{Err: fmt.Errorf("job %s failed", job.Name)}
		}
	}
	return false, nil
}
