//+build integration

package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	//+kubebuilder:scaffold:imports
)

var timeout = time.Second * 10
var duration = time.Second * 4
var interval = time.Millisecond * 300

func newSource(namespace, name, size string, fs ...func(*corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
	volumeMode := corev1.PersistentVolumeFilesystem
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"foo": "bar",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceStorage: resource.MustParse(size),
				},
			},
			StorageClassName: nil,
			VolumeMode:       &volumeMode,
		},
	}
	for _, f := range fs {
		pvc = f(pvc)
	}
	return pvc

}
func newBackup(namespace, name, size string, fs ...func(*corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
	volumeMode := corev1.PersistentVolumeFilesystem
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				ManagedLabel: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceStorage: resource.MustParse(size),
				},
			},
			StorageClassName: nil,
			VolumeMode:       &volumeMode,
		},
	}
	for _, f := range fs {
		pvc = f(pvc)
	}
	return pvc
}

func newTestJob(namespace string, src, dst client.ObjectKey, image string, saname string, state *batchv1.JobConditionType, fs ...func(*batchv1.Job) *batchv1.Job) *batchv1.Job {
	name := fmt.Sprintf("sync-%s-to-%s", src.Name, dst.Name)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
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
							// These are just the defaults. We don't actually care but it makes comparing easier
							TerminationMessagePath:   "/dev/termination-log",
							TerminationMessagePolicy: "File",
							ImagePullPolicy:          "Always",
						},
					},
					RestartPolicy:      corev1.RestartPolicyOnFailure,
					ServiceAccountName: saname,
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

	if state != nil {
		cond := batchv1.JobCondition{
			Type:   *state,
			Status: corev1.ConditionTrue,
		}
		job.Status.Conditions = append(job.Status.Conditions, cond)
		if *state == batchv1.JobComplete {
			job.Status.Succeeded = 1
		}
	}

	for _, f := range fs {
		job = f(job)
	}
	return job
}

func newTestStatefulSet(namespace, name string, replicas int32, size string) *appsv1.StatefulSet {
	l := map[string]string{
		"app": name,
	}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: l,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: l,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test",
							Image: "test",
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "data",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						Resources: corev1.ResourceRequirements{
							Requests: map[corev1.ResourceName]resource.Quantity{
								corev1.ResourceStorage: resource.MustParse(size),
							},
						},
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
					},
				},
			},
			ServiceName: name,
		},
	}
}

func newTestSA(namespace, objname string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objname,
			Namespace: namespace,
			Labels: map[string]string{
				ManagedLabel: "true",
			},
		},
	}
}

func newTestRB(namespace, objname, crname string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objname,
			Namespace: namespace,
			Labels: map[string]string{
				ManagedLabel: "true",
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     crname,
		},
		Subjects: []rbacv1.Subject{
			rbacv1.Subject{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      objname,
				Namespace: namespace,
			},
		},
	}
}

var jobSucceeded = batchv1.JobComplete
var jobFailed = batchv1.JobFailed

func pvcExists(ctx context.Context, c client.Client, other *corev1.PersistentVolumeClaim) bool {
	pvc := &corev1.PersistentVolumeClaim{}
	key := client.ObjectKeyFromObject(other)
	if err := c.Get(ctx, key, pvc); err != nil {
		return false
	}
	return assert.ObjectsAreEqual(pvc.Spec, other.Spec) && assert.ObjectsAreEqual(pvc.Labels, other.Labels)
}

func pvcNotExists(ctx context.Context, c client.Client, other *corev1.PersistentVolumeClaim) bool {
	pvc := &corev1.PersistentVolumeClaim{}
	key := client.ObjectKeyFromObject(other)
	err := c.Get(ctx, key, pvc)
	// This is needed as the testenv does not properly clean up pvcs
	return apierrors.IsNotFound(err) || (err == nil && pvc.DeletionTimestamp != nil)
}

func jobExists(ctx context.Context, c client.Client, other *batchv1.Job) bool {
	job := &batchv1.Job{}
	key := client.ObjectKeyFromObject(other)
	if err := c.Get(ctx, key, job); err != nil {
		return false
	}
	return assert.ObjectsAreEqual(job.Spec.Template.Spec.Containers, other.Spec.Template.Spec.Containers) &&
		assert.ObjectsAreEqual(job.Spec.Template.Spec.Volumes, other.Spec.Template.Spec.Volumes) &&
		assert.ObjectsAreEqual(job.Labels, other.Labels) &&
		job.Spec.Template.Spec.ServiceAccountName == other.Spec.Template.Spec.ServiceAccountName
}

func stsExists(ctx context.Context, c client.Client, other *appsv1.StatefulSet) bool {
	sts := &appsv1.StatefulSet{}
	key := client.ObjectKeyFromObject(other)
	if err := c.Get(ctx, key, sts); err != nil {
		return false
	}
	return assert.ObjectsAreEqual(sts.Spec, other.Spec) && assert.ObjectsAreEqual(sts.Labels, other.Labels)
}

func saExists(ctx context.Context, c client.Client, other *corev1.ServiceAccount) bool {
	sa := &corev1.ServiceAccount{}
	key := client.ObjectKeyFromObject(other)
	if err := c.Get(ctx, key, sa); err != nil {
		return false
	}
	return sa.Name == other.Name && sa.Namespace == other.Namespace &&
		assert.ObjectsAreEqual(sa.Labels, other.Labels)
}

func saNotExists(ctx context.Context, c client.Client, other *corev1.ServiceAccount) bool {
	sa := &corev1.ServiceAccount{}
	key := client.ObjectKeyFromObject(other)
	err := c.Get(ctx, key, sa)
	return apierrors.IsNotFound(err) || (err == nil && sa.DeletionTimestamp != nil)
}

func rbExists(ctx context.Context, c client.Client, other *rbacv1.RoleBinding) bool {
	rb := &rbacv1.RoleBinding{}
	key := client.ObjectKeyFromObject(other)
	if err := c.Get(ctx, key, rb); err != nil {
		return false
	}
	return rb.Name == other.Name && rb.Namespace == other.Namespace &&
		assert.ObjectsAreEqual(rb.Labels, other.Labels)
}

func rbNotExists(ctx context.Context, c client.Client, other *rbacv1.RoleBinding) bool {
	rb := &rbacv1.RoleBinding{}
	key := client.ObjectKeyFromObject(other)
	err := c.Get(ctx, key, rb)
	return apierrors.IsNotFound(err) || (err == nil && rb.DeletionTimestamp != nil)
}

// Only succeeds if the condition is valid for `waitFor` time.
// Checks the condition every `tick`
func consistently(t assert.TestingT, condition func() bool, waitFor time.Duration, tick time.Duration, msgAndArgs ...interface{}) bool {
	after := time.After(waitFor)

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for tick := ticker.C; ; {
		select {
		case <-after:
			return true
		case <-tick:
			if !condition() {
				return assert.Fail(t, "Condition not satisfied", msgAndArgs...)
			}
		}
	}
}
