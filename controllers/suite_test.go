package controllers

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	//+kubebuilder:scaffold:imports
)

var cfg *rest.Config
var k8sClient client.Client
var testEnv *envtest.Environment

func TestMain(m *testing.M) {
	testEnv = &envtest.Environment{}
	cfg, err := testEnv.Start()
	if err != nil {
		log.Fatalf("Failed to start testEnv: %v", err)
	}
	defer testEnv.Stop()

	err = appsv1.AddToScheme(scheme.Scheme)
	if err != nil {
		log.Fatalf("Failed to add scheme to testEnv: %v", err)
	}

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		log.Fatalf("Failed to get client for testEnv: %v", err)
	}
	m.Run()
}

// Some helper functions
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

func newTestJob(namespace string, src, dst client.ObjectKey, image string, state *batchv1.JobConditionType, fs ...func(*batchv1.Job) *batchv1.Job) *batchv1.Job {
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
		assert.ObjectsAreEqual(job.Labels, other.Labels)
}

func jobNotExists(ctx context.Context, c client.Client, other *batchv1.Job) bool {
	job := &batchv1.Job{}
	key := client.ObjectKeyFromObject(other)
	err := c.Get(ctx, key, job)
	// This is needed as the testenv does not properly clean up jobs
	// Their stuck as there is a finalizer to remove pods
	return apierrors.IsNotFound(err) || (err == nil && job.DeletionTimestamp != nil)
}
