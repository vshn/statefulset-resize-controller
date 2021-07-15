package controllers

import (
	"fmt"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/envtest/printer"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	//+kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var cfg *rest.Config
var k8sClient client.Client
var testEnv *envtest.Environment

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecsWithDefaultAndCustomReporters(t,
		"Controller Suite",
		[]Reporter{printer.NewlineReporter{}})
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: false,
	}

	cfg, err := testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	err = appsv1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	//+kubebuilder:scaffold:scheme

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

}, 60)

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})

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
				managedLabel: "true",
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
				managedLabel: "true",
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

func BeEquivalentPVC(other *corev1.PersistentVolumeClaim) types.GomegaMatcher {
	return SatisfyAll(
		WithTransform(
			func(pvc *corev1.PersistentVolumeClaim) corev1.PersistentVolumeClaimSpec {
				return pvc.Spec
			},
			BeEquivalentTo(other.Spec),
		),
		WithTransform(
			func(pvc *corev1.PersistentVolumeClaim) map[string]string {
				return pvc.ObjectMeta.Labels
			},
			BeEquivalentTo(other.ObjectMeta.Labels),
		),
	)
}
func BeEquivalentJob(other *batchv1.Job) types.GomegaMatcher {
	if other == nil {
		other = &batchv1.Job{}
		return BeEquivalentTo(other)
	}
	return SatisfyAll(
		WithTransform(
			func(job *batchv1.Job) []corev1.Container {
				return job.Spec.Template.Spec.Containers
			},
			BeEquivalentTo(other.Spec.Template.Spec.Containers),
		),
		WithTransform(
			func(job *batchv1.Job) []corev1.Volume {
				return job.Spec.Template.Spec.Volumes
			},
			BeEquivalentTo(other.Spec.Template.Spec.Volumes),
		),
		WithTransform(
			func(job *batchv1.Job) map[string]string {
				return job.ObjectMeta.Labels
			},
			BeEquivalentTo(other.ObjectMeta.Labels),
		),
	)
}
