package controllers

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("filterResizablePVCs", func() {
	type tmpl struct {
		name string
		size string
	}
	type stsIn struct {
		name      string
		namespace string
		templates []tmpl
	}
	type pvcIn struct {
		name      string
		namespace string
		size      string
	}
	type tCase struct {
		sts  stsIn
		pvcs []pvcIn
		out  map[string]string
	}

	tcs := map[string]tCase{
		"finds nothing resizable": {
			sts: stsIn{
				name:      "test",
				namespace: "foo",
				templates: []tmpl{
					{
						name: "data",
						size: "1G",
					},
				},
			},
			pvcs: []pvcIn{
				{
					name:      "data-test-0",
					namespace: "foo",
					size:      "1G",
				},
				{
					name:      "data-test-1",
					namespace: "foo",
					size:      "1G",
				},
			},
			out: map[string]string{},
		},
		"finds resizable": {
			sts: stsIn{
				name:      "test",
				namespace: "foo",
				templates: []tmpl{
					{
						name: "data",
						size: "10G",
					},
				},
			},
			pvcs: []pvcIn{
				{
					name:      "data-test-0",
					namespace: "foo",
					size:      "1G",
				},
				{
					name:      "data-test-1",
					namespace: "foo",
					size:      "1G",
				},
			},
			out: map[string]string{
				"foo:data-test-0": "10G",
				"foo:data-test-1": "10G",
			},
		},
		"does not find resizable in other namespaces": {
			sts: stsIn{
				name:      "test",
				namespace: "foo",
				templates: []tmpl{
					{
						name: "data",
						size: "10G",
					},
				},
			},
			pvcs: []pvcIn{
				{
					name:      "data-test-0",
					namespace: "bar",
					size:      "1G",
				},
				{
					name:      "data-test-1",
					namespace: "bar",
					size:      "1G",
				},
			},
			out: map[string]string{},
		},
		"finds multiple resizable": {
			sts: stsIn{
				name:      "test",
				namespace: "foo",
				templates: []tmpl{
					{
						name: "data",
						size: "10G",
					},
					{
						name: "log",
						size: "100G",
					},
				},
			},
			pvcs: []pvcIn{
				{
					name:      "data-test-0",
					namespace: "foo",
					size:      "1G",
				},
				{
					name:      "data-test-1",
					namespace: "foo",
					size:      "1G",
				},
				{
					name:      "log-test-0",
					namespace: "foo",
					size:      "15G",
				},
				{
					name:      "log-test-1",
					namespace: "foo",
					size:      "15G",
				},
			},
			out: map[string]string{
				"foo:data-test-0": "10G",
				"foo:data-test-1": "10G",
				"foo:log-test-0":  "100G",
				"foo:log-test-1":  "100G",
			},
		},
		"filters out name colisions": {
			sts: stsIn{
				name:      "test",
				namespace: "foo",
				templates: []tmpl{
					{
						name: "data",
						size: "10G",
					},
					{
						name: "log",
						size: "100G",
					},
				},
			},
			pvcs: []pvcIn{
				{
					name:      "data-test-0",
					namespace: "foo",
					size:      "1G",
				},
				{
					name:      "data-test-1",
					namespace: "foo",
					size:      "1G",
				},
				{
					name:      "data-prod-1",
					namespace: "foo",
					size:      "1G",
				},
				{
					name:      "log-test-other-0", // template "log-test" from StS "other"
					namespace: "foo",
					size:      "10G",
				},
				{
					name:      "log-test-other-1",
					namespace: "foo",
					size:      "10G",
				},
			},
			out: map[string]string{
				"foo:data-test-0": "10G",
				"foo:data-test-1": "10G",
			},
		},
		"filters out unrelated PVCs": {
			sts: stsIn{
				name:      "test",
				namespace: "foo",
				templates: []tmpl{
					{
						name: "data",
						size: "10G",
					},
					{
						name: "log",
						size: "100G",
					},
				},
			},
			pvcs: []pvcIn{
				{
					name:      "data-test-0",
					namespace: "foo",
					size:      "1G",
				},
				{
					name:      "data-test-1",
					namespace: "foo",
					size:      "1G",
				},
				{
					name:      "log-test-pvc", // unrelated pvc someone created
					namespace: "foo",
					size:      "10G",
				},
			},
			out: map[string]string{
				"foo:data-test-0": "10G",
				"foo:data-test-1": "10G",
			},
		},
	}

	for k, tc := range tcs {
		tc := tc // necessary because Ginkgo weirdness
		It(k, func() {
			sts := appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: tc.sts.name, Namespace: tc.sts.namespace},
				Spec: appsv1.StatefulSetSpec{
					VolumeClaimTemplates: []corev1.PersistentVolumeClaim{},
				},
			}
			for _, t := range tc.sts.templates {
				sts.Spec.VolumeClaimTemplates = append(sts.Spec.VolumeClaimTemplates,
					corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name: t.name,
						},
						Spec: corev1.PersistentVolumeClaimSpec{
							Resources: corev1.ResourceRequirements{
								Requests: map[corev1.ResourceName]resource.Quantity{
									corev1.ResourceStorage: resource.MustParse(t.size),
								},
							},
						},
					})
			}

			pvcs := []corev1.PersistentVolumeClaim{}

			for _, p := range tc.pvcs {
				pvcs = append(pvcs, corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      p.name,
						Namespace: p.namespace,
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						Resources: corev1.ResourceRequirements{
							Requests: map[corev1.ResourceName]resource.Quantity{
								corev1.ResourceStorage: resource.MustParse(p.size),
							},
						},
					},
				})
			}

			rps := filterResizablePVCs(sts, pvcs)
			Expect(rps).To(HaveLen(len(tc.out)))
			for _, r := range rps {
				o, ok := tc.out[fmt.Sprintf("%s:%s", r.Namespace, r.Name)]
				Expect(ok).To(BeTrue())
				Expect(r.TargetSize.String()).To(Equal(o))
			}
		})
	}
})

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

func newJob(namespace string, src, dst client.ObjectKey, image string, fs ...func(*batchv1.Job) *batchv1.Job) *batchv1.Job {
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

	for _, f := range fs {
		job = f(job)
	}
	return job
}
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

var _ = Describe("backupPVC", func() {
	timeout := time.Second * 10
	interval := time.Millisecond * 250

	type state struct {
		source *corev1.PersistentVolumeClaim
		backup *corev1.PersistentVolumeClaim
		job    *batchv1.Job
	}
	type tCase struct {
		namespace  string
		in         state
		out        state
		targetSize string
		syncImage  string
		done       bool
		fail       bool
	}

	tcs := map[string]tCase{
		"initiate backup": {
			namespace: "t1",
			syncImage: "blub",
			in: state{
				source: newSource("t1", "test", "1G"),
			},
			out: state{
				source: newSource("t1", "test", "1G"),
				backup: newBackup("t1", "test-backup-1g", "1G"),
				job: newJob("t1",
					client.ObjectKey{Namespace: "t1", Name: "test"},
					client.ObjectKey{Namespace: "t1", Name: "test-backup-1g"},
					"blub"),
			},
			targetSize: "4G",
			done:       false,
		},
		"wait to complete": {
			namespace: "t2",
			syncImage: "blub",
			in: state{
				source: newSource("t2", "test", "1G"),
				backup: newBackup("t2", "test-backup-1g", "1G"),
				job: newJob("t2",
					client.ObjectKey{Namespace: "t1", Name: "test"},
					client.ObjectKey{Namespace: "t1", Name: "test-backup-1g"},
					"blub"),
			},
			out: state{
				source: newSource("t2", "test", "1G"),
				backup: newBackup("t2", "test-backup-1g", "1G"),
				job: newJob("t2",
					client.ObjectKey{Namespace: "t1", Name: "test"},
					client.ObjectKey{Namespace: "t1", Name: "test-backup-1g"},
					"blub"),
			},
			targetSize: "4G",
			done:       false,
		},
		"complete transfer and remove job": {
			namespace: "t3",
			syncImage: "blub",
			in: state{
				source: newSource("t3", "test", "1G"),
				backup: newBackup("t3", "test-backup-1g", "1G"),
				job: newJob("t3",
					client.ObjectKey{Namespace: "t3", Name: "test"},
					client.ObjectKey{Namespace: "t3", Name: "test-backup-1g"},
					"blub",
					func(job *batchv1.Job) *batchv1.Job {
						job.Status.Succeeded = 1
						return job
					}),
			},
			out: state{
				source: newSource("t3", "test", "1G"),
				backup: newBackup("t3", "test-backup-1g", "1G"),
				job:    nil,
			},
			targetSize: "4G",
			done:       true,
		},
		"restart sync if it didn't start properly": {
			namespace: "t4",
			syncImage: "blub",
			in: state{
				source: newSource("t4", "test", "1G"),
				backup: newBackup("t4", "test-backup-1g", "1G"),
			},
			out: state{
				source: newSource("t4", "test", "1G"),
				backup: newBackup("t4", "test-backup-1g", "1G"),
				job: newJob("t4",
					client.ObjectKey{Namespace: "t4", Name: "test"},
					client.ObjectKey{Namespace: "t4", Name: "test-backup-1g"},
					"blub"),
			},
			targetSize: "4G",
		},
		"don't sync again": {
			namespace: "t5",
			syncImage: "blub",
			in: state{
				source: newSource("t5", "test", "1G"),
				backup: newBackup("t5", "test-backup-1g", "1G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
			},
			out: state{
				source: newSource("t5", "test", "1G"),
				backup: newBackup("t5", "test-backup-1g", "1G"),
			},
			targetSize: "4G",
			done:       true,
		},
	}

	for k, tc := range tcs {
		tc := tc
		ctx := context.Background()
		k := k

		It(k, func() {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: tc.namespace},
			})).To(Succeed())
			if tc.in.source != nil {
				Expect(k8sClient.Create(ctx, tc.in.source)).To(Succeed())
			}
			if tc.in.backup != nil {
				Expect(k8sClient.Create(ctx, tc.in.backup)).To(Succeed())
			}
			if tc.in.job != nil {
				stat := tc.in.job.Status
				Expect(k8sClient.Create(ctx, tc.in.job)).To(Succeed())
				tc.in.job.Status = stat // Create removes status
				Expect(k8sClient.Status().Update(ctx, tc.in.job)).To(Succeed())
			}
			r := StatefulSetReconciler{
				Client:             k8sClient,
				Scheme:             k8sClient.Scheme(),
				SyncContainerImage: tc.syncImage,
			}
			Expect(tc.in.source).NotTo(BeNil())
			err := r.backupPVC(ctx, pvcInfo{
				Name:       tc.in.source.Name,
				Namespace:  tc.in.source.Namespace,
				Labels:     tc.in.source.Labels,
				Spec:       tc.in.source.Spec,
				TargetSize: resource.MustParse(tc.targetSize),
			})

			if tc.fail {
				Expect(err).NotTo(Succeed())
				Expect(err).NotTo(MatchError(errInProgress))
				return
			}
			if !tc.done {
				Expect(err).To(MatchError(errInProgress))
			} else {
				Expect(err).To(Succeed())
			}

			if tc.out.source != nil {
				Eventually(func() (*corev1.PersistentVolumeClaim, error) {
					key := client.ObjectKeyFromObject(tc.out.source)
					pvc := &corev1.PersistentVolumeClaim{}
					err := k8sClient.Get(ctx, key, pvc)
					return pvc, err
				}, timeout, interval).Should(BeEquivalentPVC(tc.out.source))
			} else if tc.in.source != nil {
				Eventually(func() metav1.StatusReason {
					key := client.ObjectKeyFromObject(tc.in.source)
					pvc := &corev1.PersistentVolumeClaim{}
					return apierrors.ReasonForError(k8sClient.Get(ctx, key, pvc))
				}, timeout, interval).Should(Equal(metav1.StatusReasonNotFound))
			}
			if tc.out.backup != nil {
				Eventually(func() (*corev1.PersistentVolumeClaim, error) {
					key := client.ObjectKeyFromObject(tc.out.backup)
					pvc := &corev1.PersistentVolumeClaim{}
					err := k8sClient.Get(ctx, key, pvc)
					return pvc, err
				}, timeout, interval).Should(BeEquivalentPVC(tc.out.backup))
			} else if tc.in.backup != nil {
				Eventually(func() metav1.StatusReason {
					key := client.ObjectKeyFromObject(tc.in.backup)
					pvc := &corev1.PersistentVolumeClaim{}
					return apierrors.ReasonForError(k8sClient.Get(ctx, key, pvc))
				}, timeout, interval).Should(Equal(metav1.StatusReasonNotFound))
			}
			if tc.out.job != nil {
				Eventually(func() (*batchv1.Job, error) {
					key := client.ObjectKeyFromObject(tc.out.job)
					job := &batchv1.Job{}
					err := k8sClient.Get(ctx, key, job)
					return job, err
				}, timeout, interval).Should(BeEquivalentJob(tc.out.job))
			} else if tc.in.job != nil {
				Eventually(func() metav1.StatusReason {
					key := client.ObjectKeyFromObject(tc.in.job)
					job := &batchv1.Job{}
					if err := k8sClient.Get(ctx, key, job); err != nil {
						return apierrors.ReasonForError(err)
					}
					if job.DeletionTimestamp != nil {
						// This is needed as the testenv does not properly clean up jobs
						// Their stuck as there is a finalizer to remove pods
						return metav1.StatusReasonNotFound
					}
					return "found"
				}, timeout, interval).Should(Equal(metav1.StatusReasonNotFound))
			}
		})
	}
})

var _ = Describe("restorePVC", func() {
	timeout := time.Second * 10
	interval := time.Millisecond * 250

	type state struct {
		source *corev1.PersistentVolumeClaim
		backup *corev1.PersistentVolumeClaim
		job    *batchv1.Job
	}
	type tCase struct {
		namespace  string
		pvcInfo    *pvcInfo
		in         state
		out        state
		targetSize string
		syncImage  string
		done       bool
		fail       bool
	}

	tcs := map[string]tCase{
		"initiate retore": {
			namespace: "r1",
			syncImage: "blub",
			in: state{
				source: newSource("r1", "test", "1G"),
				backup: newBackup("r1", "test-backup-1g", "1G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
			},
			out: state{
				backup: newBackup("r1", "test-backup-1g", "1G"),
			},
			targetSize: "4G",
			done:       false,
		},
		"recreate source pvc and start restore": {
			namespace: "r2",
			syncImage: "blub",
			pvcInfo: &pvcInfo{
				Name:      "test",
				Namespace: "r2",
				Labels:    newSource("r1", "test", "1G").Labels,
				Spec:      newSource("r1", "test", "1G").Spec,
			},
			in: state{
				backup: newBackup("r2", "test-backup-1g", "1G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
			},
			out: state{
				source: newSource("r2", "test", "4G"),
				backup: newBackup("r2", "test-backup-1g", "1G"),
				job: newJob("r2",
					client.ObjectKey{Namespace: "r2", Name: "test-backup-1g"},
					client.ObjectKey{Namespace: "r2", Name: "test"},
					"blub"),
			},
			targetSize: "4G",
			done:       false,
		},
		"finish restore": {
			namespace: "r3",
			syncImage: "blub",
			pvcInfo: &pvcInfo{
				Name:      "test",
				Namespace: "r3",
				Labels:    newSource("r3", "test", "1G").Labels,
				Spec:      newSource("r3", "test", "1G").Spec,
			},
			in: state{
				source: newSource("r3", "test", "4G"),
				backup: newBackup("r3", "test-backup-1g", "1G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
				job: newJob("r3",
					client.ObjectKey{Namespace: "r3", Name: "test-backup-1g"},
					client.ObjectKey{Namespace: "r3", Name: "test"},
					"blub",
					func(job *batchv1.Job) *batchv1.Job {
						job.Status.Succeeded = 1
						return job
					}),
			},
			out: state{
				source: newSource("r3", "test", "4G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
				backup: newBackup("r3", "test-backup-1g", "1G"),
			},
			targetSize: "4G",
			done:       true,
		},
		"continue restore": {
			namespace: "r4",
			syncImage: "blub",
			pvcInfo: &pvcInfo{
				Name:      "test",
				Namespace: "r4",
				Labels:    newSource("r4", "test", "1G").Labels,
				Spec:      newSource("r4", "test", "1G").Spec,
			},
			in: state{
				source: newSource("r4", "test", "4G"),
				backup: newBackup("r4", "test-backup-1g", "1G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
			},
			out: state{
				source: newSource("r4", "test", "4G"),
				backup: newBackup("r4", "test-backup-1g", "1G"),
				job: newJob("r4",
					client.ObjectKey{Namespace: "r4", Name: "test-backup-1g"},
					client.ObjectKey{Namespace: "r4", Name: "test"},
					"blub",
					func(job *batchv1.Job) *batchv1.Job {
						job.Status.Succeeded = 1
						return job
					}),
			},
			targetSize: "4G",
			done:       false,
		},
		"don't restore again": {
			namespace: "r5",
			syncImage: "blub",
			pvcInfo: &pvcInfo{
				Name:      "test",
				Namespace: "r5",
				Labels:    newSource("r5", "test", "1G").Labels,
				Spec:      newSource("r5", "test", "1G").Spec,
			},
			in: state{
				source: newSource("r5", "test", "4G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
				backup: newBackup("r5", "test-backup-1g", "1G"),
			},
			out: state{
				source: newSource("r5", "test", "4G"),
				backup: newBackup("r5", "test-backup-1g", "1G"),
			},
			targetSize: "4G",
			done:       true,
		},
	}

	for k, tc := range tcs {
		tc := tc
		ctx := context.Background()
		k := k

		It(k, func() {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: tc.namespace},
			})).To(Succeed())
			if tc.in.source != nil {
				Expect(k8sClient.Create(ctx, tc.in.source)).To(Succeed())
			}
			if tc.in.backup != nil {
				Expect(k8sClient.Create(ctx, tc.in.backup)).To(Succeed())
			}
			if tc.in.job != nil {
				stat := tc.in.job.Status
				Expect(k8sClient.Create(ctx, tc.in.job)).To(Succeed())
				tc.in.job.Status = stat // Create removes status
				Expect(k8sClient.Status().Update(ctx, tc.in.job)).To(Succeed())
			}
			r := StatefulSetReconciler{
				Client:             k8sClient,
				Scheme:             k8sClient.Scheme(),
				SyncContainerImage: tc.syncImage,
			}
			pi := pvcInfo{}
			if tc.in.source != nil {
				pi = pvcInfo{
					Name:      tc.in.source.Name,
					Namespace: tc.in.source.Namespace,
					Labels:    tc.in.source.Labels,
					Spec:      tc.in.source.Spec,
				}
			}
			if tc.pvcInfo != nil {
				pi = *tc.pvcInfo
			}
			pi.TargetSize = resource.MustParse(tc.targetSize)
			err := r.restorePVC(ctx, pi)

			if tc.fail {
				Expect(err).NotTo(Succeed())
				Expect(err).NotTo(MatchError(errInProgress))
				return
			}
			if !tc.done {
				Expect(err).To(MatchError(errInProgress))
			} else {
				Expect(err).To(Succeed())
			}

			if tc.out.source != nil {
				Eventually(func() (*corev1.PersistentVolumeClaim, error) {
					key := client.ObjectKeyFromObject(tc.out.source)
					pvc := &corev1.PersistentVolumeClaim{}
					err := k8sClient.Get(ctx, key, pvc)
					return pvc, err
				}, timeout, interval).Should(BeEquivalentPVC(tc.out.source))
			} else if tc.in.source != nil {
				Eventually(func() metav1.StatusReason {
					key := client.ObjectKeyFromObject(tc.in.source)
					pvc := &corev1.PersistentVolumeClaim{}
					if err := k8sClient.Get(ctx, key, pvc); err != nil {
						return apierrors.ReasonForError(err)
					}
					if pvc.DeletionTimestamp != nil {
						// This is needed as the testenv does not properly clean up pvcs
						return metav1.StatusReasonNotFound
					}
					return "found"

				}, timeout, interval).Should(Equal(metav1.StatusReasonNotFound))
			}
			if tc.out.backup != nil {
				Eventually(func() (*corev1.PersistentVolumeClaim, error) {
					key := client.ObjectKeyFromObject(tc.out.backup)
					pvc := &corev1.PersistentVolumeClaim{}
					err := k8sClient.Get(ctx, key, pvc)
					return pvc, err
				}, timeout, interval).Should(BeEquivalentPVC(tc.out.backup))
			} else if tc.in.backup != nil {
				Eventually(func() metav1.StatusReason {
					key := client.ObjectKeyFromObject(tc.in.backup)
					pvc := &corev1.PersistentVolumeClaim{}
					return apierrors.ReasonForError(k8sClient.Get(ctx, key, pvc))
				}, timeout, interval).Should(Equal(metav1.StatusReasonNotFound))
			}
			if tc.out.job != nil {
				Eventually(func() (*batchv1.Job, error) {
					key := client.ObjectKeyFromObject(tc.out.job)
					job := &batchv1.Job{}
					err := k8sClient.Get(ctx, key, job)
					return job, err
				}, timeout, interval).Should(BeEquivalentJob(tc.out.job))
			} else if tc.in.job != nil {
				Eventually(func() metav1.StatusReason {
					key := client.ObjectKeyFromObject(tc.in.job)
					job := &batchv1.Job{}
					if err := k8sClient.Get(ctx, key, job); err != nil {
						return apierrors.ReasonForError(err)
					}
					if job.DeletionTimestamp != nil {
						// This is needed as the testenv does not properly clean up jobs
						// Their stuck as there is a finalizer to remove pods
						return metav1.StatusReasonNotFound
					}
					return "found"
				}, timeout, interval).Should(Equal(metav1.StatusReasonNotFound))
			}
		})
	}
})
