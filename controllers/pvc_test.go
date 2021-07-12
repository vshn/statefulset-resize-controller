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

func newSource(namespace, name, size string) *corev1.PersistentVolumeClaim {
	volumeMode := corev1.PersistentVolumeFilesystem
	return &corev1.PersistentVolumeClaim{
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
}
func newBackup(namespace, name, size string) *corev1.PersistentVolumeClaim {
	volumeMode := corev1.PersistentVolumeFilesystem
	return &corev1.PersistentVolumeClaim{
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
}

func newJob(namespace, name string) *batchv1.Job {
	return nil
}
func BeEquivalentPVC(other *corev1.PersistentVolumeClaim) types.GomegaMatcher {
	return SatisfyAll(
		WithTransform(func(pvc *corev1.PersistentVolumeClaim) corev1.PersistentVolumeClaimSpec { return pvc.Spec }, BeEquivalentTo(other.Spec)),
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
		err        error
	}

	tcs := map[string]tCase{
		"initiate backup": {
			namespace: "t1",
			in: state{
				source: newSource("t1", "test", "1G"),
			},
			out: state{
				source: newSource("t1", "test", "1G"),
				backup: newBackup("t1", "test-backup-1g", "1G"),
				job:    newJob("t1", "test-test-backup-1G"),
			},
			targetSize: "4G",
			err:        errInProgress,
		},
	}

	for k, tc := range tcs {
		tc := tc
		ctx := context.Background()

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
				Expect(k8sClient.Create(ctx, tc.in.job)).To(Succeed())
			}
			r := StatefulSetReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			Expect(tc.in.source).NotTo(BeNil())
			Expect(r.backupPVC(ctx, pvcInfo{
				Name:       tc.in.source.Name,
				Namespace:  tc.in.source.Namespace,
				Labels:     tc.in.source.Labels,
				Spec:       tc.in.source.Spec,
				TargetSize: resource.MustParse(tc.targetSize),
			})).To(MatchError(tc.err))

			Eventually(func() (*corev1.PersistentVolumeClaim, error) {
				var key client.ObjectKey
				if tc.out.source != nil {
					key = client.ObjectKeyFromObject(tc.out.source)
				} else {
					if tc.in.source == nil {
						return nil, nil
					}
					// We expect this to be deleted
					key = client.ObjectKeyFromObject(tc.in.source)
				}
				pvc := &corev1.PersistentVolumeClaim{}
				err := k8sClient.Get(ctx, key, pvc)
				return pvc, err
			}, timeout, interval).Should(BeEquivalentPVC(tc.out.source))
			Eventually(func() (*corev1.PersistentVolumeClaim, error) {
				var key client.ObjectKey
				if tc.out.backup != nil {
					key = client.ObjectKeyFromObject(tc.out.backup)
				} else {
					if tc.in.backup == nil {
						return nil, nil
					}
					// We expect this to be deleted
					key = client.ObjectKeyFromObject(tc.in.backup)
				}
				pvc := &corev1.PersistentVolumeClaim{}
				err := k8sClient.Get(ctx, key, pvc)
				return pvc, err
			}, timeout, interval).Should(BeEquivalentPVC(tc.out.backup))
			Eventually(func() (*batchv1.Job, error) {
				var key client.ObjectKey
				if tc.out.job != nil {
					key = client.ObjectKeyFromObject(tc.out.job)
				} else {
					if tc.in.job == nil {
						return nil, nil
					}
					// We expect this to be deleted
					key = client.ObjectKeyFromObject(tc.in.job)
				}
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, key, job)
				return job, err
			}, timeout, interval).Should(BeEquivalentTo(tc.out.job))

		})

	}

})
