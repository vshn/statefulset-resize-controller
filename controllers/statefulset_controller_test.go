package controllers

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("StatefulSet controller", func() {

	// Define utility constants for object names and testing timeouts/durations and intervals.
	const (
		name      = "test"
		namespace = "default"

		timeout  = time.Second * 10
		duration = time.Second * 10
		interval = time.Millisecond * 250
	)

	Context("When creating a StatefulSet", func() {
		It("Should check if it needs to resize", func() {
			By("By creating a new Sts")
			ctx := context.Background()
			replicas := int32(1)
			l := map[string]string{
				"app": name,
			}
			sts := &appsv1.StatefulSet{
				TypeMeta:   metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
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
										corev1.ResourceStorage: resource.MustParse("1G"),
									},
								},
								AccessModes: []corev1.PersistentVolumeAccessMode{
									corev1.ReadWriteOnce,
								},
							},
						},
					},
					ServiceName:    name,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{},
				},
				Status: appsv1.StatefulSetStatus{},
			}
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("data-%s-0", name),
					Namespace: namespace,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.ResourceRequirements{
						Requests: map[corev1.ResourceName]resource.Quantity{
							corev1.ResourceStorage: resource.MustParse("1G"),
						},
					},
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
				},
			}
			Expect(k8sClient.Create(ctx, sts)).Should(Succeed())
			Expect(k8sClient.Create(ctx, pvc)).Should(Succeed())
		})
	})
})

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
		out  map[string]struct{}
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
			out: map[string]struct{}{},
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
			out: map[string]struct{}{
				"foo:data-test-0": {},
				"foo:data-test-1": {},
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
			out: map[string]struct{}{},
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
			out: map[string]struct{}{
				"foo:data-test-0": {},
				"foo:data-test-1": {},
				"foo:log-test-0":  {},
				"foo:log-test-1":  {},
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
			out: map[string]struct{}{
				"foo:data-test-0": {},
				"foo:data-test-1": {},
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
			out: map[string]struct{}{
				"foo:data-test-0": {},
				"foo:data-test-1": {},
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
				_, ok := tc.out[fmt.Sprintf("%s:%s", r.Namespace, r.Name)]
				Expect(ok).To(BeTrue())
			}
		})
	}
})

var _ = Describe("scaledown", func() {
	type state struct {
		replicas          int32
		annotationState   string
		annotationReplica string
		statusReplicas    int32
	}
	type tCase struct {
		in   state
		out  state
		done bool
	}
	tcs := map[string]tCase{
		"should scale down and wait": {
			in: state{
				replicas:       6,
				statusReplicas: 5,
			},
			out: state{
				replicas:          0,
				statusReplicas:    5,
				annotationState:   stateScaledown,
				annotationReplica: "6",
			},
			done: false,
		},
		"should scale down and wait, even if zero replicas running": {
			in: state{
				replicas:       2,
				statusReplicas: 0,
			},
			out: state{
				replicas:          0,
				statusReplicas:    0,
				annotationState:   stateScaledown,
				annotationReplica: "2",
			},
			done: false,
		},
		"should keep waiting": {
			in: state{
				replicas:          0,
				statusReplicas:    2,
				annotationState:   stateScaledown,
				annotationReplica: "4",
			},
			out: state{
				replicas:          0,
				statusReplicas:    2,
				annotationState:   stateScaledown,
				annotationReplica: "4",
			},
			done: false,
		},
		"should should proceed": {
			in: state{
				replicas:          0,
				statusReplicas:    0,
				annotationState:   stateScaledown,
				annotationReplica: "4",
			},
			out: state{
				replicas:          0,
				statusReplicas:    0,
				annotationState:   stateBackup,
				annotationReplica: "4",
			},
			done: true,
		},
	}
	for k, tc := range tcs {
		tc := tc // necessary because Ginkgo weirdness
		It(k, func() {
			sts := appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						stateAnnotation:    tc.in.annotationState,
						replicasAnnotation: tc.in.annotationReplica,
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &tc.in.replicas,
				},
				Status: appsv1.StatefulSetStatus{
					Replicas: tc.in.statusReplicas,
				},
			}

			sts, err := scaleDown(sts)
			if tc.done {
				Expect(err).Should(Succeed())
			} else {
				Expect(err).To(MatchError(errInProgress))
			}
			Expect(*sts.Spec.Replicas).To(Equal(tc.out.replicas), "replicas")
			Expect(sts.Status.Replicas).To(Equal(tc.out.statusReplicas), "status replicas")
			Expect(sts.Annotations[stateAnnotation]).To(Equal(tc.out.annotationState), "state annotation")
			Expect(sts.Annotations[replicasAnnotation]).To(Equal(tc.out.annotationReplica), "replicas annotation")
		})
	}
})
