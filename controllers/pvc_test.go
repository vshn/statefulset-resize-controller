package controllers

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
