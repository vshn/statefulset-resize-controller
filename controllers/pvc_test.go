package controllers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestFilterResizablePVCs(t *testing.T) {
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
	newPVCIn := func(name, namespace, size string) pvcIn {
		return pvcIn{
			name:      name,
			namespace: namespace,
			size:      size,
		}
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
				newPVCIn("data-test-0", "foo", "1G"),
				newPVCIn("data-test-1", "foo", "1G"),
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
				newPVCIn("data-test-0", "foo", "1G"),
				newPVCIn("data-test-1", "foo", "1G"),
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
				newPVCIn("data-test-0", "bar", "1G"),
				newPVCIn("data-test-1", "bar", "1G"),
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
				newPVCIn("data-test-0", "foo", "1G"),
				newPVCIn("data-test-1", "foo", "1G"),
				newPVCIn("log-test-0", "foo", "15G"),
				newPVCIn("log-test-1", "foo", "15G"),
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
				newPVCIn("data-test-0", "foo", "1G"),
				newPVCIn("data-test-1", "foo", "1G"),
				newPVCIn("data-prod-1", "foo", "1G"),
				newPVCIn("log-test-other-0", "foo", "10G"), // template "log-test" from StS "other"
				newPVCIn("log-test-other-1", "foo", "10G"), // template "log-test" from StS "other"
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
				newPVCIn("data-test-0", "foo", "1G"),
				newPVCIn("data-test-1", "foo", "1G"),
				newPVCIn("log-test-pvc", "foo", "10G"), // unrelated pvc someone created
			},
			out: map[string]string{
				"foo:data-test-0": "10G",
				"foo:data-test-1": "10G",
			},
		},
	}

	for k, tc := range tcs {
		tc := tc
		t.Run(k, func(t *testing.T) {
			assert := assert.New(t)

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
			assert.Len(rps, len(tc.out))
			for _, r := range rps {
				o, ok := tc.out[fmt.Sprintf("%s:%s", r.Namespace, r.SourceName)]
				assert.True(ok)
				assert.Equal(r.TargetSize.String(), o)
			}
		})
	}
}
