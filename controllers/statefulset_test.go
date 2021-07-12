package controllers

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("scaledown", func() {
	type state struct {
		replicas          int32
		annotationState   string
		annotationScaleUp string
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
				annotationReplica: "2",
			},
			done: false,
		},
		"should keep waiting": {
			in: state{
				replicas:          0,
				statusReplicas:    2,
				annotationReplica: "4",
			},
			out: state{
				replicas:          0,
				statusReplicas:    2,
				annotationReplica: "4",
			},
			done: false,
		},
		"should should proceed": {
			in: state{
				replicas:          0,
				statusReplicas:    0,
				annotationReplica: "4",
			},
			out: state{
				replicas:          0,
				statusReplicas:    0,
				annotationReplica: "4",
			},
			done: true,
		},
		"should not scale down if scalUp job is running": {
			in: state{
				replicas:          2,
				statusReplicas:    1,
				annotationScaleUp: "true",
				annotationReplica: "2",
			},
			out: state{
				replicas:          2,
				statusReplicas:    1,
				annotationReplica: "2",
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
						replicasAnnotation: tc.in.annotationReplica,
						scalupAnnotation:   tc.in.annotationScaleUp,
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
			Expect(sts.Annotations[replicasAnnotation]).To(Equal(tc.out.annotationReplica), "replicas annotation")
		})
	}
})

var _ = Describe("scaleup", func() {
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
		fail bool
	}
	tcs := map[string]tCase{
		"should scale up and wait": {
			in: state{
				replicas:          0,
				statusReplicas:    0,
				annotationReplica: "5",
			},
			out: state{
				replicas:          5,
				statusReplicas:    0,
				annotationReplica: "5",
			},
			done: false,
		},
		"should keep waiting": {
			in: state{
				replicas:          4,
				statusReplicas:    2,
				annotationReplica: "4",
			},
			out: state{
				replicas:          4,
				statusReplicas:    2,
				annotationReplica: "4",
			},
			done: false,
		},
		"should should proceed": {
			in: state{
				replicas:          4,
				statusReplicas:    4,
				annotationReplica: "4",
			},
			out: state{
				replicas:          4,
				statusReplicas:    4,
				annotationReplica: "",
			},
			done: true,
		},
		"should should fail to parse": {
			in: state{
				replicas:          4,
				statusReplicas:    4,
				annotationReplica: "NaN",
			},
			out:  state{},
			fail: true,
		},
	}
	for k, tc := range tcs {
		tc := tc // necessary because Ginkgo weirdness
		It(k, func() {
			sts := appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
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

			sts, err := scaleUp(sts)
			if tc.fail {
				Expect(err).ShouldNot(Succeed())
			} else {
				if tc.done {
					Expect(err).Should(Succeed())
					Expect(sts.Annotations[scalupAnnotation]).To(Equal(""))
				} else {
					Expect(err).To(MatchError(errInProgress))
					Expect(sts.Annotations[scalupAnnotation]).To(Equal("true"))
				}
				Expect(*sts.Spec.Replicas).To(Equal(tc.out.replicas), "replicas")
				Expect(sts.Status.Replicas).To(Equal(tc.out.statusReplicas), "status replicas")
				Expect(sts.Annotations[replicasAnnotation]).To(Equal(tc.out.annotationReplica), "replicas annotation")
			}
		})
	}
})
