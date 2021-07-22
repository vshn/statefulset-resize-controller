package statefulset

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestScaledown(t *testing.T) {
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
		"should proceed": {
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
		t.Run(k, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)
			sts := appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						ReplicasAnnotation: tc.in.annotationReplica,
						ScalupAnnotation:   tc.in.annotationScaleUp,
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &tc.in.replicas,
				},
				Status: appsv1.StatefulSetStatus{
					Replicas:        tc.in.statusReplicas,
					CurrentRevision: "revision",
				},
			}

			si := Info{
				sts: &sts,
			}

			done := si.ScaleDown()

			assert.Equal(done, tc.done)
			assert.Equal(*sts.Spec.Replicas, tc.out.replicas, "replicas")
			assert.Equal(sts.Status.Replicas, tc.out.statusReplicas, "replicas")
			require.NotNil(sts.Annotations, "replicas annotation")
			assert.Equal(sts.Annotations[ReplicasAnnotation], tc.out.annotationReplica, "replicas annotation")
		})
	}
}

func TestScaleUp(t *testing.T) {
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
		"should proceed": {
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
		t.Run(k, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			sts := appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: k,
					Annotations: map[string]string{
						ReplicasAnnotation: tc.in.annotationReplica,
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &tc.in.replicas,
				},
				Status: appsv1.StatefulSetStatus{
					Replicas:        tc.in.statusReplicas,
					CurrentRevision: "revision",
				},
			}
			si := Info{
				sts: &sts,
			}

			done, err := si.ScaleUp()
			if tc.fail {
				assert.Error(err)
				return
			}

			assert.Equal(tc.done, done, "is done")
			assert.NotEqual(tc.done, si.isScalingUp(), "is still scaling up")
			assert.Equal(*sts.Spec.Replicas, tc.out.replicas, "replicas")
			assert.Equal(sts.Status.Replicas, tc.out.statusReplicas, "replicas")
			require.NotNil(sts.Annotations, "replicas annotation")
			assert.Equal(sts.Annotations[ReplicasAnnotation], tc.out.annotationReplica, "replicas annotation")
		})
	}
}
