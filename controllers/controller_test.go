package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// E2Eish test against test env
func TestController(t *testing.T) {
	// Setup separate test env and start controller
	req := require.New(t)

	timeout := time.Second * 10
	duration := time.Second * 4
	interval := time.Millisecond * 300

	testEnv = &envtest.Environment{}
	conf, err := testEnv.Start()
	req.Nil(err)
	defer testEnv.Stop()

	s := runtime.NewScheme()
	err = appsv1.AddToScheme(s)
	err = corev1.AddToScheme(s)
	err = batchv1.AddToScheme(s)
	req.Nil(err)

	mgr, err := ctrl.NewManager(conf, ctrl.Options{
		Scheme: s,
	})
	req.Nil(err)
	req.Nil((&StatefulSetReconciler{
		Client:             mgr.GetClient(),
		Scheme:             mgr.GetScheme(),
		Recorder:           mgr.GetEventRecorderFor("statefulset-resize-controller"),
		SyncContainerImage: "test",
	}).SetupWithManager(mgr))
	go func() {
		req.Nil(mgr.Start(ctrl.SetupSignalHandler()))
	}()

	c := mgr.GetClient()

	t.Run("e2e", func(t *testing.T) { // This allows the subtest to run in parallel
		t.Run("Don't scale down correct StatfulSets", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ns := "e2e1"
			require := require.New(t)
			require.Nil(c.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: ns,
				},
			}))
			sts := newTestStatefulSet(ns, "test")
			require.Nil(c.Create(ctx, newSource(ns, "data-test-0", "2G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.Nil(c.Create(ctx, newSource(ns, "data-test-1", "2G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.Nil(c.Create(ctx, newSource(ns, "data-test-2", "2G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.Nil(c.Create(ctx, sts))

			consistently(t, func() bool {
				return stsExists(ctx, c, sts)
			}, duration, interval)

		})
		t.Run("Don't scale down failed StatfulSets", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ns := "e2e2"
			require := require.New(t)
			require.Nil(c.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: ns,
				},
			}))
			sts := newTestStatefulSet(ns, "test")
			sts.Labels = map[string]string{FailedLabel: "true"}
			require.Nil(c.Create(ctx, newSource(ns, "data-test-0", "1G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.Nil(c.Create(ctx, newSource(ns, "data-test-1", "1G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.Nil(c.Create(ctx, newSource(ns, "data-test-2", "2G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.Nil(c.Create(ctx, sts))

			consistently(t, func() bool {
				return stsExists(ctx, c, sts)
			}, duration, interval)

		})

		t.Run("Scale down recreated StatfulSets", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ns := "e2e3"
			assert := assert.New(t)
			require := require.New(t)
			require.Nil(c.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: ns,
				},
			}))
			sts := newTestStatefulSet(ns, "test")
			require.Nil(c.Create(ctx, newSource(ns, "data-test-0", "1G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.Nil(c.Create(ctx, newSource(ns, "data-test-1", "1G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.Nil(c.Create(ctx, newSource(ns, "data-test-2", "1G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))

			require.Nil(c.Create(ctx, sts))
			r := int32(0)
			sts.Spec.Replicas = &r
			assert.Eventually(func() bool {
				return stsExists(ctx, c, sts)
			}, duration, interval)

		})

		t.Run("Fail and scale up StatfulSets if Backup job failed", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ns := "e2e4"
			assert := assert.New(t)
			require := require.New(t)
			require.Nil(c.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: ns,
				},
			}))
			sts := newTestStatefulSet(ns, "test")
			r := int32(0)
			sts.Spec.Replicas = &r
			sts.Annotations = map[string]string{
				ReplicasAnnotation: "3",
			}

			require.Nil(c.Create(ctx, newSource(ns, "data-test-0", "1G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.Nil(c.Create(ctx, sts))

			// First scale down
			r = 0
			sts.Spec.Replicas = &r
			assert.Eventually(func() bool {
				return stsExists(ctx, c, sts)
			}, duration, interval)
			require.Nil(c.Get(ctx, client.ObjectKeyFromObject(sts), sts))
			sts.Status.Replicas = 0
			sts.Status.CurrentReplicas = 0
			sts.Status.CurrentRevision = "revision"
			require.Nil(c.Status().Update(ctx, sts)) // manualy do what k8s would do

			// Check if backup is created
			bu := newBackup(ns, "data-test-0-backup-1g", "1G")
			assert.Eventually(func() bool {
				return pvcExists(ctx, c, bu)
			}, duration, interval)

			job := newTestJob(ns,
				client.ObjectKey{Namespace: ns, Name: "data-test-0"},
				client.ObjectKey{Namespace: ns, Name: bu.Name},
				"test", &jobFailed)
			jobStatus := job.Status
			assert.Eventually(func() bool {
				return jobExists(ctx, c, job)
			}, duration, interval)
			require.Nil(c.Get(ctx, client.ObjectKeyFromObject(job), job))
			job.Status = jobStatus
			require.Nil(c.Status().Update(ctx, job)) // manualy fail job

			sts.Labels = map[string]string{}
			sts.Labels[FailedLabel] = "true"
			r = 3
			sts.Spec.Replicas = &r
			assert.Eventually(func() bool {
				return stsExists(ctx, c, sts)
			}, timeout, interval)
		})
	})
}
