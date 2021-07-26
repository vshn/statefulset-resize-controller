//+build integration

package controllers

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vshn/statefulset-resize-controller/statefulset"
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

	testEnv := &envtest.Environment{}
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
		RequeueAfter:       time.Second,
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
			sts := newTestStatefulSet(ns, "test", 1, "2G")
			require.Nil(c.Create(ctx, newSource(ns, "data-test-0", "2G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.Nil(c.Create(ctx, sts))

			consistently(t, func() bool {
				return stsExists(ctx, c, sts)
			}, duration, interval, "Sts exists")

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
			sts := newTestStatefulSet(ns, "test", 1, "2G")
			sts.Labels = map[string]string{statefulset.FailedLabel: "true"}
			require.Nil(c.Create(ctx, newSource(ns, "data-test-0", "1G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.Nil(c.Create(ctx, sts))

			consistently(t, func() bool {
				return stsExists(ctx, c, sts)
			}, duration, interval)

		})

		t.Run("Resize StatfulSet", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ns := "e2e3"
			require := require.New(t)
			require.Nil(c.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: ns,
				},
			}))
			sts := newTestStatefulSet(ns, "test", 1, "2G")
			pvc := newSource(ns, "data-test-0", "1G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})
			require.Nil(c.Create(ctx, pvc))
			require.Nil(c.Create(ctx, sts))

			t.Run("Scale down", func(t *testing.T) {
				eventuallyScaledDown(t, ctx, c, sts)
			})
			t.Run("Back up", func(t *testing.T) {
				eventuallyBackedUp(t, ctx, c, pvc, true)
			})
			t.Run("Restored", func(t *testing.T) {
				eventuallyRestored(t, ctx, c, pvc, "2G")
			})
			t.Run("Scale up", func(t *testing.T) {
				eventuallyScaledUp(t, ctx, c, sts, 1)
			})
		})

		t.Run("Fail and scale up StatfulSets if Backup job failed", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ns := "e2e4"
			require.Nil(t, c.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: ns,
				},
			}))
			sts := newTestStatefulSet(ns, "test", 1, "2G")
			pvc := newSource(ns, "data-test-0", "1G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})
			require.Nil(t, c.Create(ctx, pvc))
			require.Nil(t, c.Create(ctx, sts))

			t.Run("Scale down", func(t *testing.T) {
				eventuallyScaledDown(t, ctx, c, sts)
			})
			t.Run("Back up failed", func(t *testing.T) {
				eventuallyBackedUp(t, ctx, c, pvc, false)
			})
			t.Run("Scale up", func(t *testing.T) {
				eventuallyScaledUp(t, ctx, c, sts, 1)
			})
			t.Run("Mark as failed", func(t *testing.T) {
				require.Nil(t, c.Get(ctx, client.ObjectKeyFromObject(sts), sts))
				require.Equal(t, sts.Labels[statefulset.FailedLabel], "true")
			})
		})
	})
}

func eventuallyScaledDown(t *testing.T, ctx context.Context, c client.Client, sts *appsv1.StatefulSet) bool {
	ok := assert.Eventually(t, func() bool {
		found := &appsv1.StatefulSet{}
		key := client.ObjectKeyFromObject(sts)
		if err := c.Get(ctx, key, found); err != nil {
			return false
		}
		return *found.Spec.Replicas == 0
	}, duration, interval)
	require.Nil(t, c.Get(ctx, client.ObjectKeyFromObject(sts), sts))
	sts.Status.Replicas = 0
	sts.Status.CurrentReplicas = 0
	sts.Status.CurrentRevision = "revision"
	require.Nil(t, c.Status().Update(ctx, sts)) // manualy do what k8s would do
	return ok
}
func eventuallyScaledUp(t *testing.T, ctx context.Context, c client.Client, sts *appsv1.StatefulSet, replicas int32) bool {
	ok := assert.Eventually(t, func() bool {
		found := &appsv1.StatefulSet{}
		key := client.ObjectKeyFromObject(sts)
		if err := c.Get(ctx, key, found); err != nil {
			return false
		}
		return *found.Spec.Replicas == replicas
	}, duration, interval)
	require.Nil(t, c.Get(ctx, client.ObjectKeyFromObject(sts), sts))
	sts.Status.Replicas = replicas
	sts.Status.CurrentReplicas = replicas
	sts.Status.CurrentRevision = "revision"
	require.Nil(t, c.Status().Update(ctx, sts)) // manualy do what k8s would do
	return ok
}
func eventuallyBackedUp(t *testing.T, ctx context.Context, c client.Client, pvc *corev1.PersistentVolumeClaim, successful bool) bool {
	// Check if backup is created
	bname := strings.ToLower(fmt.Sprintf("%s-backup-1g", pvc.Name))
	bu := newBackup(pvc.Namespace, bname, "1G")
	require.Eventually(t, func() bool {
		return pvcExists(ctx, c, bu)
	}, duration, interval)

	jobState := jobSucceeded
	if !successful {
		jobState = jobFailed
	}
	job := newTestJob(pvc.Namespace,
		client.ObjectKey{Namespace: pvc.Namespace, Name: pvc.Name},
		client.ObjectKey{Namespace: bu.Namespace, Name: bu.Name},
		"test", &jobState)
	jobStatus := job.Status
	ok := assert.Eventually(t, func() bool {
		return jobExists(ctx, c, job)
	}, duration, interval)
	require.Nil(t, c.Get(ctx, client.ObjectKeyFromObject(job), job))
	job.Status = jobStatus
	require.Nil(t, c.Status().Update(ctx, job)) // manualy succeed or fail job
	return ok
}

func eventuallyRestored(t *testing.T, ctx context.Context, c client.Client, pvc *corev1.PersistentVolumeClaim, size string) bool {
	require.Eventually(t, func() bool {
		return pvcNotExists(ctx, c, pvc)
	}, duration, interval, "pvc removed")
	require.Nil(t, c.Get(ctx, client.ObjectKeyFromObject(pvc), pvc))
	pvc.Finalizers = nil
	require.Nil(t, c.Update(ctx, pvc))

	pvc = newSource(pvc.Namespace, "data-test-0", size,
		func(p *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
			p.Labels = pvc.Labels
			return p
		})

	require.Eventually(t, func() bool {
		return pvcExists(ctx, c, pvc)
	}, duration, interval)

	bname := strings.ToLower(fmt.Sprintf("%s-backup-1g", pvc.Name))
	job := newTestJob(pvc.Namespace,
		client.ObjectKey{Namespace: pvc.Namespace, Name: bname},
		client.ObjectKey{Namespace: pvc.Namespace, Name: pvc.Name},
		"test", &jobSucceeded)
	jobStatus := job.Status
	ok := assert.Eventually(t, func() bool {
		return jobExists(ctx, c, job)
	}, duration, interval)
	require.Nil(t, c.Get(ctx, client.ObjectKeyFromObject(job), job))
	job.Status = jobStatus
	require.Nil(t, c.Status().Update(ctx, job)) // manualy succeed job
	return ok
}
