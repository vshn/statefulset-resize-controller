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
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/vshn/statefulset-resize-controller/statefulset"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// E2Eish test against test env
func TestController(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, stop := startTestReconciler(t, ctx, "")
	defer stop()

	t.Run("e2e", func(t *testing.T) { // This allows the subtest to run in parallel
		t.Run("Don't scale down correct StatfulSets", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ns := "e2e1"
			require := require.New(t)
			require.NoError(c.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: ns,
				},
			}))
			sts := newTestStatefulSet(ns, "test", 1, "2G")
			require.NoError(c.Create(ctx, newSource(ns, "data-test-0", "2G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.NoError(c.Create(ctx, sts))

			consistently(t, func() bool {
				return stsExists(ctx, c, sts)
			}, duration, interval, "Sts exists")

		})
		t.Run("Don't scale down failed StatfulSets", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ns := "e2e2"
			require := require.New(t)
			require.NoError(c.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: ns,
				},
			}))
			sts := newTestStatefulSet(ns, "test", 1, "2G")
			sts.Labels = map[string]string{statefulset.FailedLabel: "true"}
			require.NoError(c.Create(ctx, newSource(ns, "data-test-0", "1G",
				func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					pvc.Labels = sts.Spec.Selector.MatchLabels
					return pvc
				})))
			require.NoError(c.Create(ctx, sts))

			consistently(t, func() bool {
				return stsExists(ctx, c, sts)
			}, duration, interval)

		})

		t.Run("Resize StatfulSet", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ns := "e2e3"
			require := require.New(t)
			require.NoError(c.Create(ctx, &corev1.Namespace{
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
			require.NoError(c.Create(ctx, pvc))
			require.NoError(c.Create(ctx, sts))

			t.Run("Scale down", func(t *testing.T) {
				eventuallyScaledDown(t, ctx, c, sts)
			})

			consistently(t, func() bool {
				return rbacNotExists(t, ctx, c, ns, "somesa", "someclusterrole")
			}, duration, interval, "RBAC doesn't exist")

			t.Run("Back up", func(t *testing.T) {
				eventuallyBackedUp(t, ctx, c, pvc, true, "")
			})
			t.Run("Restored", func(t *testing.T) {
				eventuallyRestored(t, ctx, c, pvc, "2G", "")
			})
			t.Run("Scale up", func(t *testing.T) {
				eventuallyScaledUp(t, ctx, c, sts, 1)
			})
		})

		t.Run("Fail and scale up StatfulSets if Backup job failed", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ns := "e2e4"
			require.NoError(t, c.Create(ctx, &corev1.Namespace{
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
			require.NoError(t, c.Create(ctx, pvc))
			require.NoError(t, c.Create(ctx, sts))

			t.Run("Scale down", func(t *testing.T) {
				eventuallyScaledDown(t, ctx, c, sts)
			})
			t.Run("Back up failed", func(t *testing.T) {
				eventuallyBackedUp(t, ctx, c, pvc, false, "")
			})
			t.Run("Scale up", func(t *testing.T) {
				eventuallyScaledUp(t, ctx, c, sts, 1)
			})
			t.Run("Mark as failed", func(t *testing.T) {
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sts), sts))
				require.Equal(t, sts.Labels[statefulset.FailedLabel], "true")
			})
		})
	})
}

func TestControllerWithClusterRole(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	crname := "myclusterrole"
	c, stop := startTestReconciler(t, ctx, crname)
	defer stop()

	t.Run("e2e", func(t *testing.T) { // This allows the subtest to run in parallel
		t.Run("Resize StatfulSet", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ns := "e2e"
			require := require.New(t)
			require.NoError(c.Create(ctx, &corev1.Namespace{
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
			require.NoError(c.Create(ctx, pvc))
			require.NoError(c.Create(ctx, sts))

			rbacobjname := RbacObjNamePrefix + "test"

			t.Run("Scale down", func(t *testing.T) {
				eventuallyScaledDown(t, ctx, c, sts)
			})
			t.Run("RBAC created", func(t *testing.T) {
				eventuallyRbacCreated(t, ctx, c, ns, rbacobjname, crname)
			})
			t.Run("Back up", func(t *testing.T) {
				eventuallyBackedUp(t, ctx, c, pvc, true, rbacobjname)
			})
			t.Run("Restored", func(t *testing.T) {
				eventuallyRestored(t, ctx, c, pvc, "2G", rbacobjname)
			})
			t.Run("RBAC removed", func(t *testing.T) {
				eventuallyRbacRemoved(t, ctx, c, ns, rbacobjname, crname)
			})
			t.Run("Scale up", func(t *testing.T) {
				eventuallyScaledUp(t, ctx, c, sts, 1)
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
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sts), sts))
	sts.Status.Replicas = 0
	sts.Status.CurrentReplicas = 0
	sts.Status.CurrentRevision = "revision"
	require.NoError(t, c.Status().Update(ctx, sts)) // manualy do what k8s would do
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
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sts), sts))
	sts.Status.Replicas = replicas
	sts.Status.CurrentReplicas = replicas
	sts.Status.CurrentRevision = "revision"
	require.NoError(t, c.Status().Update(ctx, sts)) // manualy do what k8s would do
	return ok
}
func eventuallyBackedUp(t *testing.T, ctx context.Context, c client.Client, pvc *corev1.PersistentVolumeClaim, successful bool, saname string) bool {
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
		"test", saname, &jobState)
	jobStatus := job.Status
	ok := assert.Eventually(t, func() bool {
		return jobExists(ctx, c, job)
	}, duration, interval)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(job), job))
	job.Status = jobStatus
	require.NoError(t, c.Status().Update(ctx, job)) // manualy succeed or fail job
	return ok
}

func eventuallyRestored(t *testing.T, ctx context.Context, c client.Client, pvc *corev1.PersistentVolumeClaim, size string, saname string) bool {
	require.Eventually(t, func() bool {
		return pvcNotExists(ctx, c, pvc)
	}, duration, interval, "pvc removed")
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pvc), pvc))
	pvc.Finalizers = nil
	require.NoError(t, c.Update(ctx, pvc))

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
		"test", saname, &jobSucceeded)
	jobStatus := job.Status
	ok := assert.Eventually(t, func() bool {
		return jobExists(ctx, c, job)
	}, duration, interval)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(job), job))
	job.Status = jobStatus
	require.NoError(t, c.Status().Update(ctx, job)) // manualy succeed job
	return ok
}

func eventuallyRbacCreated(t *testing.T, ctx context.Context, c client.Client, namespace, objname, crname string) bool {
	sa := newTestSA(namespace, objname)
	require.Eventually(t, func() bool {
		return saExists(ctx, c, sa)
	}, duration, interval, "sa created")
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sa), sa))
	rb := newTestRB(namespace, objname, crname)
	require.Eventually(t, func() bool {
		return rbExists(ctx, c, rb)
	}, duration, interval, "rb created")
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(rb), rb))
	return true
}

func eventuallyRbacRemoved(t *testing.T, ctx context.Context, c client.Client, namespace, objname, crname string) bool {
	sa := newTestSA(namespace, objname)
	require.Eventually(t, func() bool {
		return saNotExists(ctx, c, sa)
	}, duration, interval, "sa created")
	rb := newTestRB(namespace, objname, crname)
	require.Eventually(t, func() bool {
		return rbNotExists(ctx, c, rb)
	}, duration, interval, "rb created")
	return true
}

func rbacNotExists(t *testing.T, ctx context.Context, c client.Client, namespace, objname, crname string) bool {
	sa := newTestSA(namespace, objname)
	rb := newTestRB(namespace, crname, objname)
	return saNotExists(ctx, c, sa) && rbNotExists(ctx, c, rb)
}

// startTestReconciler sets up a separate test env and starts the controller
func startTestReconciler(t *testing.T, ctx context.Context, crname string) (client.Client, func() error) {
	req := require.New(t)

	testEnv := &envtest.Environment{}
	conf, err := testEnv.Start()
	req.NoError(err)

	s := runtime.NewScheme()
	req.NoError(appsv1.AddToScheme(s))
	req.NoError(corev1.AddToScheme(s))
	req.NoError(batchv1.AddToScheme(s))
	req.NoError(rbacv1.AddToScheme(s))

	mgr, err := ctrl.NewManager(conf, ctrl.Options{
		Scheme: s,
	})
	req.NoError(err)
	req.NoError((&StatefulSetReconciler{
		Client:             mgr.GetClient(),
		Scheme:             mgr.GetScheme(),
		Recorder:           mgr.GetEventRecorderFor("statefulset-resize-controller"),
		SyncContainerImage: "test",
		SyncClusterRole:    crname,
		RequeueAfter:       time.Second,
	}).SetupWithManager(mgr))
	go func() {
		req.NoError(mgr.Start(ctx))
	}()

	return mgr.GetClient(), testEnv.Stop
}
