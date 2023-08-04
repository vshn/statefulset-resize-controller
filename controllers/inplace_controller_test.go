//go:build integration
// +build integration

package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const (
	testLabelName = "mylabel"
)

func TestInplaceController(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, stop := startInplaceTestReconciler(t, ctx, "")
	defer stop()

	t.Run("InplaceE2E", func(t *testing.T) {

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
			// sts.Labels[testLabelName] = "true"
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
	})
}

// startInplaceTestReconciler sets up a separate test env and starts the controller
func startInplaceTestReconciler(t *testing.T, ctx context.Context, crname string) (client.Client, func() error) {
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
	req.NoError((&InplaceReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("statefulset-resize-controller"),
		RequeueAfter: time.Second,
		LabelName:    testLabelName,
	}).SetupWithManager(mgr))
	go func() {
		req.NoError(mgr.Start(ctx))
	}()

	return mgr.GetClient(), testEnv.Stop
}
