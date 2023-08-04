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
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/pointer"
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

		sc := &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "mysc",
				Annotations: map[string]string{
					"storageclass.kubernetes.io/is-default-class": "true",
				},
			},
			Provisioner:          "mysc",
			AllowVolumeExpansion: pointer.Bool(true),
		}

		require.NoError(t, c.Create(ctx, sc))

		t.Run("Don't touch correct PVCs", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			ns := "e2e1"
			require := require.New(t)
			require.NoError(c.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: ns,
				},
			}))
			pvcSize := "2G"
			sts := newTestStatefulSet(ns, "test", 1, pvcSize)
			sts.Labels = map[string]string{
				testLabelName: "true",
			}

			pvc := applyResizablePVC(ctx, "data-test-0", ns, pvcSize, sts, c, require)

			require.NoError(c.Create(ctx, sts))

			consistently(t, func() bool {
				return pvcEqualSize(ctx, c, pvc, pvcSize)
			}, duration, interval, "PVCs equal size")

		})
		t.Run("Ignore STS without the label", func(t *testing.T) {
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

			pvc := applyResizablePVC(ctx, "data-test-0", ns, "1G", sts, c, require)

			require.NoError(c.Create(ctx, sts))

			consistently(t, func() bool {
				return pvcEqualSize(ctx, c, pvc, "1G")
			}, duration, interval, "PVCs equal size")
		})
		t.Run("Change PVCs if they not match", func(t *testing.T) {
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
			sts.Labels = map[string]string{
				testLabelName: "true",
			}

			pvc := applyResizablePVC(ctx, "data-test-0", ns, "1G", sts, c, require)

			require.NoError(c.Create(ctx, sts))

			consistently(t, func() bool {
				return pvcEqualSize(ctx, c, pvc, "2G")
			}, duration, interval, "PVCs equal size")
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
	req.NoError(storagev1.AddToScheme(s))

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

func applyResizablePVC(ctx context.Context, name, ns, size string, sts *appsv1.StatefulSet, c client.Client, require *require.Assertions) *corev1.PersistentVolumeClaim {
	pvc := newSource(ns, name, size,
		func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
			pvc.Labels = sts.Spec.Selector.MatchLabels
			return pvc
		})

	pvc.Spec.StorageClassName = pointer.String("mysc")
	require.NoError(c.Create(ctx, pvc))

	// we need to set the PVC to bound in order for the resize to work
	pvc.Status.Phase = corev1.ClaimBound
	require.NoError(c.Status().Update(ctx, pvc))
	return pvc
}
