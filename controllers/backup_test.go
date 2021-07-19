package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestBackupPVC(t *testing.T) {
	timeout := time.Second * 10
	interval := time.Millisecond * 250

	type state struct {
		source *corev1.PersistentVolumeClaim
		backup *corev1.PersistentVolumeClaim
		job    *batchv1.Job
	}
	type tCase struct {
		namespace  string
		pvcInfo    *pvcInfo
		in         state
		out        state
		targetSize string
		syncImage  string
		done       bool
		err        error
	}

	tcs := map[string]tCase{
		"initiate backup": {
			namespace: "t1",
			syncImage: "blub",
			in: state{
				source: newSource("t1", "test", "1G"),
			},
			out: state{
				source: newSource("t1", "test", "1G"),
				backup: newBackup("t1", "test-backup-1g", "1G"),
				job: newTestJob("t1",
					client.ObjectKey{Namespace: "t1", Name: "test"},
					client.ObjectKey{Namespace: "t1", Name: "test-backup-1g"},
					"blub", nil),
			},
			targetSize: "4G",
			done:       false,
		},
		"wait to complete": {
			namespace: "t2",
			syncImage: "blub",
			in: state{
				source: newSource("t2", "test", "1G"),
				backup: newBackup("t2", "test-backup-1g", "1G"),
				job: newTestJob("t2",
					client.ObjectKey{Namespace: "t1", Name: "test"},
					client.ObjectKey{Namespace: "t1", Name: "test-backup-1g"},
					"blub", nil),
			},
			out: state{
				source: newSource("t2", "test", "1G"),
				backup: newBackup("t2", "test-backup-1g", "1G"),
				job: newTestJob("t2",
					client.ObjectKey{Namespace: "t1", Name: "test"},
					client.ObjectKey{Namespace: "t1", Name: "test-backup-1g"},
					"blub", nil),
			},
			targetSize: "4G",
			done:       false,
		},
		"complete transfer and remove job": {
			namespace: "t3",
			syncImage: "blub",
			in: state{
				source: newSource("t3", "test", "1G"),
				backup: newBackup("t3", "test-backup-1g", "1G"),
				job: newTestJob("t3",
					client.ObjectKey{Namespace: "t3", Name: "test"},
					client.ObjectKey{Namespace: "t3", Name: "test-backup-1g"},
					"blub", &jobSucceeded),
			},
			out: state{
				source: newSource("t3", "test", "1G"),
				backup: newBackup("t3", "test-backup-1g", "1G"),
				job:    nil,
			},
			targetSize: "4G",
			done:       true,
		},
		"restart sync if it didn't start properly": {
			namespace: "t4",
			syncImage: "blub",
			in: state{
				source: newSource("t4", "test", "1G"),
				backup: newBackup("t4", "test-backup-1g", "1G"),
			},
			out: state{
				source: newSource("t4", "test", "1G"),
				backup: newBackup("t4", "test-backup-1g", "1G"),
				job: newTestJob("t4",
					client.ObjectKey{Namespace: "t4", Name: "test"},
					client.ObjectKey{Namespace: "t4", Name: "test-backup-1g"},
					"blub", nil),
			},
			targetSize: "4G",
		},
		"don't sync again": {
			namespace: "t5",
			syncImage: "blub",
			in: state{
				source: newSource("t5", "test", "1G"),
				backup: newBackup("t5", "test-backup-1g", "1G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{DoneAnnotation: "true"}
						return pvc
					}),
			},
			out: state{
				source: newSource("t5", "test", "1G"),
				backup: newBackup("t5", "test-backup-1g", "1G"),
			},
			targetSize: "4G",
			done:       true,
		},
		"critical, if no source is present": {
			namespace: "f1",
			syncImage: "blub",
			pvcInfo: &pvcInfo{
				Name:      "test",
				Namespace: "f1",
				Labels:    newSource("f1", "test", "1G").Labels,
				Spec:      newSource("f1", "test", "1G").Spec,
			},
			in: state{
				source: nil,
			},
			out:        state{},
			targetSize: "4G",
			err:        errCritical,
		},
		"abort, if too small backup exists": {
			namespace: "f2",
			syncImage: "blub",
			in: state{
				source: newSource("f2", "test", "2G"),
				backup: newBackup("f2", "test-backup-2g", "1G"),
			},
			out:        state{},
			targetSize: "4G",
			err:        errAbort,
		},
		"abort, if job failed": {
			namespace: "f3",
			syncImage: "blub",
			in: state{
				source: newSource("f3", "test", "1G"),
				backup: newBackup("f3", "test-backup-2g", "1G"),
				job: newTestJob("f3",
					client.ObjectKey{Namespace: "f3", Name: "test"},
					client.ObjectKey{Namespace: "f3", Name: "test-backup-1g"},
					"blub", &jobFailed),
			},
			out:        state{},
			targetSize: "4G",
			err:        errAbort,
		},
	}

	for k, tc := range tcs {
		tc := tc
		ctx := context.Background()
		k := k

		t.Run(k, func(t *testing.T) {
			t.Parallel()
			assert := assert.New(t)
			require := require.New(t)
			require.Nil(k8sClient.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: tc.namespace},
			}))
			if tc.in.source != nil {
				require.Nil(k8sClient.Create(ctx, tc.in.source))
			}
			if tc.in.backup != nil {
				require.Nil(k8sClient.Create(ctx, tc.in.backup))
			}
			if tc.in.job != nil {
				stat := tc.in.job.Status
				require.Nil(k8sClient.Create(ctx, tc.in.job))
				tc.in.job.Status = stat // Create removes status
				require.Nil(k8sClient.Status().Update(ctx, tc.in.job))
			}
			r := StatefulSetReconciler{
				Client:             k8sClient,
				Scheme:             k8sClient.Scheme(),
				SyncContainerImage: tc.syncImage,
			}
			pi := pvcInfo{}
			if tc.in.source != nil {
				pi = pvcInfo{
					Name:      tc.in.source.Name,
					Namespace: tc.in.source.Namespace,
					Labels:    tc.in.source.Labels,
					Spec:      tc.in.source.Spec,
				}
			}
			if tc.pvcInfo != nil {
				pi = *tc.pvcInfo
			}
			pi.TargetSize = resource.MustParse(tc.targetSize)

			err := r.backupPVC(ctx, pi)

			if tc.err != nil {
				require.ErrorIs(err, tc.err)
				return
			}
			if !tc.done {
				require.ErrorIs(err, errInProgress)
			} else {
				require.Nil(err)
			}

			if tc.out.source != nil {
				assert.Eventually(func() bool {
					return pvcExists(ctx, tc.out.source)
				}, timeout, interval, "Source is not as expected")
			} else if tc.in.source != nil {
				assert.Eventually(func() bool {
					return pvcNotExists(ctx, tc.in.source)
				}, timeout, interval, "Source not deleted")
			}
			if tc.out.backup != nil {
				assert.Eventually(func() bool {
					return pvcExists(ctx, tc.out.backup)
				}, timeout, interval, "Backup is not as expected")
			} else if tc.in.backup != nil {
				assert.Eventually(func() bool {
					return pvcNotExists(ctx, tc.in.backup)
				}, timeout, interval, "Backup not deleted")
			}
			if tc.out.job != nil {
				assert.Eventually(func() bool {
					return jobExists(ctx, tc.out.job)
				}, timeout, interval, "Job is not as expected")
			} else if tc.in.job != nil {
				assert.Eventually(func() bool {
					return jobNotExists(ctx, tc.in.job)
				}, timeout, interval, "Job not deleted")
			}
		})
	}
}
