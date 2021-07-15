package controllers

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("restorePVC", func() {
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
		"initiate retore": {
			namespace: "r1",
			syncImage: "blub",
			in: state{
				source: newSource("r1", "test", "1G"),
				backup: newBackup("r1", "test-backup-1g", "1G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
			},
			out: state{
				backup: newBackup("r1", "test-backup-1g", "1G"),
			},
			targetSize: "4G",
			done:       false,
		},
		"recreate source pvc and start restore": {
			namespace: "r2",
			syncImage: "blub",
			pvcInfo: &pvcInfo{
				Name:      "test",
				Namespace: "r2",
				Labels:    newSource("r1", "test", "1G").Labels,
				Spec:      newSource("r1", "test", "1G").Spec,
			},
			in: state{
				backup: newBackup("r2", "test-backup-1g", "1G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
			},
			out: state{
				source: newSource("r2", "test", "4G"),
				backup: newBackup("r2", "test-backup-1g", "1G"),
				job: newTestJob("r2",
					client.ObjectKey{Namespace: "r2", Name: "test-backup-1g"},
					client.ObjectKey{Namespace: "r2", Name: "test"},
					"blub", nil),
			},
			targetSize: "4G",
			done:       false,
		},
		"finish restore": {
			namespace: "r3",
			syncImage: "blub",
			pvcInfo: &pvcInfo{
				Name:      "test",
				Namespace: "r3",
				Labels:    newSource("r3", "test", "1G").Labels,
				Spec:      newSource("r3", "test", "1G").Spec,
			},
			in: state{
				source: newSource("r3", "test", "4G"),
				backup: newBackup("r3", "test-backup-1g", "1G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
				job: newTestJob("r3",
					client.ObjectKey{Namespace: "r3", Name: "test-backup-1g"},
					client.ObjectKey{Namespace: "r3", Name: "test"},
					"blub", &jobSucceeded),
			},
			out: state{
				source: newSource("r3", "test", "4G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
				backup: newBackup("r3", "test-backup-1g", "1G"),
			},
			targetSize: "4G",
			done:       true,
		},
		"continue restore": {
			namespace: "r4",
			syncImage: "blub",
			pvcInfo: &pvcInfo{
				Name:      "test",
				Namespace: "r4",
				Labels:    newSource("r4", "test", "1G").Labels,
				Spec:      newSource("r4", "test", "1G").Spec,
			},
			in: state{
				source: newSource("r4", "test", "4G"),
				backup: newBackup("r4", "test-backup-1g", "1G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
			},
			out: state{
				source: newSource("r4", "test", "4G"),
				backup: newBackup("r4", "test-backup-1g", "1G"),
				job: newTestJob("r4",
					client.ObjectKey{Namespace: "r4", Name: "test-backup-1g"},
					client.ObjectKey{Namespace: "r4", Name: "test"},
					"blub", &jobSucceeded),
			},
			targetSize: "4G",
			done:       false,
		},
		"don't restore again": {
			namespace: "r5",
			syncImage: "blub",
			pvcInfo: &pvcInfo{
				Name:      "test",
				Namespace: "r5",
				Labels:    newSource("r5", "test", "1G").Labels,
				Spec:      newSource("r5", "test", "1G").Spec,
			},
			in: state{
				source: newSource("r5", "test", "4G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
				backup: newBackup("r5", "test-backup-1g", "1G"),
			},
			out: state{
				source: newSource("r5", "test", "4G"),
				backup: newBackup("r5", "test-backup-1g", "1G"),
			},
			targetSize: "4G",
			done:       true,
		},
		"abort, if backup is not present": {
			namespace: "fr1",
			syncImage: "blub",
			in: state{
				source: newSource("fr1", "test", "1G"),
			},
			targetSize: "4G",
			err:        errAbort,
		},
		"critical, if job failed": {
			namespace: "fr3",
			syncImage: "blub",
			pvcInfo: &pvcInfo{
				Name:      "test",
				Namespace: "fr3",
				Labels:    newSource("fr3", "test", "1G").Labels,
				Spec:      newSource("fr3", "test", "1G").Spec,
			},
			in: state{
				source: newSource("fr3", "test", "4G"),
				backup: newBackup("fr3", "test-backup-1g", "1G",
					func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
						pvc.Annotations = map[string]string{doneAnnotation: "true"}
						return pvc
					}),
				job: newTestJob("fr3",
					client.ObjectKey{Namespace: "fr3", Name: "test-backup-1g"},
					client.ObjectKey{Namespace: "fr3", Name: "test"},
					"blub", &jobFailed),
			},
			out:        state{},
			targetSize: "4G",
			err:        errCritical,
		},
	}

	for k, tc := range tcs {
		tc := tc
		ctx := context.Background()
		k := k

		It(k, func() {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: tc.namespace},
			})).To(Succeed())
			if tc.in.source != nil {
				Expect(k8sClient.Create(ctx, tc.in.source)).To(Succeed())
			}
			if tc.in.backup != nil {
				Expect(k8sClient.Create(ctx, tc.in.backup)).To(Succeed())
			}
			if tc.in.job != nil {
				stat := tc.in.job.Status
				Expect(k8sClient.Create(ctx, tc.in.job)).To(Succeed())
				tc.in.job.Status = stat // Create removes status
				Expect(k8sClient.Status().Update(ctx, tc.in.job)).To(Succeed())
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
			err := r.restorePVC(ctx, pi)

			if tc.err != nil {
				Expect(errors.Is(err, tc.err)).To(BeTrue(), "expect error %v got %v", tc.err, err)
				return
			}

			if !tc.done {
				Expect(err).To(MatchError(errInProgress))
			} else {
				Expect(err).To(Succeed())
			}

			if tc.out.source != nil {
				Eventually(func() (*corev1.PersistentVolumeClaim, error) {
					key := client.ObjectKeyFromObject(tc.out.source)
					pvc := &corev1.PersistentVolumeClaim{}
					err := k8sClient.Get(ctx, key, pvc)
					return pvc, err
				}, timeout, interval).Should(BeEquivalentPVC(tc.out.source))
			} else if tc.in.source != nil {
				Eventually(func() metav1.StatusReason {
					key := client.ObjectKeyFromObject(tc.in.source)
					pvc := &corev1.PersistentVolumeClaim{}
					if err := k8sClient.Get(ctx, key, pvc); err != nil {
						return apierrors.ReasonForError(err)
					}
					if pvc.DeletionTimestamp != nil {
						// This is needed as the testenv does not properly clean up pvcs
						return metav1.StatusReasonNotFound
					}
					return "found"

				}, timeout, interval).Should(Equal(metav1.StatusReasonNotFound))
			}
			if tc.out.backup != nil {
				Eventually(func() (*corev1.PersistentVolumeClaim, error) {
					key := client.ObjectKeyFromObject(tc.out.backup)
					pvc := &corev1.PersistentVolumeClaim{}
					err := k8sClient.Get(ctx, key, pvc)
					return pvc, err
				}, timeout, interval).Should(BeEquivalentPVC(tc.out.backup))
			} else if tc.in.backup != nil {
				Eventually(func() metav1.StatusReason {
					key := client.ObjectKeyFromObject(tc.in.backup)
					pvc := &corev1.PersistentVolumeClaim{}
					return apierrors.ReasonForError(k8sClient.Get(ctx, key, pvc))
				}, timeout, interval).Should(Equal(metav1.StatusReasonNotFound))
			}
			if tc.out.job != nil {
				Eventually(func() (*batchv1.Job, error) {
					key := client.ObjectKeyFromObject(tc.out.job)
					job := &batchv1.Job{}
					err := k8sClient.Get(ctx, key, job)
					return job, err
				}, timeout, interval).Should(BeEquivalentJob(tc.out.job))
			} else if tc.in.job != nil {
				Eventually(func() metav1.StatusReason {
					key := client.ObjectKeyFromObject(tc.in.job)
					job := &batchv1.Job{}
					if err := k8sClient.Get(ctx, key, job); err != nil {
						return apierrors.ReasonForError(err)
					}
					if job.DeletionTimestamp != nil {
						// This is needed as the testenv does not properly clean up jobs
						// Their stuck as there is a finalizer to remove pods
						return metav1.StatusReasonNotFound
					}
					return "found"
				}, timeout, interval).Should(Equal(metav1.StatusReasonNotFound))
			}
		})
	}
})
