package pvc

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ManagedLabel is a label to mark resources to be managed by the controller
const ManagedLabel = "sts-resize.appuio.ch/managed"

// NewInfo returns a new pvc Info
func NewInfo(pvc corev1.PersistentVolumeClaim, growTo resource.Quantity) Info {
	return Info{
		SourceName: pvc.Name,
		Namespace:  pvc.Namespace,
		Labels:     pvc.Labels,
		TargetSize: growTo,
		Spec:       pvc.Spec,
	}
}

// Info describs a resizable PVC
type Info struct {
	Namespace  string
	SourceName string

	Labels     map[string]string
	Spec       corev1.PersistentVolumeClaimSpec
	TargetSize resource.Quantity

	BackedUp bool
	Restored bool
}

// BackupName return the name of the backup
func (pi Info) BackupName() string {
	q := pi.Spec.Resources.Requests[corev1.ResourceStorage] // Necessary because pointer receiver
	return strings.ToLower(fmt.Sprintf("%s-backup-%s", pi.SourceName, q.String()))
}

// GetBackup returns a pvc resource for the backup
func (pi Info) GetBackup() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pi.BackupName(),
			Namespace: pi.Namespace,
			Labels: map[string]string{
				ManagedLabel: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      pi.Spec.AccessModes,
			Resources:        pi.Spec.Resources,
			StorageClassName: pi.Spec.StorageClassName,
			VolumeMode:       pi.Spec.VolumeMode,
		},
	}
}

// GetResizedSource returns a pvc resource for the enlarged original PVC
func (pi Info) GetResizedSource() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pi.SourceName,
			Namespace: pi.Namespace,
			Labels:    pi.Labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: pi.Spec.AccessModes,
			Resources: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceStorage: pi.TargetSize,
				},
			},
			StorageClassName: pi.Spec.StorageClassName,
			VolumeMode:       pi.Spec.VolumeMode,
		},
	}
}
