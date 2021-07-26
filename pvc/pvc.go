package pvc

import (
	"fmt"
	"hash/crc64"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ManagedLabel is a label to mark resources to be managed by the controller
const ManagedLabel = "sts-resize.appuio.ch/managed"

// NewEntity returns a new pvc Info
func NewEntity(pvc corev1.PersistentVolumeClaim, growTo resource.Quantity) Entity {
	return Entity{
		SourceName: pvc.Name,
		Namespace:  pvc.Namespace,
		Labels:     pvc.Labels,
		TargetSize: growTo,
		Spec:       pvc.Spec,
	}
}

// Entity describs a resizable PVC
type Entity struct {
	Namespace  string
	SourceName string

	Labels     map[string]string
	Spec       corev1.PersistentVolumeClaimSpec
	TargetSize resource.Quantity

	BackedUp bool
	Restored bool
}

// BackupName return the name of the backup
func (pi Entity) BackupName() string {
	maxNameLength := 63
	q := pi.Spec.Resources.Requests[corev1.ResourceStorage]
	suffix := fmt.Sprintf("-backup-%s", q.String())
	name := shortenString(pi.SourceName, maxNameLength-len(suffix))
	return strings.ToLower(fmt.Sprintf("%s%s", name, suffix))
}

var crc64Table = crc64.MakeTable(crc64.ISO)

// shortenString deterministically shortens the provided string to the maximum of l characters.
// The function cannot shorten below a length of 16.
// This needs to be deterministic, as we use it to find existing backup pvcs.
// It does this by taking the CRC64 has of the complete string, truncate the name to the first l-16 characters, and appending the hash in hex.
// When using this function for backup pvcs, if we have 100'000 backups of pvcs, that start with the same ~37 letters, that are longer than ~53 letters, and have the same size, in one namespace, the likelihood of a collision, which would cause old backups to be overwritten is less than 1 in 1 Billion.
func shortenString(s string, l int) string {
	if len(s) <= l {
		return s
	}
	if l < 16 {
		return s
	}
	h := crc64.New(crc64Table)
	h.Write([]byte(s))
	return fmt.Sprintf("%s%16x", s[:l-16], h.Sum64())
}

// GetBackup returns a pvc resource for the backup
func (pi Entity) GetBackup() *corev1.PersistentVolumeClaim {
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
func (pi Entity) GetResizedSource() *corev1.PersistentVolumeClaim {
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
