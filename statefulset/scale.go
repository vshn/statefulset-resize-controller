package statefulset

import (
	"fmt"
	"strconv"
)

// ReplicasAnnotation stores the initial number of replicas before scaling down the StatefulSet.
const ReplicasAnnotation = "sts-resize.vshn.net/replicas"

// ScaleUpAnnotation marks a replica as in the process of scaling back up and prevents the controller from scaling it down.
const ScaleUpAnnotation = "sts-resize.vshn.net/scalup"

// PrepareScaleDown changes the replica to 0, if applicable.
// It saves the original state and returns true if it ran successfully before and the StatefulSet is scaled to 0.
func (s *Entity) PrepareScaleDown() bool {
	if s.isScaledDown() || s.isScalingUp() {
		return true
	}
	s.saveOriginalReplicaCount()
	r := int32(0)
	s.sts.Spec.Replicas = &r
	return false
}

// PrepareScaleUp updates the replica count to the original replicas.
// Returns true if it ran successfully before and the StatefulSet is scaled up.
func (s Entity) PrepareScaleUp() (bool, error) {
	scale, err := s.getOriginalReplicaCount()
	if err != nil {
		return false, fmt.Errorf("failed to get original scale as %s is not readable: %w", ReplicasAnnotation, err)
	}
	if s.isScaledUp(scale) {
		s.unmarkScalingUp()
		s.clearOriginalReplicaCount()
		return true, nil
	}
	s.markScalingUp()
	s.sts.Spec.Replicas = &scale
	return false, nil
}

func (s Entity) isScaledDown() bool {
	// NOTE(glrf) Checking CurrentRevision is important to prevent a race condition.
	// This makes sure that the k8s controller manager ran before us and that the set status is correct and not just uninitialized
	return s.sts.Spec.Replicas != nil &&
		*s.sts.Spec.Replicas == 0 &&
		s.sts.Status.CurrentRevision != "" &&
		s.sts.Status.Replicas == 0
}

func (s Entity) isScaledUp(scale int32) bool {
	return s.sts.Spec.Replicas != nil &&
		*s.sts.Spec.Replicas == scale &&
		s.sts.Status.CurrentRevision != "" &&
		s.sts.Status.Replicas == scale
}

func (s Entity) saveOriginalReplicaCount() {
	if s.sts.Annotations[ReplicasAnnotation] == "" {
		if s.sts.Annotations == nil {
			s.sts.Annotations = map[string]string{}
		}
		s.sts.Annotations[ReplicasAnnotation] = strconv.Itoa(int(*s.sts.Spec.Replicas))
	}
}

func (s Entity) getOriginalReplicaCount() (int32, error) {
	scale, err := strconv.Atoi(s.sts.Annotations[ReplicasAnnotation])
	if err != nil {
		return 0, err
	}
	return int32(scale), nil
}

func (s Entity) clearOriginalReplicaCount() {
	delete(s.sts.Annotations, ReplicasAnnotation)
}

func (s *Entity) markScalingUp() {
	if s.sts.Annotations == nil {
		s.sts.Annotations = map[string]string{}
	}
	s.sts.Annotations[ScaleUpAnnotation] = "true"
}

func (s Entity) isScalingUp() bool {
	return s.sts.Annotations[ScaleUpAnnotation] == "true"
}

func (s *Entity) unmarkScalingUp() {
	delete(s.sts.Annotations, ScaleUpAnnotation)
}
