package statefulset

import (
	"fmt"
	"strconv"
)

// ReplicasAnnotation stores the initial number of replicas before scaling down the StatefulSet.
const ReplicasAnnotation = "sts-resize.appuio.ch/replicas"

// ScalupAnnotation marks a replica as in the process of scaling back up and prevents the controller from scaling it down.
const ScalupAnnotation = "sts-resize.appuio.ch/scalup"

// ScaleDown scales down the replica to 0, saves the original state and returns true if it ran successfully before
func (s *Info) ScaleDown() bool {
	if s.scaledDown() || s.isScalingUp() {
		return true
	}
	s.saveOriginalReplicas()
	r := int32(0)
	s.sts.Spec.Replicas = &r
	return false
}

// ScaleUp scales back up to the original scale.
// Returns true if it ran successfully before
func (s Info) ScaleUp() (bool, error) {
	scale, err := s.getOriginalReplicas()
	if err != nil {
		return false, fmt.Errorf("failed to get original scale as %s is not readable: %w", ReplicasAnnotation, err)
	}
	if s.scaledUp(scale) {
		s.unmarkScalingUp()
		s.clearOriginalReplicas()
		return true, nil
	}
	s.markScalingUp()
	s.sts.Spec.Replicas = &scale
	return false, nil
}

func (s Info) scaledDown() bool {
	// NOTE(glrf) Checking CurrentRevision is important to prevent a race condition.
	// This makes sure that the k8s controller manager ran before us and that the set status is correct and not just uninitialized
	return s.sts.Spec.Replicas != nil &&
		*s.sts.Spec.Replicas == 0 &&
		s.sts.Status.CurrentRevision != "" &&
		s.sts.Status.Replicas == 0
}

func (s Info) scaledUp(scale int32) bool {
	return s.sts.Spec.Replicas != nil &&
		*s.sts.Spec.Replicas == scale &&
		s.sts.Status.CurrentRevision != "" &&
		s.sts.Status.Replicas == scale
}

func (s Info) saveOriginalReplicas() {
	if s.sts.Annotations[ReplicasAnnotation] == "" {
		if s.sts.Annotations == nil {
			s.sts.Annotations = map[string]string{}
		}
		s.sts.Annotations[ReplicasAnnotation] = strconv.Itoa(int(*s.sts.Spec.Replicas))
	}
}

func (s Info) getOriginalReplicas() (int32, error) {
	scale, err := strconv.Atoi(s.sts.Annotations[ReplicasAnnotation])
	if err != nil {
		return 0, err
	}
	return int32(scale), nil
}

func (s Info) clearOriginalReplicas() {
	delete(s.sts.Annotations, ReplicasAnnotation)
}

func (s *Info) markScalingUp() {
	if s.sts.Annotations == nil {
		s.sts.Annotations = map[string]string{}
	}
	s.sts.Annotations[ScalupAnnotation] = "true"
}

func (s Info) isScalingUp() bool {
	return s.sts.Annotations[ScalupAnnotation] == "true"
}

func (s *Info) unmarkScalingUp() {
	delete(s.sts.Annotations, ScalupAnnotation)
}
