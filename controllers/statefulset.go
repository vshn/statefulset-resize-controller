// Functions manipulating the statefulset

package controllers

import (
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
)

const SizeAnnotation = "sts-resize.appuio.ch/size"
const ReplicasAnnotation = "sts-resize.appuio.ch/replicas"
const ScalupAnnotation = "sts-resize.appuio.ch/scalup"

// scaleDown will scale the StatefulSet to 0.
// Might return an errInProgress, signaling that the scaling is not completed and the caller needs to backoff
// scaleDown will not update the actual kubernetes resource, but expects the caller to upate the StatefulSet
func scaleDown(sts appsv1.StatefulSet) (appsv1.StatefulSet, error) {
	if *sts.Spec.Replicas == 0 && sts.Status.Replicas == 0 {
		return sts, nil
	}
	// If we are in the process of scaling up. We do not need to scale down
	//
	if sts.Annotations[ScalupAnnotation] == "true" {
		return sts, nil
	}
	if sts.Annotations[ReplicasAnnotation] == "" {
		sts.Annotations[ReplicasAnnotation] = strconv.Itoa(int(*sts.Spec.Replicas))
	}
	z := int32(0)
	sts.Spec.Replicas = &z
	return sts, errInProgress
}

// scaleUp will scale the StatefulSet to its original number of replicas.
// Might return an errInProgress, signaling that the scaling is not completed and the caller needs to backoff.
// scaleUp will not update the actual kubernetes resource, but expects the caller to upate the StatefulSet
// Expects to be called after scaleDown and that the original replica size is available as an annotation
func scaleUp(sts appsv1.StatefulSet) (appsv1.StatefulSet, error) {
	scale, err := strconv.Atoi(sts.Annotations[ReplicasAnnotation])
	if err != nil {
		return sts, newErrCritical(fmt.Sprintf("failed to get original scale as %s is not readable", ReplicasAnnotation))
	}
	scale32 := int32(scale) // need to add this to be able to dereference the int32 version
	sts.Annotations[ScalupAnnotation] = "true"

	if *sts.Spec.Replicas == scale32 && sts.Status.Replicas == scale32 {
		delete(sts.Annotations, ReplicasAnnotation)
		delete(sts.Annotations, ScalupAnnotation)
		return sts, nil
	}
	sts.Spec.Replicas = &scale32
	return sts, errInProgress
}
