package statefulset

import (
	"encoding/json"
	"fmt"

	"github.com/vshn/statefulset-resize-controller/pvc"
	appsv1 "k8s.io/api/apps/v1"
)

// FailedLabel is a label for failed sts resizing that need human interaction
const FailedLabel = "sts-resize.appuio.ch/failed"

// PvcAnnotation is an annotation in which the initial state of the pvcs is stored in
const PvcAnnotation = "sts-resize.appuio.ch/pvcs"

// Info contains all data to manage a statfulset resizing
type Info struct {
	Old  *appsv1.StatefulSet
	Pvcs []pvc.Info

	sts *appsv1.StatefulSet
}

// NewInfo return a new StatefulSet Info
func NewInfo(sts *appsv1.StatefulSet) (*Info, error) {
	si := Info{}
	si.sts = sts.DeepCopy()
	si.Old = sts
	if sts.Annotations[PvcAnnotation] != "" {
		if err := json.Unmarshal([]byte(sts.Annotations[PvcAnnotation]), &si.Pvcs); err != nil {
			return nil, fmt.Errorf("Annotation %s malformed", PvcAnnotation)
		}
	}
	return &si, nil
}

// Sts returns the updated StatefulSet resource
func (s *Info) Sts() (*appsv1.StatefulSet, error) {
	annotation, err := json.Marshal(s.Pvcs)
	if err != nil {
		return nil, err
	}
	if s.sts.Annotations == nil {
		s.sts.Annotations = map[string]string{}
	}
	s.sts.Annotations[PvcAnnotation] = string(annotation)

	return s.sts, nil
}

// Failed returns wether we previously failed to resize this statefulset
func (s Info) Failed() bool {
	return s.sts != nil &&
		s.sts.Labels != nil &&
		s.sts.Labels[FailedLabel] == "true"
}

// SetFailed sets this statefulset to a failed state
func (s Info) SetFailed() {
	if s.sts.Labels == nil {
		s.sts.Labels = map[string]string{}
	}
	s.sts.Labels[FailedLabel] = "true"
}

// Resizing returns wether we are resizing or should be resizing this statefulset
func (s Info) Resizing() bool {
	return len(s.Pvcs) != 0
}
