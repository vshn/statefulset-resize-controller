package pvc

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestBackup(t *testing.T) {
	assert := assert.New(t)

	tcs := map[string]struct {
		name      string
		namespace string
		size      string
	}{
		"normal": {
			name:      "foo",
			namespace: "test",
			size:      "5G",
		},
		"long": {
			name:      "foooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo",
			namespace: "test",
			size:      "5G",
		},
		"google": {
			name:      "bar-with-quite-a-long-name-to-be-descriptive-right",
			namespace: "test",
			size:      "12500288599295772995277572885E",
		},
	}
	for k, tc := range tcs {
		t.Run(k, func(t *testing.T) {
			pi := Entity{
				Namespace:  tc.namespace,
				SourceName: tc.name,
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.ResourceRequirements{
						Requests: map[corev1.ResourceName]resource.Quantity{
							corev1.ResourceStorage: resource.MustParse(tc.size),
						},
					},
				},
			}

			bu := pi.GetBackup()
			assert.Equal(bu.Spec.Resources, pi.Spec.Resources)
			assert.Equal(bu.Spec.AccessModes, pi.Spec.AccessModes)
			assert.Equal(bu.Spec.StorageClassName, pi.Spec.StorageClassName)
			assert.Equal(bu.Spec.VolumeMode, pi.Spec.VolumeMode)

			assert.Equal(bu.Labels[ManagedLabel], "true")

			assert.Equal(bu.Namespace, pi.Namespace)
			assert.Regexp(regexp.MustCompile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?"), bu.Name)
			assert.LessOrEqual(len(bu.Name), 63)

		})
	}
}
