package controllers

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

var regexValidKubeName = regexp.MustCompile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?")

func TestNewJob(t *testing.T) {
	assert := assert.New(t)

	tcs := map[string]struct {
		src       string
		dst       string
		namespace string
		image     string
	}{
		"normal": {
			src:       "foo",
			dst:       "test",
			namespace: "space",
			image:     "foo/rsync",
		},
		"long": {
			src:       "foosaguagzeeauzuzfez98afaezfiueazfeaiuzfiueazfeaziufefeaaefufiueazgiu23",
			dst:       "testgeehhhauuebviuhajhjjhhhjhjfeihhejhfjehfkefhekfekfefjehfehfkjhejfe",
			namespace: "space",
			image:     "foo/rsync",
		},
	}
	for k, tc := range tcs {
		t.Run(k, func(t *testing.T) {
			job := newJob(tc.namespace, tc.image, tc.src, tc.dst)

			assert.Len(job.Spec.Template.Spec.Containers, 1)
			assert.Equal(job.Spec.Template.Spec.Containers[0].Image, tc.image)
			assert.Equal(job.Namespace, tc.namespace)

			assert.Regexp(regexValidKubeName, job.Name)
			assert.LessOrEqual(len(job.Name), 63)
		})
	}

}
