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
		saname    string
		namespace string
		image     string
	}{
		"normal": {
			src:       "foo",
			dst:       "test",
			saname:    "",
			namespace: "space",
			image:     "foo/rsync",
		},
		"long": {
			src:       "foosaguagzeeauzuzfez98afaezfiueazfeaiuzfiueazfeaziufefeaaefufiueazgiu23",
			dst:       "testgeehhhauuebviuhajhjjhhhjhjfeihhejhfjehfkefhekfekfefjehfehfkjhejfe",
			saname:    "",
			namespace: "space",
			image:     "foo/rsync",
		},
		"normal_clusterrole": {
			src:       "foo",
			dst:       "test",
			saname:    "my-super-serviceaccount",
			namespace: "space",
			image:     "foo/rsync",
		},
	}
	for k, tc := range tcs {
		t.Run(k, func(t *testing.T) {
			job := newJob(tc.namespace, tc.image, tc.saname, tc.src, tc.dst)

			assert.Len(job.Spec.Template.Spec.Containers, 1)
			assert.Equal(job.Spec.Template.Spec.Containers[0].Image, tc.image)
			assert.Equal(job.Namespace, tc.namespace)
			assert.Equal(job.Spec.Template.Spec.ServiceAccountName, tc.saname)

			assert.Regexp(regexValidKubeName, job.Name)
			assert.LessOrEqual(len(job.Name), 63)
		})
	}

}

func TestNewSA(t *testing.T) {
	tcs := map[string]struct {
		namespace string
	}{
		"normal": {
			namespace: "space",
		},
	}
	for k, tc := range tcs {
		t.Run(k, func(t *testing.T) {
			sa := newSA(tc.namespace)

			assert.Equal(t, sa.Name, RbacObjName)
			assert.Equal(t, sa.Namespace, tc.namespace)
		})

	}
}

func TestNewRB(t *testing.T) {
	tcs := map[string]struct {
		namespace string
		saname    string
		crname    string
	}{
		"normal": {
			namespace: "space",
			saname:    "mysa",
			crname:    "myclusterrole",
		},
	}
	for k, tc := range tcs {
		t.Run(k, func(t *testing.T) {
			rb := newRB(tc.namespace, tc.saname, tc.crname)

			assert.Equal(t, rb.Name, RbacObjName)
			assert.Equal(t, rb.Namespace, tc.namespace)
			assert.Len(t, rb.Subjects, 1)
			assert.Equal(t, rb.Subjects[0].Name, tc.saname)
			assert.Equal(t, rb.RoleRef.Name, tc.crname)
		})

	}
}
