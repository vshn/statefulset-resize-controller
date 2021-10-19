package controllers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRbacObjName(t *testing.T) {
	tcs := map[string]struct {
		stsname  string
		expected string
	}{
		"short": {
			stsname: "mysts",
		},
		"long": {
			stsname: "mysuperlongstsnamewhichwillbeshortenedbythecontroller",
		},
	}
	for k, tc := range tcs {
		t.Run(k, func(t *testing.T) {
			n := rbacObjName(tc.stsname)

			// regexValidKubeName is defined copy_test.go
			assert.Regexp(t, regexValidKubeName, n)
			assert.LessOrEqual(t, len(n), 63)
		})
	}
}

func TestNewSA(t *testing.T) {
	tcs := map[string]struct {
		namespace string
		name      string
	}{
		"normal": {
			namespace: "space",
			name:      "mysa",
		},
	}
	for k, tc := range tcs {
		t.Run(k, func(t *testing.T) {
			sa := newSA(tc.namespace, tc.name)

			assert.Equal(t, sa.Name, tc.name)
			assert.Equal(t, sa.Namespace, tc.namespace)
		})

	}
}

func TestNewRB(t *testing.T) {
	tcs := map[string]struct {
		namespace string
		name      string
		saname    string
		crname    string
	}{
		"normal": {
			namespace: "space",
			name:      "myrolebinding",
			saname:    "mysa",
			crname:    "myclusterrole",
		},
	}
	for k, tc := range tcs {
		t.Run(k, func(t *testing.T) {
			rb := newRB(tc.namespace, tc.name, tc.saname, tc.crname)

			assert.Equal(t, rb.Name, tc.name)
			assert.Equal(t, rb.Namespace, tc.namespace)
			assert.Len(t, rb.Subjects, 1)
			assert.Equal(t, rb.Subjects[0].Name, tc.saname)
			assert.Equal(t, rb.RoleRef.Name, tc.crname)
		})

	}
}
