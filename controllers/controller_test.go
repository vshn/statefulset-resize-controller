package controllers

import (
	"time"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("StatefulSet controller", func() {

	// Define utility constants for object names and testing timeouts/durations and intervals.
	const (
		name      = "test"
		namespace = "default"

		timeout  = time.Second * 10
		duration = time.Second * 10
		interval = time.Millisecond * 250
	)

})
