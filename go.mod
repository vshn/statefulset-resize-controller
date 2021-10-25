module github.com/vshn/statefulset-resize-controller

go 1.16

require (
	github.com/stretchr/testify v1.7.0
	k8s.io/api v0.21.3
	k8s.io/apimachinery v0.21.3
	k8s.io/client-go v0.21.3
	sigs.k8s.io/controller-runtime v0.9.5
	sigs.k8s.io/controller-runtime/tools/setup-envtest v0.0.0-20211025141024-c73b143dc503
	sigs.k8s.io/controller-tools v0.6.2
	sigs.k8s.io/kustomize/kustomize/v3 v3.10.0
)
