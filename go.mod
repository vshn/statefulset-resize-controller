module github.com/vshn/statefulset-resize-controller

go 1.16

require (
	github.com/stretchr/testify v1.8.1
	k8s.io/api v0.27.4
	k8s.io/apimachinery v0.27.4
	k8s.io/client-go v0.27.4
	k8s.io/utils v0.0.0-20230209194617-a36077c30491
	sigs.k8s.io/controller-runtime v0.9.5
	sigs.k8s.io/controller-runtime/tools/setup-envtest v0.0.0-20210713022429-8b55f85c90c3
	sigs.k8s.io/controller-tools v0.6.2
	sigs.k8s.io/kustomize/kustomize/v3 v3.10.0
)
