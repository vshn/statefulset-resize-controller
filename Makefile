# Set Shell to bash, otherwise some targets fail with dash/zsh etc.
SHELL := /bin/bash

setup-envtest ?= go run sigs.k8s.io/controller-runtime/tools/setup-envtest

# Run tests (see https://sdk.operatorframework.io/docs/building-operators/golang/references/envtest-setup)
ENVTEST_ASSETS_DIR=$(shell pwd)/testbin

all: fmt vet build

.PHONY: build
build: 
	CGO_ENABLED=0 go build


run: fmt vet ## Run against the configured Kubernetes cluster in ~/.kube/config
	go run ./main.go

.PHONY: test
test: export ENVTEST_K8S_VERSION = 1.19.x
test: fmt  ## Run tests 
	mkdir -p ${ENVTEST_ASSETS_DIR}
	$(setup-envtest) use '$(ENVTEST_K8S_VERSION)!'
	export KUBEBUILDER_ASSETS="$$($(setup-envtest) use -i -p path '$(ENVTEST_K8S_VERSION)!')"; \
		env | grep KUBEBUILDER; \
	go test ./... -coverprofile cover.out

.PHONY: fmt
fmt: generate ## Run go fmt against code
	go fmt ./...

.PHONY: vet
vet: generate ## Run go vet against code
	go vet ./...

.PHONY: lint
lint: fmt vet ## Invokes the fmt and vet targets
	@echo 'Check for uncommitted changes ...'
	git diff --exit-code

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: generate
generate:  ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	go generate

