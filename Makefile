
all: fmt vet build

.PHONY: build
build: 
	CGO_ENABLED=0 go build

.PHONY: test
test:
	go test ./... -coverprofile cover.out

.PHONY: fmt
fmt: ## Run go fmt against code
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code
	go vet ./...

.PHONY: lint
lint: fmt vet ## Invokes the fmt and vet targets
	@echo 'Check for uncommitted changes ...'
	git diff --exit-code
