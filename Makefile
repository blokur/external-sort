.PHONY: help
help: ## Show this
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

tag=v1.0.0
docker_image=askiada/external-sort
run="."
dir="./..."
short="-short"
flags=""
timeout=5m

include ./env.list
export $(shell sed 's/=.*//' ./env.list)


.PHONY: test
test:
	go test ./...

.PHONY: test_race
test_race:
	go test -race ./...

.PHONY: run
run: build
	./bin/external-sort

.PHONY: build
build:
	go build -o bin/external-sort main.go

.PHONY: build_docker
build_docker: ## Build a docker image from current git sha
	@docker build \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		-t $(docker_image):$(tag) .

.PHONY: unit_test
unit_test: ## Run unit tests. You can set: [run, timeout, short, dir, flags]. Example: make unit_test flags="-race".
	@go mod tidy; go test -trimpath --timeout=$(timeout) $(short) $(dir) -run $(run) $(flags)

.PHONY: unit_test_watch
unit_test_watch: ## Run unit tests in watch mode. You can set: [run, timeout, short, dir, flags]. Example: make unit_test flags="-race".
	@echo "running tests on $(run). waiting for changes..."
	@-zsh -c "go mod tidy; go test -trimpath --timeout=$(timeout) $(short) $(dir) -run $(run) $(flags); repeat 100 printf '#'; echo"
	@reflex -d none -r "(\.go$$)|(go.mod)" -- zsh -c "go mod tidy; go test -trimpath --timeout=$(timeout) $(short) $(dir) -run $(run) $(flags); repeat 100 printf '#'"

.PHONY: dependencies
dependencies: ## Install dependencies requried for development operations.
	@go install github.com/cespare/reflex@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.44.0
	@go get -u -d ./...
	@go mod tidy

.PHONY: lint
lint: ## Run linters.
	go fmt ./...
	go vet ./...
	golangci-lint run ./...

.PHONY: ci_tests
ci_tests: ## Run tests for CI environment.
	go test -trimpath --timeout=10m -failfast -v -race -covermode=atomic -coverprofile=coverage.out ./...

.PHONY: clean
clean: ## Clean test caches and tidy up modules.
	@go clean -testcache
	@go mod tidy
