.PHONY: setup-hooks fmt lint test build

GOLANGCI_LINT ?= golangci-lint
BUILD_DIR ?= .build

setup-hooks:
	pre-commit install --install-hooks

fmt:
	find . -path './.*' -prune -o -name '*.go' -exec gofmt -w {} +

lint:
	$(GOLANGCI_LINT) config verify
	$(GOLANGCI_LINT) run ./...

test:
	go test -race -short ./...

build:
	mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/ ./cmd/...
