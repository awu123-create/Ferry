.PHONY: setup-hooks fmt lint test test-integration build up down

GOLANGCI_LINT ?= golangci-lint
BUILD_DIR ?= .build
COMPOSE_FILE ?= deploy/docker-compose.yml

setup-hooks:
	pre-commit install --install-hooks

fmt:
	find . -path './.*' -prune -o -name '*.go' -exec gofmt -w {} +

lint:
	$(GOLANGCI_LINT) config verify
	$(GOLANGCI_LINT) run ./...

test:
	go test -race -short ./...

test-integration:
	go test -race -count=1 ./...

build:
	mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/ ./cmd/...

up:
	docker compose -f $(COMPOSE_FILE) up -d --wait

down:
	docker compose -f $(COMPOSE_FILE) down
