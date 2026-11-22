SHELL := /usr/bin/env bash

.PHONY: setup-hooks fmt fmt-check check-adr check-scripts lint test test-integration build up down migrate verify-m1

GOLANGCI_LINT ?= golangci-lint
BUILD_DIR ?= .build
COMPOSE_FILE ?= deploy/docker-compose.yml

setup-hooks:
	pre-commit install --install-hooks

fmt:
	find . -path './.*' -prune -o -name '*.go' -exec gofmt -w {} +

fmt-check:
	@set -euo pipefail; \
	unformatted="$$(find . -path './.*' -prune -o -name '*.go' -exec gofmt -l {} +)"; \
	if [ -n "$$unformatted" ]; then \
		printf "Files need gofmt:\n%s\n" "$$unformatted"; \
		exit 1; \
	fi

check-adr:
	@set -euo pipefail; \
	dups="$$(ls docs/adr | sed -n 's/^\([0-9]\{4\}\)-.*/\1/p' | sort | uniq -d)"; \
	if [ -n "$$dups" ]; then \
		printf "Duplicate ADR numeric prefixes:\n%s\n" "$$dups"; \
		exit 1; \
	fi

check-scripts:
	@set -euo pipefail; \
	shopt -s nullglob; \
	scripts=(scripts/*.sh); \
	if [ "$${#scripts[@]}" -eq 0 ]; then \
		exit 0; \
	fi; \
	for s in "$${scripts[@]}"; do \
		bash -n "$$s"; \
	done; \
	if command -v shellcheck >/dev/null 2>&1; then \
		shellcheck "$${scripts[@]}"; \
	elif [ "$${REQUIRE_SHELLCHECK:-0}" = "1" ]; then \
		echo "shellcheck is required when REQUIRE_SHELLCHECK=1" >&2; \
		exit 1; \
	else \
		echo "shellcheck not installed locally; ran bash -n only (use pre-commit or REQUIRE_SHELLCHECK=1 for full lint)" >&2; \
	fi

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

migrate:
	go run ./cmd/ferry-server -migrate-only

verify-m1:
	./scripts/verify_m1.sh
