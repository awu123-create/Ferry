SHELL := /usr/bin/env bash

.PHONY: install-tools setup-hooks fmt fmt-check check-adr check-scripts lint test test-integration build up down migrate verify-m1

GOLANGCI_LINT ?= $(firstword $(wildcard $(shell go env GOBIN)/golangci-lint $(shell go env GOPATH)/bin/golangci-lint) golangci-lint)
GOLANGCI_LINT_VERSION ?= v2.15.0
SHELLCHECK ?= shellcheck
BUILD_DIR ?= .build
COMPOSE_FILE ?= deploy/docker-compose.yml

install-tools:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	@if ! command -v pre-commit >/dev/null 2>&1; then \
		if command -v uv >/dev/null 2>&1; then \
			uv tool install pre-commit; \
		elif command -v pipx >/dev/null 2>&1; then \
			pipx install pre-commit; \
		elif command -v brew >/dev/null 2>&1; then \
			brew install pre-commit; \
		else \
			echo "pre-commit not found; install uv, pipx, or Homebrew first" >&2; \
			exit 1; \
		fi; \
	fi

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
	fi; \
	code_bare="$$( { grep -rnE --exclude='*.md' 'ADR-[0-9]{4}' cmd internal tests scripts migrations || [ $$? -eq 1 ]; } )"; \
	if [ -n "$$code_bare" ]; then \
		printf "Bare ADR references in code/scripts (use full repo path docs/adr/NNNN-*.md):\n%s\n" "$$code_bare"; \
		exit 1; \
	fi; \
	doc_bare="$$(find . \( -path './.git' -o -path './.worktrees' -o -path './.agents' -o -path './.claude' -o -path './.build' \) -prune -o -name '*.md' -print0 | xargs -0 perl -ne ' \
		if ($$ARGV =~ m{^\./docs/adr/[0-9]{4}-.*\.md$$} && $$. == 1 && /^# ADR-[0-9]{4}: /) { \
			close ARGV if eof; \
			next; \
		} \
		$$line = $$_; \
		$$line =~ s/\[(?:[^\[\]]|\[[^\[\]]*\])*\]\((?:[^)]*\/)?[0-9]{4}-[^)]+\.md(?:#[^)]+)?\)/ /g; \
		if ($$line =~ /(?<![0-9A-Za-z])ADR-[0-9]{4}(?![0-9])/) { \
			(my $$clean = $$ARGV) =~ s{^\./}{}; \
			print "$$clean:$$.:$$_"; \
		} \
		close ARGV if eof; \
	')"; \
	if [ -n "$$doc_bare" ]; then \
		printf "Bare ADR references in Markdown (link to docs/adr/NNNN-*.md):\n%s\n" "$$doc_bare"; \
		exit 1; \
	fi

check-scripts:
	SHELLCHECK="$(SHELLCHECK)" ./scripts/check_scripts.sh

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
