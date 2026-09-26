# Makefile — the local quality gate for labor-performance.
#
# Every target below mirrors a sensor in .github/workflows/ci.yml, so the
# same feedback CI gives you post-push is available locally, pre-commit.
# See CLAUDE.md's "Local quality gate" section.
#
# Restored to match CI reality 2026-09-14: this file's header previously
# claimed "v1 deliberately has NO mutation/bdd targets", but
# .github/workflows/ci.yml has run bdd, arch-test, mutation-fast, mutation,
# vuln, and api-lint as real blocking/scheduled jobs all along -- there was
# just no local target to invoke them with, so `make check-all` covered far
# less than what actually gates a merge. Targets added below so the local
# gate is a true preview of CI again.

GO                 ?= go
GOLANGCI_LINT      ?= golangci-lint
GOLANGCI_VERSION   := v2.13.1
GREMLINS_VERSION   := v0.6.0

COVERAGE_OUT       := coverage.out
# internal/analytics is inside the gate alongside domain/application: it is
# the analytical READ MODEL (ADR 0007) — pure, dependency-free aggregation
# logic that decides whether a mean exists at all, which is exactly the
# kind of code the gate is for. Keep this list in sync with ci.yml's
# -coverpkg.
COVERAGE_PKGS      := ./internal/domain/...,./internal/application/...,./internal/analytics/...
COVERAGE_THRESHOLD := 90

.DEFAULT_GOAL := help

.PHONY: help build vet fmt fmt-check lint test coverage integration-kafka-testcontainers bdd contract arch-test mutation-fast mutation vuln api-lint check check-all

help:
	@echo "labor-performance — local quality gate (targets mirror .github/workflows/ci.yml)"
	@echo ""
	@echo "  help              Print this list of targets (default target)"
	@echo "  build             go build ./..."
	@echo "  vet               go vet ./..."
	@echo "  fmt               gofmt -w . — format the tree in place"
	@echo "  fmt-check         Fail if gofmt -l . is non-empty (the CI-style check)"
	@echo "  lint              golangci-lint run ./... (pinned $(GOLANGCI_VERSION) in CI)"
	@echo "  test              go test ./... -race — unit + httptest + fake-reader kafka, no broker/DB needed"
	@echo "  coverage          CI coverage command + the $(COVERAGE_THRESHOLD)% gate"
	@echo "  integration-kafka-testcontainers  Build-tagged Kafka consumer test with an isolated Testcontainers broker"
	@echo "  bdd               godog/Gherkin acceptance tests"
	@echo "  contract          scripts/contract-test.sh — Schemathesis vs apis/openapi.yaml"
	@echo "                    (boots the service in-memory; needs st: pip install"
	@echo "                     'schemathesis==4.28.0')"
	@echo "  arch-test         Architecture fitness tests (internal/architecture/)"
	@echo "  mutation-fast     Fast blocking mutation subset (internal/domain/performance)"
	@echo "  mutation          Exhaustive mutation run over the whole domain layer (slow)"
	@echo "  vuln              Known CVEs in the dependency graph and the Go stdlib"
	@echo "  api-lint          Spectral lint on openapi.yaml, openapi-reports.yaml, asyncapi.yaml"
	@echo ""
	@echo "  check             FAST bundle: fmt-check vet build lint test"
	@echo "  check-all         check + coverage + arch-test + bdd — run this before pushing"

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

fmt-check:
	@files=$$(gofmt -l .); \
	if [ -n "$$files" ]; then \
		echo "gofmt: the following files are not formatted:"; \
		echo "$$files" | sed 's/^/  /'; \
		echo "run 'make fmt' to fix them"; \
		exit 1; \
	fi; \
	echo "gofmt: clean"

lint:
	@if ! command -v $(GOLANGCI_LINT) >/dev/null 2>&1; then \
		echo "golangci-lint is not installed (or not on PATH)."; \
		echo "Install the exact version CI pins:"; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)"; \
		exit 1; \
	fi
	$(GOLANGCI_LINT) run ./...

test:
	$(GO) test ./... -race

coverage:
	$(GO) test ./... -race -coverprofile=$(COVERAGE_OUT) -coverpkg=$(COVERAGE_PKGS)
	@COVERAGE=$$($(GO) tool cover -func=$(COVERAGE_OUT) | awk '/^total:/ {print $$3}' | tr -d '%'); \
	echo "Coverage: $${COVERAGE}% (gate: $(COVERAGE_THRESHOLD)%)"; \
	if awk -v c="$$COVERAGE" -v t="$(COVERAGE_THRESHOLD)" 'BEGIN { exit !(c < t) }'; then \
		echo "coverage $${COVERAGE}% is below the $(COVERAGE_THRESHOLD)% gate"; \
		exit 1; \
	fi

# Starts an isolated Kafka Testcontainers broker; no external broker is needed.
integration-kafka-testcontainers:
	$(GO) test -tags=integration ./internal/adapters/inbound/kafka/... -run TestConsumer_ProjectsRealBrokerMessages -v

bdd:
	$(GO) test ./... -run TestFeatures -v

# Property-based contract tests against apis/openapi.yaml — mirrors the
# `contract` CI job. Not part of check/check-all (needs Python tooling
# installed); CI runs it as its own job on every push and PR.
contract:
	./scripts/contract-test.sh

arch-test:
	$(GO) test ./internal/architecture/... -v

mutation-fast:
	@if ! command -v gremlins >/dev/null 2>&1; then \
		echo "gremlins is not installed."; \
		echo "install the version CI pins with:"; \
		echo "  go install github.com/go-gremlins/gremlins/cmd/gremlins@$(GREMLINS_VERSION)"; \
		exit 1; \
	fi
	gremlins unleash ./internal/domain/performance

mutation:
	@if ! command -v gremlins >/dev/null 2>&1; then \
		echo "gremlins is not installed."; \
		echo "install the version CI pins with:"; \
		echo "  go install github.com/go-gremlins/gremlins/cmd/gremlins@$(GREMLINS_VERSION)"; \
		exit 1; \
	fi
	gremlins unleash ./internal/domain --workers 1 --timeout-coefficient 30

vuln:
	@if ! command -v govulncheck >/dev/null 2>&1; then \
		echo "govulncheck is not installed."; \
		echo "install it with:"; \
		echo "  go install golang.org/x/vuln/cmd/govulncheck@latest"; \
		exit 1; \
	fi
	govulncheck ./...

api-lint:
	@if ! command -v spectral >/dev/null 2>&1; then \
		echo "spectral is not installed."; \
		echo "install it with:"; \
		echo "  npm install -g @stoplight/spectral-cli@6.16.3"; \
		exit 1; \
	fi
	spectral lint apis/openapi.yaml --ruleset .spectral.yaml --fail-severity=warn
	spectral lint apis/openapi-reports.yaml --ruleset .spectral.yaml --fail-severity=warn
	spectral lint apis/asyncapi.yaml --ruleset .spectral.asyncapi.yaml --fail-severity=warn

# The fast self-correction loop: run this after every change, before committing.
check: fmt-check vet build lint test

# The fuller gate a human runs before pushing.
check-all: check coverage arch-test bdd
