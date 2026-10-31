# Every target CI runs is a target here, with the same flags, so that
# `make check` locally is what the `static` and `unit` jobs run. Tool versions
# come from mise.toml (`mise install`), except govulncheck, pinned below.

# Run the pinned toolchain only; never download a different one.
export GOTOOLCHAIN := local
export CGO_ENABLED := 0

GOVULNCHECK_VERSION := v1.9.0

VERSION ?= dev
IMAGE   ?= csi-miami
LDFLAGS := -s -w -X main.version=$(VERSION)

.DEFAULT_GOAL := build

.PHONY: build test test-race cover vet fmt-check fmt lint tidy tidy-check vuln image check

build:
	mkdir -p bin
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/miami ./cmd/miami

test:
	go test ./...

# The race detector needs cgo, so test-race switches it on. GOTESTFLAGS lets
# callers add flags (cover, and CI's coverage artifact) without changing the
# base set.
GOTESTFLAGS ?=

test-race: export CGO_ENABLED := 1
test-race:
	go test -race -shuffle=on $(GOTESTFLAGS) ./...

cover:
	$(MAKE) test-race GOTESTFLAGS="-covermode=atomic -coverprofile=coverage.out"
	go tool cover -func=coverage.out | tail -n 1

vet:
	go vet ./...

fmt-check:
	@out="$$(gofmt -s -l .)"; \
	if [ -n "$$out" ]; then echo "gofmt -s needed on:"; echo "$$out"; exit 1; fi

fmt:
	gofmt -s -w .

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

tidy-check:
	@cp go.mod go.mod.orig; \
	if [ -f go.sum ]; then cp go.sum go.sum.orig; fi; \
	go mod tidy; \
	status=0; \
	cmp -s go.mod go.mod.orig || status=1; \
	if [ -f go.sum.orig ]; then cmp -s go.sum go.sum.orig || status=1; \
	elif [ -f go.sum ]; then status=1; fi; \
	mv go.mod.orig go.mod; \
	if [ -f go.sum.orig ]; then mv go.sum.orig go.sum; fi; \
	if [ $$status -ne 0 ]; then echo "go mod tidy changes go.mod or go.sum; run make tidy"; fi; \
	exit $$status

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

# Image for the host architecture, loaded into the local Docker daemon.
image:
	docker buildx build --load --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

check: fmt-check vet lint tidy-check vuln test-race
