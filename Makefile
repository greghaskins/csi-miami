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

.PHONY: build test test-race cover vet fmt-check fmt lint tidy tidy-check vuln content-hash image check smoke

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
	go tool cover -func=coverage.out

vet:
	go vet ./...

# Only tracked Go files are checked, so untracked trees (other worktrees,
# build output) cannot change the answer. gofmt's exit status is checked as
# well as its output.
fmt-check:
	@files="$$(git ls-files '*.go')"; \
	if [ -z "$$files" ]; then echo "fmt-check: no Go files"; exit 0; fi; \
	out="$$(gofmt -s -l $$files)" || { echo "$$out"; exit 1; }; \
	if [ -n "$$out" ]; then echo "gofmt -s needed on:"; echo "$$out"; exit 1; fi

fmt:
	gofmt -s -w $$(git ls-files '*.go')

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

# -diff exits non-zero on drift or on error, and never writes.
tidy-check:
	go mod tidy -diff

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

content-hash:
	@hack/content-hash.sh

# Image for the host architecture, loaded into the local Docker daemon. It is
# built by hack/image-build.sh, the script CI uses, and tagged and versioned
# x-<content hash>. A working tree whose build inputs differ from the index
# gets "-dirty" on the tag and version, so a local build is never labelled as
# the committed hash. Local builds carry no attestations.
image:
	@hash="$$(hack/content-hash.sh)" || exit 1; \
	hack/image-build.sh --arch "$$(go env GOARCH)" --tag "$(IMAGE):x-$$hash" --load

# Smoke tests against the real kernel (mount, bind, remount, unmount), run as
# root under `unshare --mount --propagation private` by hack/smoke.sh. ROOT
# ONLY: run it as `sudo -E make smoke`, with the pinned Go on PATH. It is a
# separate CI job and not part of `check`.
smoke:
	hack/smoke.sh

# Exactly what CI runs: the static job runs fmt-check vet lint tidy-check vuln
# and the unit job runs cover.
check: fmt-check vet lint tidy-check vuln cover
