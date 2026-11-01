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

.PHONY: build test test-race cover vet fmt-check fmt lint tidy tidy-check vuln content-hash image check smoke e2e envtest sanity deploy-check

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

# The PV paths of the controller against a real API server and etcd, started
# by envtest (build tag envtest). setup-envtest, pinned in mise.toml, downloads
# the binaries of the Kubernetes version below on first use and prints where
# they are. Not part of `check`: it is the envtest CI job.
ENVTEST_K8S_VERSION := 1.37.0

envtest:
	KUBEBUILDER_ASSETS="$$(setup-envtest use $(ENVTEST_K8S_VERSION) --print path)" \
		go test -tags envtest -count=1 ./internal/csi

# The csi-sanity suite (pinned in mise.toml) against the identity, controller
# and node services served together, on a local transport, with real bind
# mounts for the node half; and the tests of the delete path that mount real
# file systems (a tmpfs, bind mounts) and race a rename or a mount against the
# removal. ROOT ONLY: run it as `sudo -E env "PATH=$PATH" make sanity`, with
# the pinned Go and csi-sanity on PATH. A separate CI job, not part of `check`. The suite serves on a unix socket under
# TMPDIR, whose path is limited to about 100 bytes: if TMPDIR is long, point it
# at a short directory (TMPDIR=/tmp) for the run.
sanity:
	hack/sanity.sh

# End-to-end tests of one transport, chosen by E2E_TRANSPORT, in the mode
# E2E_MODE (default transport).
#   transport: the transport package's test against a real server (nfs3: a
#     kernel NFSv3 server in a privileged container, see hack/nfs-server).
#   kind: the driver on a kind cluster of one control plane and two workers and
#     the criteria tests of test/e2e (hack/e2e-kind.sh). E2E_IMAGE is a driver
#     image in the local Docker daemon; `make image` builds one.
# ROOT ONLY (nfs3 loads kernel modules), and Docker is needed: run it as
# `sudo -E env "PATH=$PATH" make e2e E2E_TRANSPORT=nfs3`, with the pinned Go on
# PATH. A separate CI job, not part of `check`.
E2E_TRANSPORT ?=
E2E_MODE ?=
E2E_IMAGE ?=

e2e:
	E2E_TRANSPORT="$(E2E_TRANSPORT)" E2E_MODE="$(E2E_MODE)" E2E_IMAGE="$(E2E_IMAGE)" hack/e2e.sh

# The deploy check: the e2e overlay on a kind cluster (one control plane, two
# workers), both pods Ready, /healthz and /metrics scraped through the API
# server. DEPLOY_IMAGE is a driver image in the local Docker daemon; by default
# `make image` builds one. Needs docker, kind and kubectl (mise.toml); CI runs
# it as the deploy job with the image it pulled. On failure,
# DEPLOY_ARTIFACT_DIR receives the logs.
DEPLOY_IMAGE ?=

deploy-check:
	@image="$(DEPLOY_IMAGE)"; \
	if [ -z "$$image" ]; then \
		$(MAKE) image || exit 1; \
		image="$(IMAGE):x-$$(hack/content-hash.sh)"; \
		docker image inspect "$$image" >/dev/null 2>&1 || image="$$image-dirty"; \
	fi; \
	hack/kind.sh --image "$$image"

# Exactly what CI runs: the static job runs fmt-check vet lint tidy-check vuln
# and the unit job runs cover.
check: fmt-check vet lint tidy-check vuln cover
