#!/usr/bin/env bash
# Runs the smoke tests against the real kernel. Root only: they mount
# filesystems. The test binary is built once, then executed under
# `unshare --mount --propagation private`, so nothing it mounts is visible
# outside, or survives it. The test itself fails (never skips) if it is not
# root or the namespace is not private.
#
# The smoke tests live in internal/mount, which they exercise together with
# internal/observe, and in internal/transport, which they exercise on top of
# both. Each package is built as its own test binary and run in its own
# namespace.
#
# If SMOKE_ARTIFACT_DIR is set, a failing test leaves mountinfo-<test>.txt
# there: the lines of /proc/self/mountinfo for its own temporary paths, and
# nothing else.
# Run as `sudo -E make smoke`: root builds the test binary too, so Go's build
# and module caches get root-owned files. To keep them out of your own caches,
# point GOCACHE and GOMODCACHE at scratch directories for the run.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# The Makefile sets these; running this script directly must not differ.
export GOTOOLCHAIN=local
export CGO_ENABLED=0

if [ "$(id -u)" -ne 0 ]; then
  echo "smoke: must run as root (it mounts filesystems); try: sudo -E make smoke" >&2
  exit 1
fi
if ! command -v unshare >/dev/null 2>&1; then
  echo "smoke: unshare not found" >&2
  exit 1
fi

if [ -n "${SMOKE_ARTIFACT_DIR:-}" ]; then
  mkdir -p "$SMOKE_ARTIFACT_DIR"
fi

dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT

for pkg in mount transport; do
  go test -c -tags smoke -o "$dir/$pkg.test" "./internal/$pkg"

  # The test compares its own mount namespace with the one we are leaving.
  SMOKE_PARENT_MNT_NS="$(readlink /proc/self/ns/mnt)" \
  unshare --mount --propagation private \
    "$dir/$pkg.test" -test.run '^TestSmoke' -test.v -test.count=1
done
