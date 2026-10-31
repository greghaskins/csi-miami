#!/usr/bin/env bash
# Runs the csi-sanity suite against the identity and controller services, and
# the one delete-path guard that needs a second file system.
# Root only: the controller bind-mounts the local transport the suite uses. The
# test binary is built once, then executed under
# `unshare --mount --propagation private`, so nothing it mounts is visible
# outside, or survives it. The test itself fails (never skips) if csi-sanity is
# not on PATH or it is not root.
#
# csi-sanity is the binary pinned in mise.toml; run as
# `sudo -E env "PATH=$PATH" make sanity` so that root sees the pinned Go and
# csi-sanity. Root builds the test binary too, so Go's build and module caches
# get root-owned files; to keep them out of your own caches, point GOCACHE and
# GOMODCACHE at scratch directories for the run.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# The Makefile sets these; running this script directly must not differ.
export GOTOOLCHAIN=local
export CGO_ENABLED=0

if [ "$(id -u)" -ne 0 ]; then
  echo "sanity: must run as root (it mounts a transport); try: sudo -E env \"PATH=\$PATH\" make sanity" >&2
  exit 1
fi
for tool in unshare csi-sanity; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "sanity: $tool not found on PATH" >&2
    exit 1
  fi
done

dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT

go test -c -tags sanity -o "$dir/csi.test" ./internal/csi

# A renamed test or a changed build tag would make the run below print "no
# tests to run" and exit 0, so the test must be listed first.
if ! "$dir/csi.test" -test.list '^TestSanity' | grep -q '^TestSanity'; then
  echo "sanity: internal/csi has no TestSanity in the sanity build" >&2
  exit 1
fi

unshare --mount --propagation private \
  "$dir/csi.test" -test.run '^TestSanity' -test.v -test.count=1
