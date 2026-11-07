#!/usr/bin/env bash
# Runs the smoke tests against the real kernel. Root only: they mount
# filesystems. The test binary is built once, then executed under
# `unshare --mount --propagation private`, so nothing it mounts is visible
# outside, or survives it. The test itself fails (never skips) if it is not
# root or the namespace is not private.
#
# The smoke tests live in internal/mount, which they exercise together with
# internal/observe, in internal/transport, which they exercise on top of both,
# in internal/reconcile, which runs the node reconciler on a real local
# transport, in internal/nbd, which serves a device through the kernel's nbd
# module (loaded below) and drives it with dd and blkdiscard, in
# internal/blockd, which runs the block server as a child process over a real
# nbd device, mkfs/mounts ext4 on it and kills and restarts it to prove
# crash-resume, in
# internal/systemd, which starts transient units on the host's systemd, and in
# internal/crypt and internal/ext4, which run the LUKS2 and ext4 tools on loop
# devices, and in cmd/miami, which runs the node command and drives it over
# its CSI socket.
# internal/prereq binds Adiantum through AF_ALG on the real kernel.
# internal/item/volume runs the volume item over all of it: the runner's systemd
# executes a freshly built driver binary as the block server, and the item
# stages, writes, publishes a generation, unstages and stages again.
# Each package is built as its own test binary and run in its own namespace.
#
# A last step, `tools`, builds the image that would ship (hack/image-build.sh
# --load, for this machine's architecture) and starts each tool of
# hack/tools.txt from it, with the image's own entrypoint replaced. It needs docker; without it the step is skipped with a notice,
# unless SMOKE_REQUIRE_TOOLS=1 (CI sets it), when a missing docker fails.
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

# The nbd smoke tests create devices through the module's netlink family.
if ! modprobe nbd; then
  echo "smoke: cannot load the nbd module" >&2
  exit 1
fi

# The LUKS2 smoke test needs loop devices and dm-crypt; either may be built
# in, so a failed modprobe is not fatal here (the test fails if they are
# missing).
modprobe loop || true
modprobe dm_crypt || true
modprobe adiantum || true

if [ -n "${SMOKE_ARTIFACT_DIR:-}" ]; then
  mkdir -p "$SMOKE_ARTIFACT_DIR"
fi

dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT

for path in internal/item/volume internal/prereq internal/mount internal/transport internal/reconcile internal/nbd internal/blockd internal/systemd internal/crypt internal/ext4 cmd/miami; do
  pkg="$(basename "$path")"
  go test -c -tags smoke -o "$dir/$pkg.test" "./$path"

  # A renamed test or a changed build tag would make the run below print "no
  # tests to run" and exit 0, so the tests must be listed first.
  listed="$("$dir/$pkg.test" -test.list '^TestSmoke')"
  if ! grep -q '^TestSmoke' <<<"$listed"; then
    echo "smoke: $path has no TestSmoke* test in the smoke build" >&2
    exit 1
  fi

  # The test compares its own mount namespace with the one we are leaving.
  SMOKE_PARENT_MNT_NS="$(readlink /proc/self/ns/mnt)" \
  unshare --mount --propagation private \
    "$dir/$pkg.test" -test.run '^TestSmoke' -test.v -test.count=1
done

# The tools step: the image's cryptsetup and e2fsprogs start and report their
# versions. The image has no shell, so each is the entrypoint of its own run.
if ! command -v docker >/dev/null 2>&1; then
  if [ "${SMOKE_REQUIRE_TOOLS:-}" = "1" ]; then
    echo "smoke: tools step needs docker" >&2
    exit 1
  fi
  echo "smoke: docker not found; skipping the tools step" >&2
  exit 0
fi
case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) echo "smoke: unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac
tag="csi-miami-smoke:tools"
hack/image-build.sh --arch "$arch" --tag "$tag" --load
# A build from a tree that differs from the index is tagged -dirty.
if ! docker image inspect "$tag" >/dev/null 2>&1; then
  tag="${tag}-dirty"
fi
# Every program of hack/tools.txt (the list the image is built from) is started
# from the image with the arguments the file gives. A program that cannot be
# executed (the runtime reports 125 to 127: not in the image, a library
# missing) or that prints nothing fails the step; its own exit status does not
# matter, because resize2fs has no version flag and prints its banner and then
# a usage error.
while read -r prog args; do
  case "$prog" in "" | "#"*) continue ;; esac
  set +e
  # shellcheck disable=SC2086 # the probe arguments are split on purpose
  out="$(docker run --rm --entrypoint "$prog" "$tag" $args 2>&1)"
  code=$?
  set -e
  echo "smoke: $prog: $(printf '%s' "$out" | head -n1)"
  if [ "$code" -ge 125 ] && [ "$code" -le 127 ]; then
    echo "smoke: $prog cannot run from the image (exit $code): $out" >&2
    exit 1
  fi
  if [ -z "$out" ]; then
    echo "smoke: $prog printed nothing" >&2
    exit 1
  fi
done < hack/tools.txt
