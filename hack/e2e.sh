#!/usr/bin/env bash
# Runs the end-to-end tests of one transport against a real server. Root only:
# it loads kernel modules and mounts. The transport is chosen by E2E_TRANSPORT:
#
#   nfs3   starts the kernel NFSv3 server of hack/nfs-server in a privileged
#          container, runs internal/transport's e2e test as root under
#          `unshare --mount --propagation private` against it, and removes the
#          server.
#
# If E2E_ARTIFACT_DIR is set, a failing run leaves mountinfo-<test>.txt (the
# lines of /proc/self/mountinfo for the test's own temporary paths) and
# nfs-server.log (the server container's output) there.
#
# Run as `sudo -E make e2e E2E_TRANSPORT=nfs3`, with the pinned Go on PATH.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

export GOTOOLCHAIN=local
export CGO_ENABLED=0

transport="${E2E_TRANSPORT:-}"
case "$transport" in
  nfs3) ;;
  local)
    echo "e2e: the local transport has no e2e variant yet (it arrives with the kubelet-driven tests); its smoke test, 'make smoke', covers it" >&2
    exit 2
    ;;
  "")
    echo "e2e: set E2E_TRANSPORT, for example: sudo -E make e2e E2E_TRANSPORT=nfs3" >&2
    exit 2
    ;;
  *)
    echo "e2e: unknown E2E_TRANSPORT '$transport' (nfs3)" >&2
    exit 2
    ;;
esac

if [ "$(id -u)" -ne 0 ]; then
  echo "e2e: must run as root (it loads modules and mounts); try: sudo -E make e2e E2E_TRANSPORT=$transport" >&2
  exit 1
fi
for tool in unshare docker; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "e2e: $tool not found" >&2
    exit 1
  fi
done

if [ -n "${E2E_ARTIFACT_DIR:-}" ]; then
  mkdir -p "$E2E_ARTIFACT_DIR"
fi

export NFS_SERVER_NAME="${NFS_SERVER_NAME:-miami-e2e-nfs}"
dir="$(mktemp -d)"
cleanup() {
  status=$?
  if [ "$status" -ne 0 ] && [ -n "${E2E_ARTIFACT_DIR:-}" ]; then
    docker logs "$NFS_SERVER_NAME" >"$E2E_ARTIFACT_DIR/nfs-server.log" 2>&1 || true
  fi
  hack/nfs-server/stop.sh || true
  rm -rf "$dir"
  exit "$status"
}
trap cleanup EXIT

server="$(hack/nfs-server/run.sh)"
echo "e2e: NFS server at $server"

go test -c -tags e2e -o "$dir/transport.test" ./internal/transport

# A renamed test or a changed build tag would make the run below print "no
# tests to run" and exit 0, so the test must be listed first.
if ! grep -qx 'TestE2ENFS3' <<<"$("$dir/transport.test" -test.list '^TestE2E')"; then
  echo "e2e: the e2e build of internal/transport does not list TestE2ENFS3" >&2
  exit 1
fi

# The test compares its own mount namespace with the one we are leaving.
MIAMI_E2E_NFS_SERVER="$server" \
MIAMI_E2E_NFS_EXPORT="${NFS_EXPORT:-/export}" \
SMOKE_ARTIFACT_DIR="${E2E_ARTIFACT_DIR:-}" \
SMOKE_PARENT_MNT_NS="$(readlink /proc/self/ns/mnt)" \
unshare --mount --propagation private \
  "$dir/transport.test" -test.run '^TestE2ENFS3$' -test.v -test.count=1
