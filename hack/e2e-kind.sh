#!/usr/bin/env bash
# The kind end-to-end tests: exit criteria 1 to 6 of the M1 plan, as the tests
# of test/e2e, against the driver on a kind cluster of one control plane and
# two workers. hack/e2e.sh hands over to this script for E2E_MODE=kind.
#
#   E2E_TRANSPORT=local|nfs3 E2E_IMAGE=<ref> hack/e2e-kind.sh
#
# E2E_IMAGE is a driver image already in the local Docker daemon (`make image`
# builds one, CI pulls the content-addressed per-arch tag); it is loaded into
# the nodes and never pulled by them. The cluster is made, the driver applied
# and waited for with the functions of hack/lib/kind.sh. For nfs3 the kernel
# NFS server of hack/nfs-server is started on the docker network the nodes are
# on and its IPv4 address goes into the overlay; that needs root. The test
# binary is built, listed (a renamed test or tag must not pass by running
# nothing), run, and its output checked for a pass of every variant this
# transport must run. The cluster and the server are removed at the end.
#
# The nfs3 run also runs the tests of the volume kind (exit criteria 1 to 9 of
# the M2 plan). A volume is ext4 over dm-crypt over an nbd device served by a
# block server that runs as a systemd unit, and the kind nodes are containers
# that run systemd and share the runner's kernel, so the runner needs the nbd
# and dm-crypt modules. They are loaded before the cluster is made, with the
# nbd devices created up front (a privileged container gets the device nodes
# that exist when it starts, and nothing makes a node for a device made later).
# The tests also use the runner's own tools: dmesg (the hung-task check),
# cryptsetup, losetup, e2fsck and mount (the restore drill), and the driver's
# binary, which this script builds, for `miami restore`.
#
# On failure E2E_ARTIFACT_DIR (if set) receives what the tests wrote (the
# driver pods' logs, YAML, events and mountinfo of the failed test), the test
# output, kind's logs, both pods' logs and each node's mountinfo, and the NFS
# server's output. KEEP_CLUSTER=1 keeps the cluster.
#
# Needs docker, kind, kubectl and the pinned Go on PATH.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=hack/lib/kind.sh
source hack/lib/kind.sh

export GOTOOLCHAIN=local
export CGO_ENABLED=0

transport="${E2E_TRANSPORT:-}"
image="${E2E_IMAGE:-}"
case "$transport" in
  local|nfs3) ;;
  *)
    echo "e2e: set E2E_TRANSPORT to local or nfs3 (got '${transport}')" >&2
    exit 2
    ;;
esac
if [ -z "$image" ]; then
  echo "e2e: set E2E_IMAGE to a driver image in the local Docker daemon" >&2
  exit 2
fi
if [ "$transport" = nfs3 ] && [ "$(id -u)" -ne 0 ]; then
  echo "e2e: the nfs3 transport must run as root (it loads kernel modules); try: sudo -E env \"PATH=\$PATH\" make e2e E2E_MODE=kind E2E_TRANSPORT=nfs3 E2E_IMAGE=..." >&2
  exit 1
fi
kind_require_tools || exit 1
if ! command -v go >/dev/null 2>&1; then
  echo "e2e: go not found" >&2
  exit 1
fi

cluster="${KIND_CLUSTER_NAME:-miami-e2e}"
kind_init "$cluster" "${E2E_TIMEOUT:-300s}"
export NFS_SERVER_NAME="${NFS_SERVER_NAME:-miami-e2e-nfs}"
artifacts="${E2E_ARTIFACT_DIR:-$kind_work/artifacts}"
mkdir -p "$artifacts"
log="$kind_work/e2e-test.log"
nfs_started=0

cleanup() {
  status=$?
  trap - EXIT
  if [ "$status" -ne 0 ] && [ -n "${E2E_ARTIFACT_DIR:-}" ]; then
    echo "e2e: failed; collecting artifacts into $E2E_ARTIFACT_DIR" >&2
    kind_collect "$E2E_ARTIFACT_DIR" || true
    cp "$log" "$E2E_ARTIFACT_DIR/e2e-test.log" 2>/dev/null || true
    if [ "$nfs_started" = 1 ]; then
      docker logs "$NFS_SERVER_NAME" >"$E2E_ARTIFACT_DIR/nfs-server.log" 2>&1 || true
    fi
    # Root wrote these; the artifact upload runs as the runner's user.
    chmod -R a+rX "$E2E_ARTIFACT_DIR" 2>/dev/null || true
  fi
  if [ "$nfs_started" = 1 ]; then
    hack/nfs-server/stop.sh || true
  fi
  kind_delete
  exit "$status"
}
trap cleanup EXIT

if [ "$transport" = nfs3 ]; then
  # nbds_max devices exist from the start; the module may already be loaded
  # with none (the first `modprobe` wins), in which case it is reloaded.
  modprobe nbd nbds_max=16 max_part=0
  if [ ! -e /dev/nbd0 ]; then
    rmmod nbd
    modprobe nbd nbds_max=16 max_part=0
  fi
  modprobe dm_crypt
  # The ciphers of the default class are loaded by the kernel on demand; load
  # them here so that a missing one fails the run before the cluster exists.
  modprobe adiantum || true
  for dev in /dev/nbd0 /dev/mapper/control; do
    if [ ! -e "$dev" ]; then
      echo "e2e: $dev does not exist on the runner; the volume tests need the nbd and dm-crypt modules" >&2
      exit 1
    fi
  done
  for tool in cryptsetup losetup e2fsck mount dmesg; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      echo "e2e: $tool not found on the runner (the volume tests use it)" >&2
      exit 1
    fi
  done
  # The hung-task detector is what the NAS outage test reads: it must be on,
  # report every stuck task, and check often enough to see one within the
  # outage. The threshold stays at the kernel's usual 120 seconds.
  if ! sysctl -w kernel.hung_task_timeout_secs=120 kernel.hung_task_check_interval_secs=10 kernel.hung_task_warnings=-1; then
    echo "e2e: the runner's kernel has no hung-task detector; the NAS outage test would be blind" >&2
    exit 1
  fi
  go build -o "$kind_work/miami" ./cmd/miami
fi

kind_create

server=""
if [ "$transport" = nfs3 ]; then
  # The cluster's nodes are on the network kind made; a server on the default
  # bridge would not be reachable from them.
  nfs_started=1
  server="$(NFS_SERVER_NETWORK=kind hack/nfs-server/run.sh)"
  echo "e2e: NFS server at $server"
fi
# The local transport's path (the example class uses /export/data) is a
# directory on each node that the overlay mounts into the node plugin.
for node in $(kind get nodes --name "$cluster"); do
  docker exec "$node" mkdir -p /export/data
done
kind_apply "$image" "$server"
kind_wait

go test -c -tags e2e -o "$kind_work/e2e.test" ./test/e2e

# The tests every transport runs a variant of, and the ones only nfs3 runs.
# A listing that lacks one means a rename or a build tag would pass by running
# nothing.
volume_tests=()
if [ "$transport" = nfs3 ]; then
  expected=(
    TestTwoPodsOnTwoNodesSeeEachOthersWrites
    TestDeleteRemovesTheDirectory
    TestNodePluginRestartChangesNothing
    TestNodeReboot
    TestMetrics
    TestNFSServerRestart
    TestLockSerializesPodsAndBreaksAStaleLock
  )
  # The volume kind's tests have no variant subtests: they run for nfs3 only
  # and skip for local, so a pass of the test itself is the check.
  volume_tests=(
    TestVolumeExt4Semantics
    TestVolumeSurvivesReschedule
    TestVolumePluginRestart
    TestVolumeServerKill
    TestVolumeNASOutageNoHungTask
    TestVolumeTakeover
    TestVolumeOrphanDrain
    TestVolumeNodeRebootReusesCache
    TestVolumeRestoreDrill
    TestVolumeExpandOnline
    TestCacheCapacityAdvertised
    TestCacheVAPTypeChecks
    TestCacheFenced
    TestCacheCommittedTracksStages
    TestCacheWebhookUsesPodNamespace
    TestCacheWebhookRequestsFootprint
    TestCacheSchedulerHoldsTheLine
    TestCacheStageCheckRefusesAndRecovers
    TestCacheStageCheckNeverRefusesExisting
    TestCacheWebhookFailsOpen
    TestCachePolicyNegative
    TestCacheRepatchAfterRemoval
    TestCacheCertManagerComponent
  )
else
  expected=(
    TestLocalVolumeOnOneNode
    TestDeleteRemovesTheDirectory
    TestNodePluginRestartChangesNothing
    TestNodeReboot
    TestMetrics
    TestLockSerializesPodsAndBreaksAStaleLock
  )
fi
listed="$("$kind_work/e2e.test" -test.list '^Test')"
for name in "${expected[@]}" "${volume_tests[@]}"; do
  if ! grep -qx "$name" <<<"$listed"; then
    echo "e2e: the e2e build of test/e2e does not list $name" >&2
    exit 1
  fi
done

export MIAMI_E2E_NAMESPACE="$kind_ns"
export MIAMI_E2E_TIMEOUT="${MIAMI_E2E_TIMEOUT:-150s}"
export MIAMI_E2E_TRANSPORT="$transport"
export MIAMI_E2E_KIND_CLUSTER="$cluster"
export MIAMI_E2E_ARTIFACT_DIR="$artifacts"
export MIAMI_E2E_REPO="$PWD"
export MIAMI_E2E_MIAMI="$kind_work/miami"
if [ "$transport" = nfs3 ]; then
  export MIAMI_E2E_STORAGE_CLASS="miami-remote-dir-rwx"
  export MIAMI_E2E_NFS_SERVER="$server"
  export MIAMI_E2E_NFS_CONTAINER="$NFS_SERVER_NAME"
else
  export MIAMI_E2E_STORAGE_CLASS="miami-local-dir-rwo"
fi

run_args=(-test.v -test.count=1 -test.timeout=100m)
if [ "${E2E_FAILFAST:-1}" != 0 ]; then
  run_args+=(-test.failfast)
fi
if [ -n "${E2E_RUN:-}" ]; then
  run_args+=("-test.run=${E2E_RUN}")
fi
started=$SECONDS
"$kind_work/e2e.test" "${run_args[@]}" 2>&1 | tee "$log"
echo "e2e: the tests took $((SECONDS - started)) seconds"
if [ -n "${E2E_RUN:-}" ]; then
  echo "e2e: E2E_RUN is set; the pass of every expected test was not checked" >&2
  exit 0
fi

# Every test above ran the variant for this transport and passed; a test that
# found nothing to run for it would also exit 0.
for name in "${expected[@]}"; do
  if ! grep -Eq "^--- PASS: ${name}( |$)" "$log" || ! grep -Eq "^    --- PASS: ${name}/${transport} \(" "$log"; then
    echo "e2e: ${name} did not report a pass for the ${transport} variant" >&2
    exit 1
  fi
done
for name in "${volume_tests[@]}"; do
  if ! grep -Eq "^--- PASS: ${name} " "$log"; then
    echo "e2e: ${name} did not pass" >&2
    exit 1
  fi
done
# Both modes of the takeover test, not only the parent.
for sub in out-of-service-taint force-detach-annotation; do
  if [ "${#volume_tests[@]}" -gt 0 ] && ! grep -Eq "^    --- PASS: TestVolumeTakeover/${sub} " "$log"; then
    echo "e2e: TestVolumeTakeover/${sub} did not pass" >&2
    exit 1
  fi
done
echo "e2e: all $((${#expected[@]} + ${#volume_tests[@]})) criteria passed for ${transport}"
