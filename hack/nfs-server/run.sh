#!/usr/bin/env bash
# Starts the kernel NFSv3 server of the e2e tests in a privileged container on
# the local docker network and prints its IP address on stdout (and nothing
# else there). The export is a docker volume (named <container>-export) mounted
# at NFS_EXPORT (default /export), so its files survive a restart of the
# container: `docker restart` of the server keeps its data and its address, and
# the kernel client's file handles stay valid. Run as root: it loads kernel
# modules. hack/nfs-server/stop.sh removes the container and the volume.
#
# NFS_SERVER_NETWORK names the docker network to start the container on (the
# kind end-to-end tests pass "kind", so the cluster's nodes can reach it);
# unset, it is the default bridge and the address printed is that network's.
#
# The host kernel must have the nfsd module available. If it does not, this
# fails with a message that says so; it never falls back to a user-space
# server, because the point is a real kernel server.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

name="${NFS_SERVER_NAME:-miami-e2e-nfs}"
image="miami-e2e-nfs:local"
wait_seconds="${NFS_SERVER_WAIT:-60}"

log() { echo "nfs-server: $*" >&2; }

if [ "$(id -u)" -ne 0 ]; then
  log "must run as root (it loads kernel modules); try: sudo -E $0"
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  log "cannot reach the docker daemon"
  exit 1
fi

# The server needs nfsd; the mounts under test need the NFS client. Loading a
# module that is built in or already loaded is not an error; what matters is
# that the file system type is registered afterwards.
log "kernel $(uname -r)"
for fstype in nfsd nfs; do
  if grep -qw "$fstype" /proc/filesystems; then
    log "$fstype is already available"
  else
    log "loading $fstype"
    modprobe "$fstype" >&2 || true
  fi
  if ! grep -qw "$fstype" /proc/filesystems; then
    log "the kernel ($(uname -r)) has no '$fstype' file system and 'modprobe $fstype' did not provide one"
    log "this harness needs the kernel NFS server (nfsd) and client (nfs); it will not substitute a user-space server"
    exit 1
  fi
  log "$fstype is available"
done

volume="${name}-export"
docker rm -f "$name" >/dev/null 2>&1 || true
docker volume rm -f "$volume" >/dev/null 2>&1 || true
docker volume create "$volume" >/dev/null
docker build --quiet -t "$image" . >/dev/null

network_args=()
if [ -n "${NFS_SERVER_NETWORK:-}" ]; then
  network_args=(--network "$NFS_SERVER_NETWORK")
fi
docker run -d --name "$name" --privileged "${network_args[@]}" \
  -e "EXPORT_DIR=${NFS_EXPORT:-/export}" \
  -v "$volume:${NFS_EXPORT:-/export}" \
  "$image" >/dev/null

for _ in $(seq "$wait_seconds"); do
  if docker logs "$name" 2>&1 | grep -q '^nfs-server: ready$'; then
    ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$name")"
    if [ -z "$ip" ]; then
      log "the container has no IP address"
      exit 1
    fi
    log "ready at $ip"
    echo "$ip"
    exit 0
  fi
  if [ "$(docker inspect -f '{{.State.Running}}' "$name")" != "true" ]; then
    log "the server container exited; its output follows"
    docker logs "$name" >&2 || true
    exit 1
  fi
  sleep 1
done

log "the server was not ready after ${wait_seconds}s; its output follows"
docker logs "$name" >&2 || true
exit 1
