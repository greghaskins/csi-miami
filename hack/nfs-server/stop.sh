#!/usr/bin/env bash
# Removes the NFS server container started by run.sh, and its export volume.
# Safe to run when there is none. It stops the nfsd threads before removing the
# container: threads left in the kernel keep the container's network namespace
# alive.
set -euo pipefail

name="${NFS_SERVER_NAME:-miami-e2e-nfs}"

if docker inspect "$name" >/dev/null 2>&1; then
  docker exec "$name" sh -c 'rpc.nfsd 0; exportfs -ua' >/dev/null 2>&1 || true
  docker stop --time 10 "$name" >/dev/null 2>&1 || true
  docker rm -f "$name" >/dev/null
fi
docker volume rm -f "${name}-export" >/dev/null 2>&1 || true
