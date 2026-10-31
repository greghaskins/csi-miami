#!/bin/sh
# Starts a kernel NFSv3 server exporting a tmpfs. Runs as the container's
# process 1 and needs --privileged: it mounts filesystems and starts nfsd
# threads in the host kernel. Prints "nfs-server: ready" when the export is
# live, and stops the nfsd threads on SIGTERM, because threads left running
# keep the container's network namespace alive.
set -eu

export_dir="${EXPORT_DIR:-/export}"
threads="${NFSD_THREADS:-8}"

log() { echo "nfs-server: $*"; }

stop() {
  log "stopping"
  rpc.nfsd 0 || true
  exportfs -ua || true
  exit 0
}
trap stop TERM INT

mkdir -p "$export_dir"
mount -t tmpfs -o size=256m tmpfs "$export_dir"
chmod 0777 "$export_dir"

# The nfsd control file system; the image may be run with it already mounted.
if ! grep -qw nfsd /proc/mounts; then
  mount -t nfsd nfsd /proc/fs/nfsd
fi

# fsid=0 is needed for a tmpfs (it has no UUID); insecure admits the client's
# unprivileged source ports; no_root_squash lets the tests write as root.
echo "$export_dir *(fsid=0,rw,no_root_squash,insecure,no_subtree_check,async)" > /etc/exports

log "starting rpcbind"
rpcbind
log "exporting $export_dir"
exportfs -ra
log "starting mountd"
rpc.mountd -p 20048 --no-nfs-version 2 --no-nfs-version 4
log "starting nfsd"
rpc.nfsd --no-nfs-version 2 --no-nfs-version 4 "$threads"

exportfs -v
rpcinfo -p 127.0.0.1
log "ready"

# Wait for a signal; the trap above does the stopping.
while :; do
  sleep 3600 &
  wait $! || true
done
