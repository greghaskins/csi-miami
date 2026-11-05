#!/usr/bin/env bash
# Runs `miami bench` for the nbd and the loop stack on this host and prints
# both tables and the per-phase ratios (gate G1). Root only: both stacks mount
# file systems. The run happens under `unshare --mount --propagation private`,
# so nothing it mounts is visible outside or outlives it.
#
#   hack/bench.sh [--scale small|full] [--size 8Gi] [--dir DIR] [--store DIR] [--no-threshold]
#
# --dir and --store must be empty directories (or ones a previous run left
# behind); by default both are fresh directories under $TMPDIR. The exit status
# is the gate's verdict (1 on FAIL). --no-threshold, which CI uses with the
# small scale, makes a FAIL verdict exit 0; a run that breaks still exits
# non-zero.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

export GOTOOLCHAIN=local
export CGO_ENABLED=0

scale=full
size=8Gi
dir=""
store=""
nogate=0
while [ $# -gt 0 ]; do
  case "$1" in
    --scale) scale="$2"; shift 2 ;;
    --size) size="$2"; shift 2 ;;
    --dir) dir="$2"; shift 2 ;;
    --store) store="$2"; shift 2 ;;
    --no-threshold) nogate=1; shift ;;
    *) echo "bench: unknown argument $1" >&2; exit 2 ;;
  esac
done

if [ "$(id -u)" -ne 0 ]; then
  echo "bench: must run as root (it mounts filesystems); try: sudo -E hack/bench.sh" >&2
  exit 1
fi
command -v unshare >/dev/null 2>&1 || { echo "bench: unshare not found" >&2; exit 1; }
modprobe nbd || { echo "bench: cannot load the nbd module" >&2; exit 1; }
modprobe loop || true
modprobe dm_crypt || true
modprobe adiantum || true

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
dir="${dir:-$work/dir}"
store="${store:-$work/store}"

go build -o "$work/miami" ./cmd/miami

for stack in nbd loop; do
  echo "== $stack"
  # The table goes to the log and the same run's report to a file for the
  # comparison, so the numbers shown are the numbers compared.
  unshare --mount --propagation private \
    "$work/miami" bench --stack "$stack" --dir "$dir/$stack" --store "$store" \
      --size "$size" --scale "$scale" --out "$work/$stack.json"
  echo
done

echo "== nbd against loop"
rc=0
"$work/miami" bench --compare "$work/nbd.json" "$work/loop.json" || rc=$?
if [ "$rc" -eq 1 ] && [ "$nogate" -eq 1 ]; then
  echo "bench: over the limit, not enforced with --no-threshold"
  exit 0
fi
exit "$rc"
