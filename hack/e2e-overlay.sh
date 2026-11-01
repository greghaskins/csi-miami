#!/usr/bin/env bash
# Writes a throwaway kustomization that applies deploy/kustomize/overlays/e2e
# with the two values that vary per run, so the checked-in overlay never
# changes:
#
#   hack/e2e-overlay.sh --image <ref> [--nfs-server <ipv4>] --out <dir>
#   kubectl apply -k <dir>
#
#   --image       the driver image: name:tag or name@sha256:<digest>
#   --nfs-server  an IPv4 address (decision 21); replaces transport.server of
#                 StorageClass miami-nfs3. Omitted: the placeholder stays.
#   --out         an existing, empty directory to write kustomization.yaml in
#
# The deploy check (hack/kind.sh) and the end-to-end tests both use it.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
driver="ghcr.io/greghaskins/csi-miami"

image=""
server=""
out=""
while [ $# -gt 0 ]; do
  case "$1" in
    --image) image="${2:?--image needs a value}"; shift 2 ;;
    --nfs-server) server="${2:?--nfs-server needs a value}"; shift 2 ;;
    --out) out="${2:?--out needs a value}"; shift 2 ;;
    *) echo "e2e-overlay: unknown argument: $1" >&2; exit 2 ;;
  esac
done
if [ -z "$image" ] || [ -z "$out" ]; then
  echo "usage: hack/e2e-overlay.sh --image <ref> [--nfs-server <ipv4>] --out <dir>" >&2
  exit 2
fi
if [ ! -d "$out" ]; then
  echo "e2e-overlay: $out is not a directory" >&2
  exit 2
fi

# kustomize refuses an absolute path as a resource, so the base is named
# relative to the output directory.
base="$(realpath --relative-to "$out" "${root}/deploy/kustomize/overlays/e2e")"

if [[ "$image" =~ ^([^@]+)@(sha256:[0-9a-f]{64})$ ]]; then
  name="${BASH_REMATCH[1]}"
  pin="digest: ${BASH_REMATCH[2]}"
elif [[ "$image" =~ ^([^@]+/)?([^/@:]+):([^/@:]+)$ ]]; then
  name="${BASH_REMATCH[1]}${BASH_REMATCH[2]}"
  pin="newTag: ${BASH_REMATCH[3]}"
else
  echo "e2e-overlay: image must be name:tag or name@sha256:<digest>: $image" >&2
  exit 2
fi

{
  echo "apiVersion: kustomize.config.k8s.io/v1beta1"
  echo "kind: Kustomization"
  echo "resources:"
  echo "  - ${base}"
  echo "images:"
  echo "  - name: ${driver}"
  echo "    newName: ${name}"
  echo "    ${pin}"
  if [ -n "$server" ]; then
    octet='(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])'
    if ! [[ "$server" =~ ^${octet}\.${octet}\.${octet}\.${octet}$ ]]; then
      echo "e2e-overlay: --nfs-server must be an IPv4 address: $server" >&2
      exit 2
    fi
    echo "patches:"
    echo "  - target:"
    echo "      kind: StorageClass"
    echo "      name: miami-nfs3"
    echo "    patch: |-"
    echo "      - op: replace"
    echo "        path: /parameters/transport.server"
    echo "        value: \"${server}\""
  fi
} > "$out/kustomization.yaml"
