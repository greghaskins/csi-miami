#!/usr/bin/env bash
# Installs kind and kubectl at the versions mise.toml pins into <bin-dir>, for
# CI jobs that run a kind cluster (the deploy check and the kind end-to-end
# tests): kind is built with the pinned Go, kubectl is downloaded from the
# Kubernetes release for this architecture and checked against its published
# checksum.
#
#   hack/install-kind-tools.sh <bin-dir>
#
# The caller puts <bin-dir> on PATH.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
export GOTOOLCHAIN=local

bin="${1:?usage: hack/install-kind-tools.sh <bin-dir>}"
mkdir -p "$bin"
bin="$(cd "$bin" && pwd)"

kind="$(sed -n 's/^kind = "\(.*\)"$/\1/p' mise.toml)"
kubectl="$(sed -n 's/^kubectl = "\(.*\)"$/\1/p' mise.toml)"
if [ -z "$kind" ] || [ -z "$kubectl" ]; then
  echo "install-kind-tools: mise.toml does not pin kind and kubectl" >&2
  exit 1
fi

case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo "install-kind-tools: unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

GOBIN="$bin" go install "sigs.k8s.io/kind@v${kind}"
url="https://dl.k8s.io/release/v${kubectl}/bin/linux/${arch}/kubectl"
curl -fsSL --retry 0 -o "${bin}/kubectl" "$url"
printf '%s  %s\n' "$(curl -fsSL "${url}.sha256")" "${bin}/kubectl" | sha256sum --check -
chmod +x "${bin}/kubectl"
