#!/usr/bin/env bash
# CI only: install a pinned gitleaks release with checksum verification and
# put it on PATH. Locally gitleaks comes from mise (mise.toml).
set -euo pipefail
version=8.30.1
case "$(uname -m)" in
  x86_64)  arch=x64;   sha=551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb ;;
  aarch64) arch=arm64; sha=e4a487ee7ccd7d3a7f7ec08657610aa3606637dab924210b3aee62570fb4b080 ;;
  *) echo "unsupported arch" >&2; exit 1 ;;
esac
dir=${RUNNER_TEMP:-/tmp}/gitleaks
mkdir -p "$dir"
curl -sSL -o "$dir/gitleaks.tgz" \
  "https://github.com/gitleaks/gitleaks/releases/download/v${version}/gitleaks_${version}_linux_${arch}.tar.gz"
echo "$sha  $dir/gitleaks.tgz" | sha256sum -c - >/dev/null
tar -xzf "$dir/gitleaks.tgz" -C "$dir" gitleaks
echo "$dir" >>"${GITHUB_PATH:-/dev/null}"
"$dir/gitleaks" version
