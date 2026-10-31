#!/usr/bin/env bash
# Prints the content hash of the image's build inputs: 16 hex characters.
#
# Images are identified by this hash, not by commit, so a commit that leaves
# the inputs unchanged (docs, workflow edits, a rebase) has the same image.
# It reads only the index (mode, blob hash and path of each tracked file), so
# it is identical on every runner and every architecture.
#
# The paths below are exactly the allowlist in .dockerignore, plus the
# Dockerfile and .dockerignore themselves. Keep the two lists in agreement.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

entries="$(git ls-files -s -- Dockerfile .dockerignore go.mod go.sum cmd internal)"
if [ -z "$entries" ]; then
  echo "content-hash: no tracked build inputs found" >&2
  exit 1
fi

printf '%s\n' "$entries" | sha256sum | cut -c1-16
