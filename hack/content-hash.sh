#!/usr/bin/env bash
# Prints the content hash of the image's build inputs: 16 hex characters.
#
# Images are identified by this hash, not by commit, so a commit that leaves
# the inputs unchanged (docs, workflow edits, a rebase) has the same image.
# It reads only the index (mode, blob hash and path of each tracked file), so
# it is identical on every runner and every architecture.
#
# The inputs are the Dockerfile, the build context allowlisted in
# .dockerignore (go.mod, go.sum, cmd, internal) and the two scripts that
# decide how the image is built and hashed. Keep this list in agreement with
# .dockerignore. `--paths` prints the list, one path per line, for
# hack/image-build.sh.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

paths=(
  Dockerfile
  .dockerignore
  go.mod
  go.sum
  cmd
  internal
  hack/content-hash.sh
  hack/image-build.sh
)

if [ "${1:-}" = "--paths" ]; then
  printf '%s\n' "${paths[@]}"
  exit 0
fi

entries="$(git ls-files -s -- "${paths[@]}")"
if [ -z "$entries" ]; then
  echo "content-hash: no tracked build inputs found" >&2
  exit 1
fi

printf '%s\n' "$entries" | sha256sum | cut -c1-16
