#!/usr/bin/env bash
# Tells whether an image reference exists in its registry.
#
#   hack/image-exists.sh <ref>
#
# Exit 0: it exists. Exit 1: the registry says it is absent (not found,
# manifest unknown, name unknown). Exit 2: anything else, such as a network
# or authorization failure, which must never be read as "absent".
set -uo pipefail

ref="${1:?usage: hack/image-exists.sh <ref>}"

if err="$(docker buildx imagetools inspect "$ref" 2>&1 > /dev/null)"; then
  exit 0
fi
if printf '%s' "$err" | grep -Eqi 'not found|manifest unknown|MANIFEST_UNKNOWN|NAME_UNKNOWN'; then
  exit 1
fi
echo "image-exists: cannot tell whether ${ref} exists: ${err}" >&2
exit 2
