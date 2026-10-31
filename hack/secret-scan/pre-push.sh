#!/usr/bin/env bash
# Before anything leaves this machine: the content and message of every
# commit being pushed, and the name of the ref it goes to. On a public
# repository a push is publication; nothing after this point is private.
set -euo pipefail
fail=0
while read -r _local_ref local_sha remote_ref remote_sha; do
  [[ "$local_sha" =~ ^0+$ ]] && continue   # deleting a ref
  printf '%s' "${remote_ref#refs/heads/}" | hack/secret-scan/text.sh "ref name $remote_ref" || fail=1
  hack/secret-scan/range.sh "$remote_sha" "$local_sha" || fail=1
done
exit $fail
