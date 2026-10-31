#!/usr/bin/env bash
# Before anything leaves this machine: the content and message of every
# commit being pushed, and the name of the ref it goes to. On a public
# repository a push is publication; nothing after this point is private.
#
# Under pre-commit the range arrives in PRE_COMMIT_FROM_REF, PRE_COMMIT_TO_REF
# and PRE_COMMIT_REMOTE_BRANCH (pre-commit consumes git's stdin). As a plain
# git pre-push hook it arrives on stdin. With neither, refuse: a hook that
# scans nothing must not pass.
set -euo pipefail
fail=0
scan() {
  local from=$1 to=$2 ref=$3
  printf '%s' "${ref#refs/heads/}" | hack/secret-scan/text.sh "the ref name" || fail=1
  hack/secret-scan/range.sh "$from" "$to" || fail=1
}
if [[ -n "${PRE_COMMIT_TO_REF:-}" ]]; then
  scan "${PRE_COMMIT_FROM_REF:-0}" "$PRE_COMMIT_TO_REF" "${PRE_COMMIT_REMOTE_BRANCH:-}"
elif [[ ! -t 0 ]]; then
  n=0
  while read -r _local_ref local_sha remote_ref remote_sha; do
    n=$((n + 1))
    [[ "$local_sha" =~ ^0+$ ]] && continue   # deleting a ref
    scan "$remote_sha" "$local_sha" "$remote_ref"
  done
  if [[ $n -eq 0 ]]; then echo "secret-scan: pre-push received no refs" >&2; exit 1; fi
else
  echo "secret-scan: pre-push has no range to scan" >&2
  exit 1
fi
exit $fail
