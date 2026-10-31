#!/usr/bin/env bash
# Content and commit messages of every commit in BASE..HEAD. A BASE that is
# unknown (a brand-new branch) or not an ancestor falls back to the last 50
# commits, which is never silently nothing.
set -euo pipefail
base=${1:?base}; head=${2:?head}
if [[ "$base" =~ ^0+$ ]] || ! git merge-base --is-ancestor "$base" "$head" 2>/dev/null; then
  range="-n 50 $head"
else
  range="$base..$head"
fi
fail=0
# shellcheck disable=SC2086
gitleaks git --redact --no-banner --exit-code 1 --log-opts="$range" . || fail=1
# shellcheck disable=SC2086
for c in $(git rev-list $range); do
  git log -1 --format=%B "$c" | hack/secret-scan/text.sh "commit message $c" || fail=1
done
exit $fail
