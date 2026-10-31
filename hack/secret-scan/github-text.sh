#!/usr/bin/env bash
# CI only: scan the text of the GitHub event that triggered the workflow,
# one field at a time. Writes to $GITHUB_OUTPUT:
#   hit=true|false
#   kind=comment|text
#   fields=<space-separated names of the fields that matched, in the order
#          listed here; empty when nothing matched>
# Field names: title, body, ref (pull request head branch), tag (release tag
# name), name (release name). A comment is reported as kind=comment with
# fields=body. Never prints the text. Exits 0 with or without a hit; exits 1
# for an unhandled event or a failure to read the event.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
ev=${GITHUB_EVENT_NAME:?}; path=${GITHUB_EVENT_PATH:?}
# Each entry is name=jq-path, in the order the names are reported.
case "$ev" in
  issue_comment|pull_request_review_comment|discussion_comment)
    kind=comment; spec=(body=.comment.body) ;;
  issues)               kind=text; spec=(title=.issue.title body=.issue.body) ;;
  pull_request)         kind=text; spec=(title=.pull_request.title body=.pull_request.body ref=.pull_request.head.ref) ;;
  pull_request_review)  kind=text; spec=(body=.review.body) ;;
  release)              kind=text; spec=(tag=.release.tag_name name=.release.name body=.release.body) ;;
  discussion)           kind=text; spec=(title=.discussion.title body=.discussion.body) ;;
  *) echo "secret-scan: unhandled event $ev" >&2; exit 1 ;;
esac
fields=()
for s in "${spec[@]}"; do
  name=${s%%=*}; q=${s#*=}
  # A null field is skipped; jq failing stops the script (set -e).
  value=$(jq -r "$q // empty" "$path")
  if [[ -z $value ]]; then
    continue
  fi
  if ! printf '%s\n' "$value" | hack/secret-scan/text.sh "$ev $name"; then
    fields+=("$name")
  fi
done
hit=false
if ((${#fields[@]} > 0)); then
  hit=true
fi
{ echo "hit=$hit"; echo "kind=$kind"; echo "fields=${fields[*]-}"; } >>"${GITHUB_OUTPUT:-/dev/null}"
