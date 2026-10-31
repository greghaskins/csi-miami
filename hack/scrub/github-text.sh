#!/usr/bin/env bash
# CI only: scan the text of the GitHub event that triggered the workflow.
# Writes hit=true|false and kind=comment|text to $GITHUB_OUTPUT. Never
# prints the text.
set -euo pipefail
ev=${GITHUB_EVENT_NAME:?}; path=${GITHUB_EVENT_PATH:?}
case "$ev" in
  issue_comment|pull_request_review_comment|discussion_comment)
    kind=comment; q='[.comment.body]' ;;
  issues)               kind=text; q='[.issue.title, .issue.body]' ;;
  pull_request_target)  kind=text; q='[.pull_request.title, .pull_request.body, .pull_request.head.ref]' ;;
  pull_request_review)  kind=text; q='[.review.body]' ;;
  release)              kind=text; q='[.release.tag_name, .release.name, .release.body]' ;;
  discussion)           kind=text; q='[.discussion.title, .discussion.body]' ;;
  *) echo "scrub: unhandled event $ev" >&2; exit 1 ;;
esac
hit=false
if ! jq -r "$q | map(select(. != null)) | join(\"\n\")" "$path" | hack/scrub/text.sh "$ev text"; then
  hit=true
fi
{ echo "hit=$hit"; echo "kind=$kind"; } >>"${GITHUB_OUTPUT:-/dev/null}"
