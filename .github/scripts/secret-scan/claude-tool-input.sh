#!/usr/bin/env bash
# Claude Code PreToolUse hook: scan what an agent is about to publish to
# GitHub (pull request, issue, comment, review, release text) before the
# API call. Reads the hook JSON on stdin; exit 2 blocks the call. Only
# GitHub-publishing calls are scanned, and for those a missing config
# blocks rather than passes.
set -euo pipefail
input=$(cat)
tool=$(jq -r '.tool_name // ""' <<<"$input")
case "$tool" in
  mcp__github__*) ;;
  Bash)
    cmd=$(jq -r '.tool_input.command // ""' <<<"$input")
    grep -q -E '(^|[;&| ])gh +(pr|issue|release|api|repo) ' <<<"$cmd" || exit 0
    # The private detector list guards what this repository publishes. A call
    # that names another repository with --repo (or -R) publishes elsewhere
    # and is not scanned; a call with no --repo publishes to the repository of
    # the directory it runs in (gh resolves it from the remote), so one that
    # changes into another checkout first is not scanned either.
    if grep -q -E -- '(^|[[:space:]])(--repo|-R)[= ]+[^[:space:]]+' <<<"$cmd"; then
      target=$(grep -o -E -- '(--repo|-R)[= ]+[^[:space:]]+' <<<"$cmd" | head -1 | sed -E 's/^(--repo|-R)[= ]+//')
      case "$target" in
        *csi-miami*) ;;
        *) exit 0 ;;
      esac
    elif grep -q -E '(^|[;&|] *)cd +[^[:space:];&|]+' <<<"$cmd"; then
      dir=$(grep -o -E '(^|[;&|] *)cd +[^[:space:];&|]+' <<<"$cmd" | head -1 | sed -E 's/^.*cd +//')
      remote=$(git -C "$dir" remote get-url origin 2>/dev/null || true)
      case "$remote" in
        *csi-miami*) ;;
        *) exit 0 ;;
      esac
    fi ;;
  *) exit 0 ;;
esac
# Only now, for a GitHub-publishing call: a missing config blocks the call.
if ! (. "$(dirname "${BASH_SOURCE[0]}")/env.sh") 2>/dev/null || ! command -v gitleaks >/dev/null; then
  echo "secret-scan: gitleaks and GITLEAKS_CONFIG_TOML (or its _B64 form) are required before publishing to GitHub" >&2
  exit 2
fi
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
if ! jq -r '.tool_input' <<<"$input" | gitleaks stdin --redact --no-banner --exit-code 1 >/dev/null 2>&1; then
  echo "secret-scan: the tool input matched secret scanning; not sent" >&2
  exit 2
fi
