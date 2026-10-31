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
    grep -q -E '(^|[;&| ])gh +(pr|issue|release|api|repo) ' <<<"$cmd" || exit 0 ;;
  *) exit 0 ;;
esac
if ! command -v gitleaks >/dev/null || [[ -z "${GITLEAKS_CONFIG_TOML:-}" ]]; then
  echo "secret-scan: gitleaks and GITLEAKS_CONFIG_TOML are required before publishing to GitHub" >&2
  exit 2
fi
if ! jq -r '.tool_input' <<<"$input" | gitleaks stdin --redact --no-banner --exit-code 1 >/dev/null 2>&1; then
  echo "secret-scan: the tool input matched secret scanning; not sent" >&2
  exit 2
fi
