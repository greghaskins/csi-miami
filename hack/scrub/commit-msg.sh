#!/usr/bin/env bash
# The commit message, with comment lines removed as git itself would.
set -euo pipefail
if ! git stripspace --strip-comments <"$1" | gitleaks stdin --redact --no-banner --exit-code 1 >/dev/null 2>&1; then
  echo "scrub: the commit message contains text the rule set forbids" >&2
  exit 1
fi
