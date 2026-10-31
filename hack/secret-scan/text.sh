#!/usr/bin/env bash
# Scan stdin as one piece of text; $1 names it in the failure message.
# Never prints the text.
set -euo pipefail
if ! gitleaks stdin --redact --no-banner --exit-code 1 >/dev/null 2>&1; then
  echo "secret-scan: $1 matched secret scanning" >&2
  exit 1
fi
exit 0
