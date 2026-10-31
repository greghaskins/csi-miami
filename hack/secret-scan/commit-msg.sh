#!/usr/bin/env bash
# The commit message, with comment lines removed as git itself would.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
if ! git stripspace --strip-comments <"$1" | gitleaks stdin --redact --no-banner --exit-code 1 >/dev/null 2>&1; then
  echo "secret-scan: the commit message matched secret scanning" >&2
  exit 1
fi
