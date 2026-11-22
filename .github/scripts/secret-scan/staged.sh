#!/usr/bin/env bash
# Staged content and paths, before a commit is created.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
exec gitleaks git --pre-commit --staged --redact --no-banner --exit-code 1 .
