#!/usr/bin/env bash
# Staged content and paths, before a commit is created.
set -euo pipefail
exec gitleaks git --pre-commit --staged --redact --no-banner --exit-code 1 .
