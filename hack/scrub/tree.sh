#!/usr/bin/env bash
# Every tracked file and path in the working tree.
set -euo pipefail
exec gitleaks dir --redact --no-banner --exit-code 1 .
