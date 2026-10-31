#!/usr/bin/env bash
# gitleaks silently uses its built-in rules when no config is given, which
# would make every check here pass while checking nothing. Fail closed.
set -euo pipefail
if ! command -v gitleaks >/dev/null; then
  echo "secret-scan: gitleaks is not on PATH (run: mise install)" >&2
  exit 1
fi
if [[ -z "${GITLEAKS_CONFIG_TOML:-}" ]]; then
  echo "secret-scan: GITLEAKS_CONFIG_TOML is not set; the gitleaks config is required (README, Development)" >&2
  exit 1
fi
if ! grep -q '^\[\[rules\]\]' <<<"$GITLEAKS_CONFIG_TOML"; then
  echo "secret-scan: GITLEAKS_CONFIG_TOML has no [[rules]] table" >&2
  exit 1
fi
