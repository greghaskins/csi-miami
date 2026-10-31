#!/usr/bin/env bash
# gitleaks silently uses its built-in rules when no config is given, which
# would make every check here pass while checking nothing. env.sh refuses
# when no config is set; this adds the rest.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
if ! command -v gitleaks >/dev/null; then
  echo "secret-scan: gitleaks is not on PATH (run: mise install)" >&2
  exit 1
fi
if ! grep -q '^\[\[rules\]\]' <<<"$GITLEAKS_CONFIG_TOML"; then
  echo "secret-scan: the gitleaks config has no [[rules]] table" >&2
  exit 1
fi
