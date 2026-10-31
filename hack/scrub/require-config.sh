#!/usr/bin/env bash
# gitleaks silently uses its built-in rules when no config is given, which
# would make every scrub check pass while checking nothing. Fail closed.
set -euo pipefail
if ! command -v gitleaks >/dev/null; then
  echo "scrub: gitleaks is not on PATH (run: mise install)" >&2
  exit 1
fi
if [[ -z "${GITLEAKS_CONFIG_TOML:-}" ]]; then
  echo "scrub: GITLEAKS_CONFIG_TOML is not set; the rule set is required (README, Development)" >&2
  exit 1
fi
if ! grep -q '^\[\[rules\]\]' <<<"$GITLEAKS_CONFIG_TOML"; then
  echo "scrub: GITLEAKS_CONFIG_TOML has no [[rules]] table" >&2
  exit 1
fi
