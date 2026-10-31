# Sourced by every script here. The gitleaks config arrives as
# GITLEAKS_CONFIG_TOML (the TOML itself) or, for settings that cannot hold a
# multi-line value, GITLEAKS_CONFIG_TOML_B64 (the same TOML, base64, one
# line). The second is decoded into the first. With neither, the script
# stops: gitleaks would otherwise fall back to its default rules silently.
if [[ -z "${GITLEAKS_CONFIG_TOML:-}" && -n "${GITLEAKS_CONFIG_TOML_B64:-}" ]]; then
  GITLEAKS_CONFIG_TOML=$(printf '%s' "$GITLEAKS_CONFIG_TOML_B64" | base64 -d 2>/dev/null) || GITLEAKS_CONFIG_TOML=""
  export GITLEAKS_CONFIG_TOML
fi
if [[ -z "${GITLEAKS_CONFIG_TOML:-}" ]]; then
  echo "secret-scan: neither GITLEAKS_CONFIG_TOML nor GITLEAKS_CONFIG_TOML_B64 is set; the gitleaks config is required (README, Development)" >&2
  exit 1
fi
