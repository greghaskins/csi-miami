# csi-miami
Kubernetes Container Storage Interface (CSI) for Mounting Items Across My Infrastructure

## Development

Tools are pinned in `mise.toml`; `mise install` installs them and the git
hooks. Every commit and every push, locally and in CI, goes through
gitleaks secret scanning, and `.claude/settings.json` applies the same scan
to anything an agent publishes to GitHub from this checkout. The gitleaks config is kept outside this repository: export it as
`GITLEAKS_CONFIG_TOML` (the full TOML, normally extending the default rules
with `[extend] useDefault = true`), or as `GITLEAKS_CONFIG_TOML_B64` where a
setting cannot hold a multi-line value, before committing; without it the hooks
refuse to run rather than passing silently. CI reads the same content from
a repository secret of the same name.
