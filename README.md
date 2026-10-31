# csi-miami
Kubernetes Container Storage Interface (CSI) for Mounting Items Across My Infrastructure

## Development

Tools are pinned in `mise.toml`; `mise install` installs them and the git
hooks. Every commit, locally and in CI, is scrubbed against a private rule
set that is deliberately not in this repository. Export it as
`GITLEAKS_CONFIG_TOML` (the full gitleaks TOML config) before committing;
without it the hooks refuse to run rather than passing silently. CI reads
the same content from a repository secret of the same name.

