# csi-miami

**MIAMI**: **M**ounting **I**tems **A**cross **M**y **I**nfrastructure. A
Kubernetes CSI driver for my own clusters: real ext4 volumes on a NAS that
only speaks NFSv3, plus plain shared and node-local directories.

> **TECH:** Pod's stuck. The events keep piling up.
>
> **LIEUTENANT:** *(sunglasses on)* The evidence is *mounting.*
>
> **YEEEAAAH**

## Mounting modes

```
shared directory · RWX · M1
  pod ─▶ <export>/<ns>/<pvc>/ ─▶ NAS (NFSv3)

local directory · one node · M1
  pod ─▶ /export/data/<pvc>/ ─▶ node disk

image on the NAS · RWO · M2
  pod ─▶ ext4 ─▶ loop ─▶ pvc.img ─▶ NAS (NFSv3)

shared image · RWX · M3
  pod ─▶ NFSv4 ─▶ holder node's ext4 ─▶ loop ─▶ NAS

local image · one node · M4
  pod ─▶ ext4 ─▶ loop ─▶ pvc.img ─▶ node disk
```

Images can add LUKS2 and are held by one node at a time (attach plus a
heartbeat file on the NAS). Scratch is any of these with a path outside the
backup set. Details: [architecture brief](docs/design/2026-10-30-architecture-brief.md).

## Try it

```sh
kubectl apply -k deploy/kustomize/base
kubectl apply -k deploy/kustomize/examples   # edit server and paths first
```

## Development

`mise install` installs the pinned tools and git hooks. Commits, pushes, CI
and agent posts to GitHub all run gitleaks, whose config lives outside the
repo: export `GITLEAKS_CONFIG_TOML` (or `GITLEAKS_CONFIG_TOML_B64`) first,
normally `[extend] useDefault = true` plus your rules, or the hooks refuse
to run. CI reads a secret of the same name.

`make check` runs CI's `static` and `unit` jobs, `make build` builds
`bin/miami`, and `make image` builds the image for the host architecture.
