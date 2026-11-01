# csi-miami

**MIAMI**: **M**ounting **I**tems **A**cross **M**y **I**nfrastructure. A
Kubernetes CSI driver for my own personal needs, on a bare-metal cluster with
a NAS, attached storage, and self-hosted workloads.

> **TECH:** Pod's stuck. The events keep piling up.
>
> **LIEUTENANT:** *(sunglasses on)* The evidence is *mounting.*
>
> **YEEEAAAH**

## How it works

```
┌─ node ─────────────────────────────────────┐
│                                            │
│  ┌─ workload pod ───────────────────────┐  │
│  │  /data                               │  │
│  └──────────────────▲───────────────────┘  │
│                     │ bind mount           │
│  ┌─ miami node (DaemonSet) ─────────────┐  │
│  │  mounts the NAS export on the host   │  │
│  │  directory: bind the subdirectory    │  │
│  │  image: loop ▶ (LUKS2) ▶ ext4        │  │
│  └──────┬─────────────────────┬─────────┘  │
│         │                     │            │
│  ┌──────▼──────────┐          │            │
│  │ local disk      │          │            │
│  │ /export/data/   │          │            │
│  └─────────────────┘          │            │
└───────────────────────────────┼────────────┘
                                │ NFSv3, soft
                     ┌──────────▼──────────┐
                     │ NAS                 │
                     │ <ns>/<pvc>/         │
                     │ pvc.img, pvc.holder │
                     └─────────────────────┘
```

The `miami node` DaemonSet runs on every node and does all the mounting. A
small `miami controller` Deployment creates and deletes volumes on the NAS.
Images are held by one node at a time (attach plus the `.holder` heartbeat).
Details: [architecture brief](docs/design/2026-10-30-architecture-brief.md).

## Milestones

| | Mode | Where the data lives | Access |
|---|---|---|---|
| **M1** (building) | Shared directory | a subdirectory on the NAS | many nodes |
| **M1** (building) | Local directory | a subdirectory on the local disk | one node |
| **M2** (next) | Image on the NAS | an ext4 `.img` on the NAS | one node at a time |
| **M3** | Shared image | an M2 image, re-exported over NFSv4 by its holder node | many nodes |
| **M4** | Local image | an ext4 `.img` on the local disk, moved with `miami move` | one node |
| Later | S3 service | an S3 endpoint backed by an image volume | over the network |

## Installation

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

## Contributing

This is my own personal pet project. Others are welcome to use the code
under the [GPLv3](LICENSE) and modify it to their hearts' content.
