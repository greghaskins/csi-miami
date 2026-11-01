# csi-miami

**MIAMI**: **M**ounting **I**tems **A**cross **M**y **I**nfrastructure. A
Kubernetes CSI driver for my own personal needs, on a bare-metal cluster with
a NAS, attached storage, and self-hosted workloads.

The goal: make bare-metal hardware behave like a proper Kubernetes storage
layer. csi-miami is responsible for:

1. **Flexibility** across directories, images and blobs, local disks and a remote NAS.
2. **Strong RWO and RWX semantics**, available when a workload needs them.
3. **Encryption at rest.**

All of it should be mostly transparent to workloads, especially open source
projects run off the shelf.

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
│  │  image: loop ▶ ext4                  │  │
│  │  blobs: opaque, content-addressed    │  │
│  │  encryption: optional, any item      │  │
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
Details: [architecture brief](docs/design/2026-10-30-architecture-brief.md).

## Four independent choices

Each StorageClass picks one option per dimension, in any combination.

| Dimension | Options |
|---|---|
| Location | local disk, NAS (NFSv3) |
| Item type | flat files in a directory, filesystem image (ext4 on a loop device), opaque blobs |
| Sharing | single node (RWO), shared (RWX) |
| Encryption | off, on |

## Milestones

| | Adds |
|---|---|
| **M1** (building) | directories, on local disk and on the NAS, shared |
| **M2** (next) | images on the NAS, single node with exclusivity and fencing, encryption at rest |
| **M3** | shared images, re-exported over NFSv4 by the node holding them |
| **M4** | images on local disk, moved between nodes with `miami move` |
| Later, maybe | blobs: opaque, content-addressed (an S3 interface on top is out of scope) |

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
