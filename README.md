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

The `miami node` DaemonSet runs on every worker node and does all the mounting. A
small `miami controller` Deployment creates and deletes volumes on the NAS.
Details: [architecture brief](docs/design/2026-10-30-architecture-brief.md).

## Four independent choices

Each StorageClass picks one option per dimension, in any combination.

| Dimension | Options |
|---|---|
| Location | local disk, NAS (NFSv3) |
| Item type | flat files in a directory, filesystem image (ext4 on a loop device), opaque blobs (content-addressed, no S3 interface) |
| Sharing | single node (RWO), shared (RWX) |
| Encryption | off, on |

## Supported today

| | Location | Sharing | Encryption |
|---|---|---|---|
| **Directories** | **local disk:** in progress<br>**NAS:** in progress | **single node:** in progress<br>**shared:** in progress | **off:** in progress<br>**on:** not yet |
| **Images** | **local disk:** not yet<br>**NAS:** not yet | **single node:** not yet<br>**shared:** not yet | **off:** not yet<br>**on:** not yet |
| **Blobs** | **local disk:** not yet<br>**NAS:** not yet | **single node:** not yet<br>**shared:** not yet | **off:** not yet<br>**on:** not yet |

## Installation

`kubectl apply -k deploy/kustomize/base` creates the `miami-system` namespace, the CSIDriver, the RBAC (ServiceAccounts, ClusterRoles and bindings), a node DaemonSet, and a controller Deployment. The examples are applied separately.

**Namespace.** Everything runs in `miami-system`, labeled with Pod Security Admission `enforce: privileged` for the node agent's privileged security context and the controller's `SYS_ADMIN` capability (the controller mounts NFSv3 transports with `mount(2)`, which containerd's default AppArmor profile denies even with the capability).

**Images.** The base uses the placeholder `ghcr.io/greghaskins/csi-miami:unset`, a tag that cannot be pulled; every overlay sets the real image with `images:`, preferably by digest. The sidecars (csi-provisioner, csi-attacher, csi-node-driver-registrar, livenessprobe) are pinned by digest and are public at `registry.k8s.io/sig-storage/`. The driver image is public at ghcr.io/greghaskins/csi-miami.

**Kubelet directory.** The base defaults to `/var/lib/kubelet`. Every distribution has its own:

| Distribution | Path |
|---|---|
| k0s | `/var/lib/k0s/kubelet` |
| k3s | `/var/lib/rancher/k3s/agent/kubelet` |
| microk8s | `/var/snap/microk8s/common/var/lib/kubelet` |

An overlay (at `deploy/kustomize/overlays/<name>/kustomization.yaml`, or with `../../base` replaced if elsewhere) that sets a different path must repeat the kustomization.yaml replacement (the base snippet is in comments there). Set `newTag` to a commit sha (the publish job tags images by sha) or a release tag:

```yaml
resources:
  - ../../base
images:
  - name: ghcr.io/greghaskins/csi-miami
    newTag: <sha or tag>
configMapGenerator:
  - name: miami-deploy
    namespace: miami-system
    behavior: merge
    literals:
      - kubeletDir=/var/lib/k0s/kubelet
replacements:
  - source: {kind: ConfigMap, name: miami-deploy, fieldPath: data.kubeletDir}
    targets:
      - select: {kind: DaemonSet, name: miami-node}
        fieldPaths:
          - spec.template.spec.containers.[name=miami].env.[name=KUBELET_DIR].value
          - spec.template.spec.containers.[name=csi-node-driver-registrar].env.[name=KUBELET_DIR].value
          - spec.template.spec.containers.[name=miami].volumeMounts.[name=kubelet-dir].mountPath
          - spec.template.spec.volumes.[name=kubelet-dir].hostPath.path
```

**Control planes.** The driver runs on worker nodes by default: a required node affinity on the node DaemonSet and the controller Deployment excludes nodes labelled `node-role.kubernetes.io/control-plane` or `node-role.kubernetes.io/master`. The DaemonSet tolerates every taint, so custom taints on workers do not keep it off. To run the driver on control planes, remove the affinity in your overlay with a JSON patch (the base kustomization.yaml has the snippet).

**Local transport.** A `local` transport requires its host directory mounted into the node container. Add a volumeMount to the miami container with `mountPropagation: HostToContainer` and a hostPath volume to the pod spec.

**Host watchdogs.** Host NFS watchdogs or scanners must exclude the driver's transports and stage roots. In `<kubelet-dir>/plugins/miami.csi.greghaskins.com/` exclude `transports` (the mounts) and `intents` (the intent records). Also exclude `<kubelet-dir>/plugins/kubernetes.io/csi/miami.csi.greghaskins.com` (the stage root). In the controller's `/var/lib/miami` (an emptyDir mount inside the container) exclude `transports`.

**Apply the examples:**

```sh
kubectl apply -k deploy/kustomize/base
kubectl apply -k deploy/kustomize/examples
```

Edit `deploy/kustomize/examples/miami-remote-dir-rwx.yaml` and `miami-local-dir-rwo.yaml` first: set `transport.server` and `transport.export` for nfs3, and `transport.path` for local.

## StorageClass parameters

**Naming.** A class is named `miami-<location>-<type>-<sharing>`, with a `-<crypt>` suffix when it is encrypted. `type` is `dir` (a directory item; `img` for image items later), `location` is `local` (the node's disk) or `remote` (a network server, NFSv3 today), and `sharing` is `rwo` (one node) or `rwx` (many nodes). The crypt token is defined with encryption in M2. The examples are `miami-remote-dir-rwx` (a directory on NFSv3, many nodes) and `miami-local-dir-rwo` (a directory on the node's disk, one node).

| Parameter | Values | Required | Meaning |
|---|---|---|---|
| `kind` | `directory` | yes | Item type; only directories in M1 |
| `transport.kind` | `nfs3`, `local` | yes | Transport type |
| `transport.server` | IPv4 (lower case), hostname (lower case) | nfs3 | NFS server; IPv6 and ports not supported |
| `transport.export` | absolute path | nfs3 | NFS export path |
| `transport.path` | absolute path | local | Host path |
| `subdir` | relative path with tokens | no | Prefix under the transport; `${pvc.namespace}` and `${pvc.name}` are expanded; elements must not be `..` or shaped like `pvc-<uuid>`. The volume's own name is always appended; default none |
| `mode` | 1–4 octal digits | no | Mode of the volume's directory when created (default `0755`); `0777` (world-writable for root-squashed exports) and `2775` (setgid) are common; setuid (4–7 leading) is refused. An existing directory is adopted and never chmod'ed; upgrade every node plugin before setting this parameter |

A directory item accepts access modes `SINGLE_NODE_WRITER`, `SINGLE_NODE_MULTI_WRITER`, `MULTI_NODE_MULTI_WRITER` and `MULTI_NODE_READER_ONLY`. No `fsType` is accepted.

## Locking across nodes

The kernel's `flock` does not work across nodes because the NFS mount uses `nolock`. Cross-node coordination uses `mkdir` and `O_EXCL`, which are server round-trips. The `miami lock` helper wraps these:

```sh
miami lock acquire --dir <path> --name <lock> --holder <string>
miami lock release --dir <path> --name <lock> --holder <string>
miami lock run     --dir <path> --name <lock> -- <command> [args...]
```

A lock is the directory `<dir>/<lock>.lock` holding `holder` and `since` files. `acquire` attempts once (exit 0 held, 1 another holder, 2 error, 3 the lock path is not a lock) and prints a line on stderr naming the reason when it broke a stale lock. `release` removes the lock if held by the given `--holder` (exit 0 released, 1 not held by that holder, 2 error, 3 not a lock). `run` acquires, executes the command with stdio inherited, renews `since` every `--renew` (default 30 seconds), and releases on exit; a lost lock terminates the command (exit 2) and a contended lock exits 1 without starting the command. `--stale` (default 5 minutes, floor 60 seconds) lets an acquirer break a lock older than that on the server's clock; a stamp more than 30 seconds ahead of the server's clock is also stale (a stamp set into the future, or a server clock stepped back, cannot keep a lock unbreakable). `--holder` is optional for `run` (default `<hostname>:<pid>`) and required for `acquire` and `release`. Two containers of one pod that are both PID 1 get the same default, so containers that share a pod should pass `--holder` with a value that tells them apart. `--timeout` (default 10 seconds) bounds each file system call.

Exit codes of `run`: the command's status (0–255; 128+n if killed by signal n), 1 if another holder contests (command not started), 3 if the lock path is not a lock (command not started), 2 on error or if the lock was lost (command terminated).

Only a lock-shaped directory is ever broken: a real directory (not a symbolic link) holding at most nine regular files, named `holder`, `since`, their temporary names, or the `.nfs<hex>` files NFSv3 leaves for a moment when a file is renamed over while open. A regular file, a symbolic link or a directory with other contents at `<dir>/<lock>.lock` may be someone's data: it is reported (exit 3) and never moved or removed. Breakers take turns through a `.<lock>.breaking` directory. A killed process can leave hidden entries beside the lock (`.<lock>.breaking`, `.<lock>.breaking-dead-*`, `.<lock>.broken-*`, `.<lock>.removed-*`, `.<lock>.probe-*`, `.<lock>.tmp-*`); they are harmless and can be deleted by hand.

The lock is a lease without fencing. NFSv3 has no conditional rename, so a holder that renews at the wrong moment while a breaker acts (one round trip of bad luck), or a breaker that stalls for longer than `--stale`, can leave a live lock stranded under a hidden name and two holders overlapping for up to one renew interval (`--renew`, 30 seconds by default) plus the stop grace (`--timeout`, 10 seconds by default). The work a lock protects must tolerate that.

Example wrapper with retry on contention:

```sh
while true; do
  miami lock run --dir /nfs/export --name mylock -- mycommand
  rc=$?
  [ $rc -eq 1 ] || break
  sleep 1
done
exit $rc
```

The helper runs the command in its own process group. SIGTERM or SIGINT sent to the helper is forwarded to that group. A wrapper script should `exec` its command so the group's leader is the real program and will receive the signal.

## Limitations

- **Close-to-open consistency on NFSv3.** Attribute cache is 30 seconds. A file or directory created on another node is seen on the next lookup; directory listings lag up to 30 seconds.
- **No cross-node `flock` or `fcntl` locks.** The mount uses `nolock`. Use `miami lock` or `mkdir` with `O_EXCL`.
- **Soft NFS mounts.** During an outage, I/O blocks up to 180 seconds, then fails with `ETIMEDOUT`. A program holding a file open across a server restart must check errors on `fsync` and `close` and write again. Requires Linux 5.3+ for `softerr`.
- **ESTALE on server restart.** During the restart, directories answer `ESTALE` and recover when the server returns. A file held open across the window may not. The driver reports the condition (`miami_transport_up`, `miami_volume_condition`, `TransportUnavailable` event) and does not remediate in M1.
- **Directory items only.** M1 does not support filesystem images, opaque blobs, encryption, fencing, expansion or exclusivity.
- **Transport options are fixed in code.** The NFSv3 profile (`vers=3,proto=tcp,softerr,timeo=100,retrans=17,nolock,lookupcache=positive,actimeo=30`) and the `local` bind are not configurable per StorageClass.
- **Mount-point checks and old profiles.** A `local` transport does not require its path to be a mount point, so a directory meant to be a separate disk is bound from whatever is there when the disk is not mounted; and a transport mounted by an older option profile is reported as a conflict until it is unmounted by hand.
- **Local volumes are not deleted by the controller.** When a `local` volume is deleted, the controller succeeds and logs a warning. The directory remains on the node's disk and is not removed (the node-local tier handles removal in M4).

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
