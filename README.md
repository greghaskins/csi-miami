# csi-miami

**MIAMI**: **M**ounting **I**tems **A**cross **M**y **I**nfrastructure. A
Kubernetes CSI driver for my own personal needs, on a bare-metal cluster with
a NAS, attached storage, and self-hosted workloads.

The goal: make bare-metal hardware behave like a proper Kubernetes storage
layer. csi-miami is responsible for:

1. **Flexibility** across directories and block volumes (blobs are a layout on a volume), local disks and a remote NAS.
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
│  │  volume: ext4 ▶ dm-crypt ▶ nbd,      │  │
│  │    served from a node-local cache    │  │
│  │  encryption: LUKS2, on for volumes   │  │
│  └──────┬─────────────────────┬─────────┘  │
│         │                     │            │
│  ┌──────▼──────────┐          │            │
│  │ local disk      │          │            │
│  │ /export/data/   │          │            │
│  │ volume cache    │          │            │
│  └─────────────────┘          │            │
└───────────────────────────────┼────────────┘
                                │ NFSv3, soft
                     ┌──────────▼──────────┐
                     │ NAS                 │
                     │ <ns>/<pvc>/         │
                     │ chunks/ manifests/  │
                     │ current, pvc.holder │
                     └─────────────────────┘
```

The `miami node` DaemonSet runs on every worker node and does all the mounting. A
small `miami controller` Deployment creates and deletes volumes on the NAS.
Details: [architecture brief](docs/design/2026-10-30-architecture-brief.md).

A **directory** is a subdirectory of the NAS export bound straight into the pod.

A **volume** is a block device. The live data is on the node, in a cache; the NAS holds point-in-time **generations** of it, as content-addressed chunks plus a manifest naming them (`chunks/<aa>/<sha256>`, `manifests/<epoch>.<n>`, `current`). Each volume gets one `miami blockd` block server, a transient systemd unit on the host (`miami-blockd-<volume>.service`) that serves an nbd device from the cache and publishes a generation every few minutes, at a watermark of unpublished data, on request, and always at unstage. The plugin pod can restart, upgrade or crash without the device noticing; a killed block server is restarted by systemd and re-points the same device. A volume is encrypted (LUKS2) unless its class says `encryption: "off"`, so the chunks on the NAS are ciphertext. Operating one: [docs/operating-volumes.md](docs/operating-volumes.md). Design: [chunk-backed volumes](docs/design/2026-11-03-chunk-backed-volumes-design.md).

## Four independent choices

Each StorageClass picks one option per dimension, in any combination the item type allows.

| Dimension | Options |
|---|---|
| Location | local disk, NAS (NFSv3). A volume's store must be on the NAS; its node-local data is a cache |
| Item type | flat files in a directory, block volume (ext4 on an nbd device served from chunks; blobs are a layout on one) |
| Sharing | single node (RWO), shared (RWX; directories only for now) |
| Encryption | off, on (volumes only) |

## Supported today

| | Location | Sharing | Encryption |
|---|---|---|---|
| **Directories** | **local disk:** in progress<br>**NAS:** in progress | **single node:** in progress<br>**shared:** in progress | **off:** in progress<br>**on:** not supported |
| **Volumes** | **local disk:** not supported<br>**NAS:** in progress | **single node:** in progress<br>**shared:** not yet (M3, re-export) | **off:** in progress (`-plain` classes)<br>**on:** in progress (the default) |

"In progress" means merged on the M2 branch and covered by tests. The end-to-end suite for volumes passes on kind (both architectures, the nfs3 variant); only the throughput gate (G1, `miami bench` on a Raspberry Pi 4) is pending. Blobs are not a kind: a content-addressed store is a layout an application keeps on a volume.

## Installation

`kubectl apply -k deploy/kustomize/base` creates the `miami-system` namespace, the CSIDriver, the RBAC (ServiceAccounts, ClusterRoles and bindings), a node DaemonSet, and a controller Deployment. The examples are applied separately.

**Namespace.** Everything runs in `miami-system`, labeled with Pod Security Admission `enforce: privileged` for the node agent's privileged security context and the controller's `SYS_ADMIN` capability (the controller mounts NFSv3 transports with `mount(2)`, which containerd's default AppArmor profile denies even with the capability).

**Images.** The base uses the placeholder `ghcr.io/greghaskins/csi-miami:unset`, a tag that cannot be pulled; every overlay sets the real image with `images:`, preferably by digest. The sidecars (csi-provisioner, csi-attacher, csi-resizer, csi-node-driver-registrar, livenessprobe) are pinned by digest and are public at `registry.k8s.io/sig-storage/`. The driver image is public at ghcr.io/greghaskins/csi-miami.

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

**Volume kind.** A node serves volumes only when it has what they need, probed on every pass and exported as `miami_node_capability{feature}`; a node missing one still serves directories, and a volume staged there waits with the `prerequisite-missing` condition.

| Feature | What the node needs |
|---|---|
| `nbd` | the kernel's nbd netlink family resolves (load the `nbd` module at boot) and `/sys/block/nbd*` can be listed |
| `device-mapper` | dm-crypt and `/dev/mapper/control`, for encrypted volumes |
| `adiantum` | the kernel can provide `adiantum(xchacha12,aes)` (probed by an `AF_ALG` bind, which loads it), for the default cipher |
| `systemd-socket` | systemd as the host's init, with its private socket `/run/systemd/private` |
| `cache-dir` | a writable directory for the volume caches, on local disk |

The DaemonSet carries what this takes: hostPaths for the host's `/dev` (`HostToContainer`), `/run/systemd` and `/sys` (both read-only), on top of the kubelet directory and the privileged container it already has. There is no `hostPID`: each volume's block server is a systemd unit on the host. The caches (`<kubelet-dir>/plugins/miami.csi.greghaskins.com/cache`) and the per-build copies of the binary the units run (`.../bin`) live under the kubelet directory, which the container and the host see at the same path, so they need no hostPath of their own; `-cache-dir`, `-bin-dir` and `-shared-dir` move them (see [docs/operating-volumes.md](docs/operating-volumes.md)). `cryptsetup`, `e2fsprogs` and `blkid` exist only inside the driver image; install nothing on the node.

**Host watchdogs.** Host NFS watchdogs or scanners must exclude the driver's transports and stage roots. In `<kubelet-dir>/plugins/miami.csi.greghaskins.com/` exclude `transports` (the mounts) and `intents` (the intent records). Also exclude `<kubelet-dir>/plugins/kubernetes.io/csi/miami.csi.greghaskins.com` (the stage root). In the controller's `/var/lib/miami` (an emptyDir mount inside the container) exclude `transports`.

**Apply the examples:**

```sh
kubectl apply -k deploy/kustomize/base
kubectl apply -k deploy/kustomize/examples
```

Edit the examples first: in `miami-remote-dir-rwx.yaml`, `miami-remote-vol-rwo.yaml` and `miami-remote-vol-rwo-plain.yaml` set `transport.server` and `transport.export` (nfs3); in `miami-local-dir-rwo.yaml` set `transport.path`.

## StorageClass parameters

**Naming.** A class is named `miami-<location>-<type>-<sharing>`, with a `-plain` suffix when a volume class is not encrypted. `type` is `dir` (a directory item) or `vol` (a block volume), `location` is `local` (the node's disk) or `remote` (a network server, NFSv3 today), and `sharing` is `rwo` (one node) or `rwx` (many nodes). A volume is encrypted by default, so the encrypted class has no suffix and the unencrypted one says so: `miami-remote-vol-rwo` and `miami-remote-vol-rwo-plain`. A directory has no encryption and no suffix. The examples are `miami-remote-dir-rwx` (a directory on NFSv3, many nodes), `miami-local-dir-rwo` (a directory on the node's disk, one node), `miami-remote-vol-rwo` (an encrypted volume on the NAS) and `miami-remote-vol-rwo-plain` (the same, unencrypted: the chunks on the NAS are plaintext ext4 blocks).

| Parameter | Values | Required | Meaning |
|---|---|---|---|
| `kind` | `directory`, `volume` | yes | Item type |
| `transport.kind` | `nfs3`, `local` | yes | Transport type; a volume needs `nfs3` (`local` is refused: a chunk store on the node's own disk would only back up the node to itself) |
| `transport.server` | IPv4 (lower case), hostname (lower case) | nfs3 | NFS server; IPv6 and ports not supported |
| `transport.export` | absolute path | nfs3 | NFS export path |
| `transport.path` | absolute path | local | Host path |
| `subdir` | relative path with tokens | no | Prefix under the transport; `${pvc.namespace}` and `${pvc.name}` are expanded; elements must not be `..` or shaped like `pvc-<uuid>`. The volume's own name is always appended; default none |
| `mode` | 1–4 octal digits | no | Mode of the volume's directory when created (default `0755`); `0777` (world-writable for root-squashed exports) and `2775` (setgid) are common; setuid (4–7 leading) is refused. An existing directory is adopted and never chmod'ed; upgrade every node plugin before setting this parameter |

An unknown parameter is `InvalidArgument`, so a typo cannot silently mean the default. A directory item accepts access modes `SINGLE_NODE_WRITER`, `SINGLE_NODE_MULTI_WRITER`, `MULTI_NODE_MULTI_WRITER` and `MULTI_NODE_READER_ONLY`. No `fsType` is accepted.

**Parameters of `kind: volume`.** All optional; a directory class that sets one is refused.

| Parameter | Values | Default | Meaning |
|---|---|---|---|
| `chunkSize` | power of two, 1Mi to 256Mi | the smallest power of two of at least 4Mi that keeps the manifest to 65,536 lines (4Mi up to 256Gi, 64Mi at 4Ti) | Size of one chunk of the device, fixed at creation. A value that would exceed the line cap for the volume is refused. Small chunks (1 to 4Mi) bound read amplification for a volume that runs lazily |
| `cacheSize` | bytes, or a number with `Ki`, `Mi`, `Gi` or `Ti` | the volume's size | Bound of the node-local cache, at least `chunkSize`. Clean chunks are evicted least-recently-used; unpublished (dirty) chunks are pinned. Below the volume's size the volume runs lazily, fetching what it reads |
| `encryption` | `"on"`, `"off"` (quote them in YAML) | `"on"` | LUKS2 under ext4. `"off"` is the `-plain` class and needs no Secret |
| `cipher` | `xchacha12,aes-adiantum-plain64`, `aes-xts-plain64` | `xchacha12,aes-adiantum-plain64` | Adiantum suits CPUs without AES instructions |
| `keySize` | bits: 256 for Adiantum, 256 or 512 for `aes-xts-plain64` | 256 | |
| `generations.interval` | duration, at least `1m` | `5m` | How often a volume with unpublished writes publishes a generation |
| `generations.keep` | integer, at least 1 | 3 | Manifests kept; chunks no kept manifest references are removed after a publish |
| `generations.maxAge` | duration, at least `generations.interval` | `1h` | A volume with unpublished writes whose newest generation is older than this is reported overdue |
| `generations.dirtyWatermark` | bytes, as `cacheSize`; at most `cacheSize` | half of `cacheSize` | Unpublished bytes that trigger a generation without waiting for the interval |
| `takeoverTimeout` | | | **Rejected.** Decision 52 removed the timed takeover; a class that sets it is `InvalidArgument` (a PV that still carries it is read and ignored). See [docs/operating-volumes.md](docs/operating-volumes.md) for what moves a volume instead |

A volume is ext4 (`csi.storage.k8s.io/fstype: ext4`, or no `fsType` at all; any other is refused), mounted `discard,errors=remount-ro,nosuid,nodev`. Set the fstype in the class: the kubelet applies a pod's `fsGroup` only to a volume whose PV carries one, and the provisioner sets it only from that parameter. A volume accepts access modes `SINGLE_NODE_WRITER` and `SINGLE_NODE_MULTI_WRITER` (RWO); RWX comes with M3. The requested capacity is rounded up to whole chunks and the rounded size is reported. `allowVolumeExpansion: true` enables online growth (grow-only; the controller records the new size beside the volume, the node grows the device, the mapping and the file system, and the `csi-resizer` sidecar drives it).

**The key.** An encrypted class names a Secret in the claim's namespace for both the node-stage and the node-publish references (so a restage from a publish still has the key after a plugin restart); the examples use `miami-volume-key`. Its `key` entry is the key, byte for byte, and must be between 32 and 4096 bytes (a shorter or longer one is refused as `InvalidArgument` at stage and publish, naming the entry and its length, never the value); generate one with `openssl rand -base64 48`, and is read only to format and open the volume: it is held in memory, never written to the intent record, an event or a log, and never reaches the block server. Create the Secret before the first claim in a namespace, and do not delete it while volumes use the class. To rotate, set `previousKey` to the old value and `key` to the new one; at the next stage of each volume, if `key` does not open the volume and `previousKey` does, the keyslot is changed to `key` (`cryptsetup luksChangeKey`) and the volume opens. A volume that is already staged is not touched until it is next staged. Remove `previousKey` once every volume has been staged again. **Rotation changes a keyslot, not the master key.** `cryptsetup luksChangeKey` re-wraps the same volume (master) key under the new key; the data is not re-encrypted. Every older copy of the volume's header (older generations in the store, NAS snapshots, offsite copies) still opens with the old key, and anyone who held the old key and can read one of those copies can read the data. If a key may have been compromised, rotation is not enough: create a new volume with a new key and copy the data into it (mount both, copy, and delete the old volume and its snapshots and offsite copies), because only a new volume has a new master key. A missing key is the `key-required` condition (and a `KeyRequired` event); a key that matches neither is `bad-key`.

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

**Directories and transports**

- **Close-to-open consistency on NFSv3.** Attribute cache is 30 seconds. A file or directory created on another node is seen on the next lookup; directory listings lag up to 30 seconds.
- **No cross-node `flock` or `fcntl` locks.** The mount uses `nolock`. Use `miami lock` or `mkdir` with `O_EXCL`.
- **Soft NFS mounts.** During an outage, I/O blocks up to 180 seconds, then fails with `ETIMEDOUT`. A program holding a file open across a server restart must check errors on `fsync` and `close` and write again. Requires Linux 5.3+ for `softerr`.
- **ESTALE on server restart.** During the restart, directories answer `ESTALE` and recover when the server returns. A file held open across the window may not. The driver reports the condition (`miami_transport_up`, `miami_volume_condition`, `TransportUnavailable` event) and does not remediate in M1.
- **Directories have none of the volume machinery.** A directory item has no encryption, fencing, expansion or exclusivity; those belong to the volume kind. There is no filesystem-image item (a loop device over a file on the NAS) and no opaque-blob kind: both are served by a volume.
- **Transport options are fixed in code.** The NFSv3 profile (`vers=3,proto=tcp,softerr,timeo=100,retrans=17,nolock,lookupcache=positive,actimeo=30,noacl`) and the `local` bind are not configurable per StorageClass.
- **Mount-point checks and old profiles.** A `local` transport does not require its path to be a mount point, so a directory meant to be a separate disk is bound from whatever is there when the disk is not mounted; and a transport mounted by an older option profile is reported as a conflict until it is unmounted by hand.
- **Local volumes are not deleted by the controller.** When a `local` volume is deleted, the controller succeeds and logs a warning. The directory remains on the node's disk and is not removed (the node-local tier handles removal in M4).

**Volumes**

- **RWO only.** A volume mounts on one node at a time; the access modes are `SINGLE_NODE_WRITER` and `SINGLE_NODE_MULTI_WRITER`. RWX by re-export from the holder node is M3. No snapshots or clones (M4), no shrink, and the store must be on the NAS: `transport.kind: local` is refused.
- **The node that holds a volume keeps it until it syncs it.** A volume moves to another node only after its holder has unstaged it cleanly: the final generation published, `current` re-read from the store and found to name it. No node takes a volume over because it is old or its node is NotReady (decision 52). A pod scheduled elsewhere waits as `holder-contended`, naming the holder. A node that is down keeps its volumes until it returns (it then drains any it no longer stages on its own) or an operator detaches them, which gives up whatever the holder had not published. The runbook is in [docs/operating-volumes.md](docs/operating-volumes.md).
- **No automatic remediation.** The driver reports a failure (a condition, an event, a metric) and repairs nothing by deleting pods (decision 53). The one case left for a person: ext4 gone read-only after the cache filled with unpublished chunks while the store was unreachable; restart the pod once the store is back and the cache has drained.
- **The store being down is survivable, up to the cache.** A pod keeps running on local data while the NAS is unreachable, and generations wait. Writes fail with I/O errors only when the cache is smaller than the volume and fills with unpublished chunks. What a node's death loses is bounded by what it had not published: at most one generation interval plus the publish time when the store is reachable, more when it is not.
- **Plaintext on the node.** Encryption at rest protects the NAS, its snapshots and the offsite copy. The key is held by the node's kernel for as long as the volume is mapped, and plaintext is in the node's page cache; the node's own cache holds ciphertext chunks. A compromised node is outside the model. A `-plain` class stores plaintext ext4 blocks, on the NAS and in the cache.
- **Retention is the offsite job's.** The driver keeps the last `generations.keep` manifests and removes chunks only they referenced. A mirror that never deletes accumulates history until a lifecycle rule ages it.
- **A running block server keeps its binary until the volume is next staged.** A plugin upgrade does not touch a served volume; `miami_volumes_served{state="outdated-binary"}` counts the ones still on an older build. A rollout that must reach every server is a drain.
- **Restore needs coreutils, and cryptsetup for an encrypted volume.** [docs/restore.md](docs/restore.md) has the drill, `miami restore` and `miami import`. Throughput against a loop device is measured by `miami bench`: [docs/bench.md](docs/bench.md).

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
