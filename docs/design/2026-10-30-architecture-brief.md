# csi-miami architecture brief

**Status:** Draft, decisions 1 to 5 recorded (section 8)
**Created:** 2026-10-30
**Updated:** 2026-10-30

MIAMI = Mounting Items Atop My Infrastructure. csi-miami is a Kubernetes
CSI driver that mounts *items* (an ext4 image file, a node-local image, a
plain shared directory) *atop* *transports* (NFSv3 first; local paths;
later SMB or anything that yields a directory on a node). Its reason to exist is
the same as every file-backed block driver's: shared storage that only
speaks NFSv3 loses POSIX mode bits and locking semantics, and workloads
such as git, SQLite and anything that calls `flock` need a real local
filesystem. The driver gives each pod a real ext4 over a loop device whose
backing file lives on the shared transport.

This brief proposes the architecture and lists the decisions that need an
owner's call before code is written. It is informed by operating earlier
drivers of this shape and by the failure modes they taught; those lessons
are stated here as design inputs, not as history.

## 1. Required capabilities (v1 parity target)

| Capability | v1 |
|---|---|
| Persistent `ReadWriteOnce` ext4 volume as a sparse `.img` on an NFSv3 export | yes |
| Exec bits, `chmod`, `flock` inside the volume behave like local disk | yes, canary-tested |
| StorageClass-driven transport config, no CRDs | yes |
| Per-namespace subtree on the export (`${pvc.namespace}/...` tokens) | yes |
| LUKS2 encryption, cipher selectable (Adiantum for CPUs without AES instructions), key from a node-stage Secret, slot rotation via `previousKey` | yes |
| Offline expansion (truncate, then `resize2fs` at next stage) | yes |
| Cross-node exclusivity: `attachRequired` plus a holder heartbeat judged on the transport's clock | yes, from day one |
| Unattended takeover after node death, with self-fencing on soft NFSv3 | yes |
| Failed-volume detection (stale NFS handle, forced read-only) with opt-in pod deletion behind an admission policy | yes |
| `fsGroupPolicy: File` for image volumes | yes |
| `VolumeCondition`, `GET_VOLUME_STATS` | yes |
| Multi-arch image (arm64, amd64), kustomize base pinned by tag | yes |
| `ReadWriteMany` directory item straight on the transport (no loop) | yes, section 4.8 |
| Writable cross-node Secret directory through FUSE | rejected, section 4.9 |
| Node-local persistent and scratch tiers | future, section 5 |
| S3-compatible object service built on the driver | future, section 5 |

## 2. Lessons that shape the design

Each item below is a root cause observed in drivers of this shape. The
design treats them as constraints.

1. **A state file that describes the kernel is a liability.** Restart bugs
   (leaked loop devices, poisoned "is the store mounted" caches, startup
   reconcile rounds) come from a cache disagreeing with mountinfo or
   `losetup`. Eventually the invariants all say "the kernel is the truth".
2. **Mounting the transport inside the plugin's private mount namespace is
   the single most expensive choice.** After a plugin restart the kernel
   reports loop back-files relative to a mount that no longer exists, so
   identity matching degrades into trailing-path heuristics and silly-rename
   special cases, and host-side mount watchdogs cannot see the transport.
3. **Imperative stage sequences need manual undo.** A-then-B-then-C with
   `detachOnFail` paths grows guards for every partial state (stacked
   mounts, adopted mounts, idle leftover loops, orphan holders, orphan
   dm-crypt mappings), and unstage re-derives the same facts from the kernel
   anyway.
4. **A controller that learns transport config only at `CreateVolume`
   leaks images.** After a restart or a StorageClass option change, delete
   has nothing to go on. The config must travel with the volume.
5. **Kubernetes RWO is not a cross-node mutex without `attachRequired`.**
   Pod deletion with a force-detach, surge rollouts and node partitions can
   put the same image read-write on two nodes.
6. **The loop's NFS file handle cannot be repaired.** `LOOP_CHANGE_FD`
   refuses, so recovery is always tear-down-and-reattach, never in place.
   Design the recovery path as the normal path.
7. **A hard NFS mount turns a NAS stall into a D-state node** that only a
   power cycle fixes. Soft NFSv3 with known `timeo` and `retrans` is also
   what makes self-fencing computable. The driver should own those options,
   not validate user-supplied ones.
8. **FUSE-backed volumes are the most fragile thing a node plugin can
   carry.** `requiresRepublish` plus kubernetes/kubernetes#121271 forbids
   a final error after the first publish, so a persisted registry with
   boot IDs is needed to tell restarts from first publishes; the mount dies
   with the process, so every plugin upgrade restarts consumers; and the
   content cap, flat namespace and `EPERM` on `chmod` all leak into the
   consumer. For the one use that motivated it (a small credential file
   refreshed exactly once cluster-wide) the hard part is the refresh
   contract, which needs a cooperating wrapper regardless of the
   filesystem beneath it.
9. **`flock` on an NFS mount with `nolock` coordinates nothing across
   nodes and reports success.** Cross-node mutual exclusion on NFSv3 must
   use `mkdir` or `O_EXCL` create, which the protocol makes atomic, with
   stale locks broken by the server's mtime.
10. **Leases are awkward.** `create` cannot be scoped by `resourceNames`,
    and GitOps tools commonly exclude `coordination.k8s.io/Lease`.
11. **Tests that pin argv strings don't catch behaviour.** An NFSv3 mount
    can break in the container image while every unit test stays green.
    End-to-end on NFSv3 from the first milestone.

## 3. Principles

- **The kernel is the only truth about this node.** mountinfo, `losetup
  --list`, dm sysfs and the holder file are observed state. Persisted local
  state is *intent* only.
- **Level-triggered reconciliation per node.** CSI RPCs write intent and
  wait for convergence; a reconciler drives observed toward desired, one
  idempotent step per layer. Garbage collection is the same loop with no
  intent.
- **Transports outlive the process.** Transport mounts live on the host
  mount table so a plugin restart changes nothing the kernel reports.
- **Volumes are self-describing.** Enough transport config rides in the
  volume ID or volume context for any component to find the item without a
  registry, a cache or a PV lookup.
- **The driver owns the transport profile.** Users name a server and an
  export; the driver chooses mount options it knows how to fence.
- **Exclusivity is layered and both layers are on by default.**
- **Small privileged surface, explicit grants**, each mirrored by an
  admission policy.
- **One binary, one DaemonSet, one CSI driver name**; the item kind is a
  StorageClass parameter (section 4.3).
- **Observability from day one.** Metrics and events are part of the
  contract.

## 4. Architecture

### 4.1 Vocabulary and layers

```
pod ──bind──▶ Mount (stage path)      ext4 over loop (+ dm-crypt)    bind of a subdirectory
                    ▲                         ▲                             ▲
                  Item                   ImageItem                    DirectoryItem
                    ▲                         ▲                             ▲
                Transport                       nfs3 / smb / local
```

- **Transport**: yields a directory on this node. `nfs3` (server, export,
  driver-owned options), `local` (a host path; also how a pre-mounted SMB or
  FUSE directory is used), later `smb`. Identified by a short hash of its
  canonical config, mounted once per node at `<root>/transports/<id>`.
- **Item**: a thing inside a transport directory that becomes a mount.
  `image` (sparse ext4 `.img`, optional LUKS2, single-node) and
  `directory` (a subdirectory bound straight into the pod, many-node,
  with the transport's own semantics).
- **Mount**: the staged filesystem plus its bind mounts into pods.

Package boundaries follow these nouns. Each layer exposes `Observe(ctx)
(state, error)` and `Converge(ctx, desired, observed) error`; the glue is
the reconciler, not the RPC handlers.

### 4.2 Node agent as a reconciler

- **Intent store**: one small record per volume under the kubelet plugin
  directory: item kind, transport config, stage path, mount flags, access
  mode, encryption flag, fenced flag. Written on `NodeStageVolume`, removed
  on `NodeUnstageVolume`. Nothing else is persisted.
- **Observers**: a `/proc/self/mountinfo` parser (single source; no
  `findmnt` shell-outs), `losetup --list --json`, dm-crypt mappings from
  sysfs, holder files on each mounted transport. Every observation runs
  under a deadline in its own goroutine, because a stat on a hung mount
  ignores context cancellation.
- **Converge loop**: on every RPC, on a timer, and at startup. For each
  intended volume: transport present → holder held → loop attached (direct
  I/O, recorded sector size) → dm-crypt open → fsck and resize if needed →
  mounted at the stage path → binds present. For each observed artifact
  with no intent: release holder, close mapping, detach loop, unmount.
- **Health loop**: the same observers report forced read-only and shutdown
  flags into `VolumeCondition`; remediation (4.6) consumes it.

The CSI servers become thin: validate, write intent, trigger converge, wait
with a deadline, map the result to a status code. Stage and unstage of one
volume stay serialized.

### 4.3 Component shape

- **One binary**, `miami`, with subcommands `node` and `controller`.
- **One DaemonSet**: `hostNetwork` (NAS export ACLs typically admit node
  IPs only), privileged for loop and dm, kubelet dir `Bidirectional`, host
  `/dev`. One CSI socket, one registrar. `CSIDriver` name
  `miami.csi.greghaskins.com` (decision 1): `attachRequired: true`,
  `podInfoOnMount: false`, `fsGroupPolicy: File`, `Persistent` lifecycle,
  `requiresRepublish: false`. Directory items ride the same settings; the
  attacher creates one `VolumeAttachment` per node for them, which is
  harmless.
- **One controller Deployment** with csi-provisioner, csi-resizer,
  csi-attacher and livenessprobe. **Decision 2: option A.** It mounts
  transports with the same transport package, is stateless, and performs
  only `truncate`, `unlink`, `mkdir`, `rm -r` (directory items) and `stat`.
  Formatting moves to first stage, so the controller never runs `mkfs`. It
  needs `hostNetwork` and `privileged` for the NFS mount; the DaemonSet is
  already privileged on every node, so the alternative (an unprivileged
  controller delegating delete and expand to a node agent through a PV
  annotation handshake) buys little and adds a cross-component protocol.
- **Transport mounts on the host mount table**: the agent mounts into a
  `hostPath` directory with `mountPropagation: Bidirectional`, so the mount
  persists across plugin restarts, loop back-files always show the host
  path, and host-side watchdogs can see them. Transports are
  reference-counted by intent and unmounted when idle for a configurable
  period (default: never).

### 4.4 Self-describing volumes

**Decision 3: opaque IDs, everything in the volume context.** Volume ID is
`<kind>:<name>`; the volume context carries the full transport config and
item parameters. `DeleteVolume` and `ControllerExpandVolume` receive only
the ID, so the controller reads the PV for those two calls (Kubernetes
1.31+ keeps the PV until delete returns; on older clusters a delete with
no PV succeeds without deleting and logs it). This maximizes flexibility:
no 128-byte constraint on the ID, a transport change (server address,
export path) is corrected by editing a PV rather than re-provisioning, and
static provisioning works, which both migration (4.10) and adopting
pre-existing images need. The controller never deletes on doubt: if the
rebuilt transport config does not contain the named file, it returns
`Unavailable`.

On disk: `<name>.img` beside `<name>.holder` under `<export>/<subdir>/`.
Capacity is `stat().Size()`; sizes round to 4 KiB; the loop sector size is
read from the ext4 superblock or LUKS2 header, never assumed. Create is
idempotent against the file; a size mismatch is `AlreadyExists`.

### 4.5 Exclusivity, takeover and fencing

1. `attachRequired: true` with csi-attacher: the attach/detach controller
   serializes attachment, and a surge pod on another node waits with a
   Multi-Attach event.
2. Holder file `<name>.holder` on the transport: `O_EXCL` create, renewal
   by `rename` or `link(2)` (transports are mounted `nolock`), age measured
   on the transport's clock through a probe file, never across node clocks.
3. Takeover when: the holder node carries the `out-of-service` taint; or
   the holder node is Ready, no longer reports the volume in
   `volumesInUse`/`volumesAttached`, and the holder is stale for 30s; or
   the transport is fenceable, the holder node is not Ready, and the holder
   is older than the takeover timeout.

The simplification is in **who decides fenceability**. The driver owns a
named NFSv3 transport profile (`nfsvers=3,proto=tcp,soft,timeo=100,
retrans=3,nolock` plus a small allowlist of NAS-specific extras such as
`noatime`, `actimeo`), so the NFS major timeout `S` is a constant of the
profile, the takeover timeout `T` is one driver flag validated against it
at startup, and `FenceAfter = T - S - margin` and `ResetAfter = T - margin`
are derived once. No per-StorageClass validation of raw option strings, no
"not fenceable because of option X" branches. A profile the driver cannot
fence (NFSv4, `hard`, `local`) is non-fenceable by name and gets taint-only
takeover. Self-fencing: ext4 shutdown ioctl at `FenceAfter`, TCP reset of
the NFS socket (`ss -K`, probed at startup) at `ResetAfter`, direct-I/O
loops always on NFS transports.

### 4.6 Failed volumes and recovery

- Detection: ext4 state from mountinfo (forced read-only, `emergency_ro`,
  shutdown). Some kernel series expose nothing in mountinfo; an optional
  active probe is decision Q8.
- Report: `VolumeCondition` abnormal; one event per consuming pod.
- Remediation: a shared package for all item kinds deletes controller-owned
  pods on this node that mount the failed volume, with a UID precondition,
  a rate limit, and a ValidatingAdmissionPolicy narrowing the grant to own
  node, controller-owned, mounting one of our volumes. Opt-out by flag.
- Re-stage in place when nothing is published. The StorageClass sets both
  `node-stage-secret-*` and `node-publish-secret-*` to the same Secret, so
  a re-stage triggered from publish has the encryption key even after a
  plugin restart.

### 4.7 Encryption

LUKS2 via cryptsetup, formatted on first stage, 4096-byte sectors, cipher
and key size from StorageClass parameters, key passed over `/dev/fd/N`
only, slot rotation when `previousKey` is present, keys never persisted.
Minimum encrypted volume size accounts for the LUKS2 header.

### 4.8 Directory items

A `directory` item is a subdirectory of the transport
(`<export>/<subdir>/<name>/`) bound straight into the pod: `ReadWriteMany`,
no loop, no holder, no fencing, the transport's own POSIX semantics. It
exists so that one driver covers both needs of a shared NAS: image items
where a workload needs a real local filesystem, directory items where
plain shared files are enough, both under the same driver-owned transport
profile and per-namespace subtree rules. Create is `mkdir`, delete is
`rm -r`, expand is a no-op (reported capacity is the export's).

The documented contract: close-to-open consistency (a reader that opens
after the writer closes sees the new content); `flock` and `fcntl` locks
are node-local on a `nolock` transport and must not be relied on across
nodes; cross-node mutual exclusion is `mkdir` or `O_EXCL` create plus a
stale-lock rule on the server's mtime. The driver ships a small
`miami lock` helper (run, hold, release, break-if-older-than) so that
wrappers around non-cooperating writers do not reinvent it.

### 4.9 Rejected: FUSE-backed Kubernetes object volumes

Considered and rejected for v1. A Secret or ConfigMap rendered as a
writable directory through FUSE, with a lock behind it, solves the
"refresh a shared credential exactly once" problem with API-server
consistency and per-object RBAC. It costs a second `CSIDriver` name (its
required spec fields contradict the image driver's), borrowed pod identity,
a persisted publish registry with boot IDs, consumer restarts on every
plugin upgrade, and a content cap, for one or two small files. The same
problem is served by a directory item plus a locking wrapper (4.8). If a
Secret must remain the source of truth, a sidecar mirroring the file into
it is simpler than a filesystem. This can be revisited if a consumer
appears that needs the API-server guarantees specifically.

### 4.10 Migration from an existing file-backed driver

**Decision 4: a rename tool.** Per volume, with the workload scaled to
zero: set the old PV's `reclaimPolicy` to `Retain`; delete the PVC and the
PV; rename the image file to the new layout on the transport; create a
static PV for `miami.csi.greghaskins.com` with the same capacity, the
volume context the new driver expects, and a `claimRef` to the PVC name;
recreate the PVC; scale up. The tool does the Kubernetes side and prints
the rename it expects, or runs it over the mounted transport when given
access. Both drivers can be installed side by side during the window;
nothing forces a flag day.

### 4.11 Observability and security posture

- Prometheus `/metrics` on both roles: staged volumes by kind and
  transport, holder renew latency and failures, transport health, fenced
  count, failed volumes, remediation actions, reconcile duration.
- Events on the PVC and pod for every state change a human would ask about.
- Every API write the node agent can perform is enumerated in RBAC and
  mirrored by a VAP.
- Image: slim Debian for the image role (`e2fsprogs`, `util-linux`,
  `cryptsetup`, `nfs-common`, `iproute2`; build-assert `netbase`),
  distroless for roles that need no shell. CI mounts NFSv3 for real.

## 5. Future directions the design must not preclude

These are not v1 scope. They are listed so that v1 choices (item kinds,
transport abstraction, topology handling, ID format) leave room for them.

### 5.1 Node-local tiers: persistent and scratch

Two tiers share one mechanism, an **image item on the `local` transport
pointed at the node's own disk** (same loop/ext4 code path, a real size
cap, which matters when root and data share one disk):

- **local-persistent**: the PV advertises node topology; the volume is
  pinned to its node like a local-path volume, but with a quota, optional
  LUKS2, and the same observability. Candidate replacement for a
  local-path provisioner under databases that handle their own
  replication.
- **scratch**: persistent-but-recreatable. No topology; if a pod lands on
  a node without the image, stage creates a fresh empty one, and the old
  copy is garbage-collected by the converge loop using a PV annotation
  naming the current node. No holder, no fencing. Excluded from any backup
  that covers the shared transport, by construction, because it never
  touches it. Optional `seed` parameter to warm from a transport path.

What v1 must keep open: the `local` transport is a first-class transport
(not a test shim); item parameters include a topology mode; the reconciler
can create an image at stage time (already true for encrypted volumes).

### 5.2 S3-compatible object service

An S3 endpoint whose durability comes from a csi-miami image volume, so it
inherits encryption, exclusivity, takeover and whatever backup covers the
transport. Shapes considered: reuse an existing single-node S3 engine on a
driver-provisioned PVC and own only bucket provisioning (through COSI
`BucketClaim`/`BucketAccess`) and the operational wrapper; or write the S3
front end here with buckets as directories inside one image. Reuse is the
expected path. What v1 must keep open: nothing special beyond a reliable
RWO volume with takeover; the service is a consumer, not an item kind.

## 6. Testing strategy

- Unit tests through an `exec.Runner` seam, weighted toward table tests of
  the reconciler against fake observers rather than argv pinning.
- `envtest` for API semantics (PV lookups, scratch GC, admission policy
  CEL).
- Root smoke under `unshare --mount`, including plugin replacement and a
  simulated reboot (boot ID change).
- kind e2e in three transport variants (local, NFSv3, NFSv4 to prove the
  non-fenceable path) with fault injection: NFS server restart, partition,
  plugin restart, force detach, surge rollout.
- csi-sanity for both driver names.

## 7. Non-goals

- RWX on image items; only directory items are many-node.
- Online expansion, snapshots, clones, block mode, fsTypes other than ext4.
- Any cross-node locking guarantee on directory items beyond what `mkdir`
  and `O_EXCL` give.
- Nodes that cannot reach the NAS from the host network.

## 8. Decisions

Recorded:

1. **Driver name**: `miami.csi.greghaskins.com`. One name; item kind is a
   StorageClass parameter. (The owner's first choice was an `image.`
   prefix, which only made sense with a second driver name; confirm.)
2. **Controller shape**: option A, stateless and privileged, never formats.
3. **Volume ID**: opaque; all config in the volume context; PV lookup for
   delete and expand. Chosen for flexibility and compatibility.
4. **Migration**: rename tool with static PVs (4.10).
5. **License**: GPL-3.0. The sidecars are separate images and the Go
   dependencies (gRPC, CSI spec, kernel-interface libraries) are
   Apache-2.0 or BSD, which flow into GPLv3 without conflict.

Pending:

6. **FUSE-backed object volumes**: rejected in favour of directory items
   plus a locking wrapper (4.8, 4.9); confirm.
7. **A Kubernetes Lease alongside the transport-clock holder**: proposed
   no; the transport clock covers the partition that matters (NAS reachable,
   API server not).
8. **Transport profile ownership**: driver-dictated named profiles
   (proposed) versus raw option strings in StorageClasses.
9. **Active failed-volume probe** for kernels whose mountinfo shows
   nothing, or document the gap.
10. **Whether directory items replace a separate NFS CSI driver** in the
    first deployment or later.
11. **Which node-local tier comes first** once section 5.1 is scheduled,
    and whether it displaces an existing local-path provisioner.

## 9. Milestones (proposal)

1. **M1, image items on NFSv3 and local**: node reconciler, transports on
   the host mount table, self-describing IDs, holder plus attach
   exclusivity, driver-owned transport profile with fencing, LUKS2, offline
   expand, metrics, kind e2e on NFSv3.
2. **M2, recovery**: failed-volume detection, shared remediation package
   and VAP, re-stage via the publish secret, canaries, migration tooling.
3. **M3, directory items** and the `miami lock` helper.
4. **Later**: node-local tiers (5.1), then the S3 service (5.2).
