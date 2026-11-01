# csi-miami architecture brief

**Status:** Draft, decisions 1 to 27 recorded (section 9)
**Created:** 2026-10-30
**Updated:** 2026-10-31

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
| `ReadWriteMany` on an image volume, re-exported from the holder node | milestone 4, section 4.9 |
| Writable cross-node Secret directory through FUSE | rejected, section 4.10 |
| Scratch (not backed up): a StorageClass whose transport path is outside the backup set | yes, section 5.1 |
| Node-local persistent volumes with a move operation | future, section 5.2 |
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
12. **After an NFS server restart there is a window of stale handles.** In
    that window the transport's directories answer `ESTALE`. Directories
    recover when the server is back; a file held open across the window may
    not. The driver reports the condition (transport unavailable, with the
    health gauge and event T14 adds) and does not unmount or remount in M1;
    what remediation looks like is 4.6's question for M2, which also records
    what the restart end-to-end test saw of a file held open.
13. **An external watchdog that lazy-unmounts NFS mounts is a failure source,
    not a safety net.** A lazy unmount detaches the mount from every running
    pod's view while the processes keep working on a detached tree, and it
    hides the mount from monitoring. A host-side tool must leave the
    transports root and the stage root alone.
14. **Read-only is a publish property, never a health signal.** NFS does not
    flip a mount read-only when the server fails; a read-only mount that is
    unhealthy shows as errors and timeouts, so health is judged by asking the
    server, never by the mount's flags.
15. **Ownership changes by the kubelet fail on every file of a squashing
    export and flood the logs.** A root-squashed export refuses `chown`, so a
    recursive fsGroup change reports one error per file, and on a large share
    it also takes minutes. The CSIDriver's `fsGroupPolicy` is therefore
    `ReadWriteOnceWithFSType` (decision 27), which leaves directory items
    alone.
16. **A mount-health signal belongs in the first deployment.** "Is it
    mounted" is not "does the server answer": a dead server leaves the mount
    in place. The signal that matters is a bounded probe of each transport
    and a metric for its outcome; T14 adds it.
17. **Transport lifetime and node drain interact.** A cluster whose drains
    expect no NFS mounts left on a node sets a finite `--transport-idle` in
    its overlay so idle transports unmount; the driver's default stays
    "never" (4.3), because remounting on demand costs a first-use delay.

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
- **The driver owns the mount options.** Users name a server and an
  export; the driver mounts with the one option set it knows how to fence.
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
pod ──bind──▶ Mount (stage path)      ext4 over loop (+ dm-crypt)    (no mount at the stage path:
                                                                  the pod binds the subdirectory)
                    ▲                         ▲                             ▲
                  Item                   ImageItem                    DirectoryItem
                    ▲                         ▲                             ▲
                Transport               nfs3 / smb / local        nfs3 / local / export
                                                                  (export = a holder node's
                                                                   re-exported image, 4.9)
```

- **Transport**: yields a directory on this node. `nfs3` (server, export,
  driver-owned options), `local` (a host path; also how a pre-mounted SMB or
  FUSE directory is used), `export` (another node's re-exported image,
  NFSv4, section 4.9), later `smb`. Identified by a short hash of its
  canonical config, mounted once per node at `<root>/transports/<id>`.
- **Item**: a thing inside a transport directory that becomes a mount.
  `image` (sparse ext4 `.img`, optional LUKS2, single-node) and
  `directory` (a subdirectory bound straight into the pod, many-node,
  with the transport's own semantics).
- **Mount**: the staged filesystem plus its bind mounts into pods. An image
  item has one, at the stage path. A directory item has none there: its pods
  are bound straight from the directory in the transport (decision 25).

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
  mounted at the stage path (images only; a directory item makes no mount
  there) → binds present, of the stage path for an image and of the item
  directory in the transport for a directory. For each observed artifact
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
  `podInfoOnMount: false`, `fsGroupPolicy: ReadWriteOnceWithFSType`
  (decision 27), `Persistent` lifecycle,
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
- **Transport mounts on the host mount table**: the agent mounts under the
  kubelet directory (`<kubelet-dir>/plugins/miami.csi.greghaskins.com/transports`,
  decision 27), which is a `hostPath` with `mountPropagation: Bidirectional`,
  so the mount persists across plugin restarts, loop back-files always show
  the host path, and the mounts are visible in the kubelet's mount namespace. Transports are
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
export path) is a static-PV recreation (4.11's tool) rather than an edit,
because a PV's volume source is immutable and the volume context lives in it,
and static provisioning works, which both migration (4.11) and adopting
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
4. **Two clocks must agree before any timed takeover.** The holder's age
   is measured on the transport's clock (probe-file mtime minus holder
   mtime), and the taker must additionally have observed the holder's
   renewal counter unchanged for the full period on its own monotonic
   clock. The second condition makes a server clock jump harmless: it
   cannot make every live holder look stale at once. **Decision 7: no
   Kubernetes Lease beside the holder.** In the partition that matters
   (holder reaches the NAS but not the API server) a Lease expires while
   the holder keeps writing, so a Lease-driven takeover would corrupt the
   image; in every other partition the holder plus Node status already
   decide correctly.

The simplification is in **who decides fenceability**. **Decision 8:
the driver owns mount options.** The driver mounts NFSv3 with one fixed option set
(`vers=3,proto=tcp,softerr,timeo=100,retrans=3,nolock,lookupcache=positive,
actimeo=30,noatime`; the reasons are in the M1 plan, section 8), so the NFS
major timeout `S` is a constant, the takeover timeout `T` is one driver
flag validated against it at startup, and `FenceAfter = T - S - margin` and
`ResetAfter = T - margin` are derived once. No per-StorageClass option
strings, no `hard` variant, no "not fenceable because of option X"
branches. `local` transports are non-fenceable by kind and get taint-only
takeover. Self-fencing: ext4 shutdown ioctl at `FenceAfter`, TCP reset of
the NFS socket (`ss -K`, probed at startup) at `ResetAfter`, direct-I/O
loops always on NFS transports.

### 4.6 Failed volumes and recovery

- Passive detection: ext4 state from mountinfo (forced read-only,
  `emergency_ro`, shutdown). Some kernel series expose nothing there, so
  it is never the only signal.
- **Active probe (decision 9).** Every staged image is probed on a timer,
  each probe in its own goroutine under a deadline: an `O_DIRECT` read of
  the ext4 superblock through the loop device (bypasses the page cache, so
  a stale NFS handle or dead transport surfaces as `EIO` or a timeout
  rather than a cached success) and a `statfs` on the stage mount. Each
  transport is probed the same way through its clock probe file. Two
  consecutive failures mark the volume failed; a probe that exceeds its
  deadline counts as a failure and its goroutine is abandoned, never
  joined, because a hung NFS call does not return. Probe results are
  exported as metrics so a wedge is visible before a pod notices.
- Report: `VolumeCondition` abnormal; one event per consuming pod.
- **What M1 does about a transport that does not answer (decision 28).** It
  reports and does not remediate. Each pass asks every transport the intents
  reference whether it answers (`statfs` of its mount directory under the stat
  deadline), exported as `miami_transport_up{kind,id}`; a stat of a volume's
  item directory that returns ESTALE, times out or is busy is the same
  condition, counted in `miami_volume_condition{condition="transport-unavailable"}`
  and raised once as a `TransportUnavailable` event on the PersistentVolume.
  Nothing is unmounted, remounted or killed on account of it, and the next pass
  looks again; remediation is the work of this section's later steps, which M1
  does not build.

  What the NFS server restart end-to-end test observed (the server container
  stopped and started, its export on a docker volume, the `softerr` profile, on
  both architectures): the gauge went to 0 and the condition to
  `transport-unavailable` within 12 seconds of the stop, and both recovered
  within 26 seconds of it, 12 seconds after the server was back; the transport
  and publish mounts kept their mount IDs throughout; the writing pod's numbered
  sequence had no gap. **A file a pod held open across the restart** (fsynced
  every second) kept working: the export keeps its file system and `fsid=0`, so
  the client's file handles stay valid, nothing returned ESTALE, and with the
  server back after 14 seconds no write on the held descriptor failed. The wait
  of a soft mount is bounded by its major timeout, 40 seconds for the profile
  (`timeo` of 10 seconds times `retrans` 3 plus 1, which the first run of the
  test matched): in that run the server was held down for about 40 seconds,
  and the write in flight on the held descriptor failed with ETIMEDOUT
  ("Connection timed out", returned by the fsync), its data was not in the file,
  and later writes through the same descriptor succeeded. So an application that
  holds a file open across a server restart must check the errors of `fsync` and
  `close` and write again; the driver cannot and does not mask it.
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
`rm -r`, expand is a no-op (reported capacity is the export's). Staging
records the volume and checks that the directory exists; nothing is mounted
at the stage path, and each publish is a bind of the directory (decision 25).

The documented contract: close-to-open consistency (a reader that opens
after the writer closes sees the new content); `flock` and `fcntl` locks
are node-local on a `nolock` transport and must not be relied on across
nodes; cross-node mutual exclusion is `mkdir` or `O_EXCL` create plus a
stale-lock rule on the server's mtime. The driver ships a small
`miami lock` helper (run, hold, release, break-if-older-than) so that
wrappers around non-cooperating writers do not reinvent it.

### 4.9 `ReadWriteMany` on an image: re-export from the holder node

Two nodes can never loop-mount one ext4 image, so the only honest
many-node access to an image is for one node to stage it and serve it to
the others. An image item whose StorageClass sets `export: nfs4` accepts
`MULTI_NODE_MULTI_WRITER` and works like this:

- **Holder node**: stages the image exactly as in 4.5 (loop, optional
  dm-crypt, ext4, holder file), then exports the stage mount through the
  kernel NFS server: one export per volume with its own `fsid`, NFSv4 only
  (no portmapper, no mountd on the wire), restricted to node addresses.
  The holder file records the export address.
- **Other nodes**: stage the volume by mounting the holder's export as an
  `export` transport (NFSv4.2, driver-owned options) and bind it into pods.
  Discovery is the holder file on the shared transport; the data path has
  no API-server dependency.
- **Semantics**: pods on the holder node get full local ext4 semantics;
  pods elsewhere get NFSv4 close-to-open semantics, with mode bits intact
  (the server is a Linux kernel, not NAS firmware) and real cross-node
  locking. The kernel NFS server takes its locks in the same lock table as
  local processes, so holder-local pods and remote pods lock coherently
  against each other.
- **Lifecycle**: the holder keeps the image staged and exported while any
  remote node has it mounted, tracked by per-node client markers
  (`<name>.client.<node>`) beside the holder file; its own pods leaving
  does not release it. When the holder dies, a client node promotes
  itself through the normal holder acquisition and fencing path,
  re-exports, and every other client remounts when it sees the holder
  file change. Recovery is tear-down-and-reattach, already the normal
  path.
- **Cost**: remote writes cross the network twice, and the holder node is
  load-bearing for that volume until a takeover completes (bounded by the
  takeover timeout). Documented, and acceptable where the alternative is
  losing POSIX semantics.

In the layer vocabulary the holder's export is just another transport, so
an RWX image volume is a directory item on an `export` transport; the CSI
surface hides that and presents one PVC.

**Decision 11: the kernel NFS server**, on these conditions. Consumers see
a normal PVC; the only difference from an RWO image is `export: nfs4` on
the StorageClass and `ReadWriteMany` on the claim. Nothing in a pod
manifest changes. Security: exports are NFSv4-only, per-volume `fsid`,
limited to the cluster's node addresses (a CIDR flag, or the Node list),
`no_root_squash` because pods legitimately run as root inside their own
volume, and the export disappears the moment the holder unstages. The
trust boundary is "nodes are trusted", the same boundary the NAS exports
already draw; `sec=sys` means a trusted node vouches for UIDs. Host-network
traffic is not subject to NetworkPolicy, so port 2049 on nodes should be
firewalled to the node network at the host level, and the DaemonSet runs
`rpc.nfsd` with `-N 2 -N 3`. Node prerequisites: the `nfsd` kernel module
and the server's userspace (`rpc.nfsd`, `rpc.mountd`, `exportfs`) available
to the DaemonSet. A node without them can still be a client.

### 4.10 Rejected: FUSE-backed Kubernetes object volumes

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

### 4.11 Migration from an existing file-backed driver

**Decision 4: a rename tool.** Per volume, with the workload scaled to
zero: set the old PV's `reclaimPolicy` to `Retain`; delete the PVC and the
PV; rename the image file to the new layout on the transport; create a
static PV for `miami.csi.greghaskins.com` with the same capacity, the
volume context the new driver expects, and a `claimRef` to the PVC name;
recreate the PVC; scale up. The tool does the Kubernetes side and prints
the rename it expects, or runs it over the mounted transport when given
access. The rename keeps the `pvc-<uuid>` name (decision 22). Both drivers
can be installed side by side during the window; nothing forces a flag day.

### 4.12 Observability and security posture

- Prometheus `/metrics` on both roles: staged volumes by kind and
  transport, holder renew latency and failures, transport health, fenced
  count, failed volumes, remediation actions, reconcile duration.
- Events on the PVC and pod for every state change a human would ask about.
- Every API write the node agent can perform is enumerated in RBAC and
  mirrored by a VAP.
- Image and supply chain: section 7.

## 5. Storage tiers

### 5.1 Scratch is a location, not a kind

"Persistent but recreatable" state needs no special item kind. What
decides whether a volume is backed up is where its file lives on the
transport, so scratch is **a StorageClass whose transport path sits outside
the backup set** (a sibling export or subtree that the NAS backup job does
not cover). Same image items, same exclusivity, same encryption if wanted;
only the location differs. The driver's one obligation is to make the
location an explicit, visible StorageClass parameter so that "is this
backed up?" can be answered by reading the class. **Decision 12.**

### 5.2 Node-local persistent volumes

A close replacement for a local-path provisioner, with only the features
actually needed:

- An **image item on the `local` transport** pointed at the node's own
  disk: a real size cap (root and data commonly share one disk), optional
  LUKS2, the same observers and metrics. A directory item on the same
  transport is also possible and is exactly what a local-path provisioner
  gives today; the image form is the recommended default.
- **Topology**: `CreateVolume` returns the chosen node as accessible
  topology, so the PV carries node affinity and the scheduler pins pods
  to it. `WaitForFirstConsumer` picks the node from the first pod.
- No holder, no fencing, no re-export: single-node by construction.
- **Move without fuss**: `miami move <pvc> --to <node>` with the workload
  scaled to zero. The source node exports its local transport directory
  through the same NFSv4 re-export path as 4.9; the target node mounts it
  as an `export` transport and copies the image sparsely; the tool then
  recreates the PV with the new node's topology and the same `claimRef`
  (PV node affinity is immutable, so this is the same Retain, delete,
  recreate dance as migration, 4.11) and removes the source copy. One
  command, no external copy host, no NAS round trip.
- When the node is gone for good, the same tool can recreate the PV
  empty on another node, for workloads that rebuild their own state.

### 5.3 S3-compatible object service

An S3 endpoint whose durability comes from a csi-miami image volume, so it
inherits encryption, exclusivity, takeover and whatever backup covers the
transport. Shapes considered: reuse an existing single-node S3 engine on a
driver-provisioned PVC and own only bucket provisioning (through COSI
`BucketClaim`/`BucketAccess`) and the operational wrapper; or write the S3
front end here with buckets as directories inside one image. Reuse is the
expected path. Nothing in v1 needs to change for it; the service is a
consumer, not an item kind.

## 6. Testing strategy

- Unit tests through an `exec.Runner` seam, weighted toward table tests of
  the reconciler against fake observers rather than argv pinning.
- `envtest` for API semantics (PV lookups, PV recreation in move and
  migration, admission policy CEL).
- Root smoke under `unshare --mount`, including plugin replacement and a
  simulated reboot (boot ID change).
- kind e2e in three transport variants (local, NFSv3, NFSv4 to prove the
  non-fenceable path) with fault injection: NFS server restart, partition,
  plugin restart, force detach, surge rollout.
- csi-sanity for both driver names.

## 7. Image, supply chain and CI

### 7.1 A static binary plus two tools, one multi-stage build

Two external tools stay in the image, because reimplementing either is a
data-loss risk not worth taking: `e2fsprogs` (`mkfs.ext4`, `e2fsck -p` on
every stage, `resize2fs`) and `cryptsetup` (LUKS2 format, open, resize,
key-slot add and kill). Everything else the driver does reaches the kernel
directly, so there is no shell and no helper whose absence can break a
mount:

- **NFS**: `mount(2)` with a kernel-ready option string (`addr=` resolved
  by the driver, `vers=3`, the profile's options). The kernel performs the
  NFSv3 MOUNT request itself, so no `mount.nfs`, no `rpcbind`, no
  `/etc/services`. Bind mounts and `umount2` are syscalls; mounted state is
  read from `/proc/self/mountinfo`.
- **Loop devices**: `LOOP_CTL_GET_FREE`, `LOOP_CONFIGURE` (direct I/O,
  sector size), `LOOP_SET_CAPACITY`, `LOOP_CLR_FD`; observed through sysfs.
- **Fencing**: `EXT4_IOC_SHUTDOWN` and a netlink `SOCK_DESTROY`; no `ss`.
- **Probes**: `O_DIRECT` reads and `statfs`, syscalls.
- **Device nodes**: dm-crypt mappings are created by `cryptsetup`, which
  makes its own `/dev/mapper` node when no udev is present.

**The image (decision 13).** One multi-stage Dockerfile, built with
`buildx`, and nothing published but the driver image. Stage one is a
digest-pinned musl distribution that installs `e2fsprogs` and `cryptsetup`;
a script computes the shared-library closure of exactly the binaries the
driver calls. Stage two is `scratch` with that closure and the Go binary,
cross-compiled from `TARGETARCH` with `CGO_ENABLED=0`. No shell, no package
manager, no `busybox`; roughly 10 to 15 MB per architecture. Each
architecture is built natively on its own runner (`ubuntu-latest` for
amd64, GitHub's arm64 Ubuntu runner for arm64), so nothing is ever
emulated, and a separate job combines the two per-arch tags into the
multi-arch manifest with `buildx imagetools create`. `buildx` emits the
SBOM and provenance attestations per architecture. The controller is the
same image with a different subcommand.

Updates are ordinary dependency bumps: the dependency bot moves the pinned
base digest, CI rebuilds and scans, the PR merges. There is no separately
maintained base image, no schedule and no second pipeline.

**Why not `ko`.** `ko` cannot install packages, so keeping the real tools
under `ko` means publishing and maintaining a tools base of our own. A
single Dockerfile gives the same shell-free image with one artifact and
one update path.

**Rejected: borrowing the tools from the host.** Running the host's
`e2fsck` and `cryptsetup` in a child process chrooted into a read-only
host-root mount works and keeps the image to one binary, but it demands
host packages, which almost no CSI driver does, ties correctness to a tool
version that varies per node and per OS upgrade, and means CI cannot test
the bytes that run. It remains an escape hatch, not the default.

Milestone 3's kernel NFS re-export needs the kernel's export table fed;
that is either the host's `nfs-utils` or a small Go responder for the
kernel's export upcall channel, decided then.

Runtime posture:

- Node DaemonSet: `privileged: true` is unavoidable (kubelet allows
  `Bidirectional` mount propagation only for privileged containers, and
  loop and dm need device access), so the rest is tightened:
  `readOnlyRootFilesystem: true` with an `emptyDir` for the few paths that
  must be writable, no `hostPID`, `hostNetwork` only because NAS ACLs admit
  node addresses, `automountServiceAccountToken` true only for the node
  role, resource requests and limits set.
- Controller Deployment: not privileged. `capabilities: add: [SYS_ADMIN]`
  for the mount, `drop: [ALL]` otherwise, `readOnlyRootFilesystem`,
  `seccompProfile: RuntimeDefault` (the default profile permits `mount`
  under `CAP_SYS_ADMIN`), `hostNetwork` for the NAS ACL, `runAsUser: 0`
  because `mount(2)` requires it.
- Sidecar images (provisioner, resizer, attacher, registrar, livenessprobe)
  are pinned by digest in the kustomize base, with automated bump PRs.

### 7.2 Supply chain

- Images are signed with keyless cosign from the release workflow's OIDC
  identity, carry `buildx`'s SBOM and provenance attestations, and the
  kustomize base references them by tag with the digest recorded alongside.
- `govulncheck` on every PR; container scanning on every built image;
  dependency updates for Go modules, GitHub Actions and sidecar images by
  automated PRs. Actions are pinned by commit SHA, and every workflow
  declares the least `permissions:` it needs.
- Multi-arch (`linux/amd64`, `linux/arm64`) is two native builds and one
  manifest job; nothing runs under emulation, and the arm64 binary is
  smoke-tested on arm64 hardware before it is published. GitHub's hosted
  arm64 runners are free for public repositories and billed for private
  ones; the repository is public, so they cost nothing.

### 7.3 CI: fast by parallelism, robust by running the real thing

Every PR runs the full set below as parallel jobs; nothing waits for
`main`. The target is under ten minutes wall clock.

| Job | What it proves | Runs on |
|---|---|---|
| `secret-scan` | gitleaks over the tree, every commit's content and message in the PR range (specific findings may be exempted through `.gitleaksignore`), and the branch name, with the config held in a repository secret; a second workflow scans issue, PR, review, comment, discussion and release text as it is posted from accounts with write access (bot accounts by an owner-held list of account IDs) and removes the comment or clears only the field that matches | amd64 |
| `static` | `gofmt -s`, `go vet`, `golangci-lint`, `go mod tidy` drift, `govulncheck` | amd64 |
| `unit` | `go test -race -shuffle=on ./...` with coverage | amd64 |
| `envtest` | PV lookups, PV recreation, admission policy CEL against a real API server | amd64 |
| `build` (matrix) | native `buildx` from the one Dockerfile; per-arch image pushed as `:x-<hash>-<arch>` only when that content tag is missing, and run from the registry on every PR | amd64 and arm64 |
| `smoke` (matrix) | root on the runner: loop devices, ext4, LUKS2, `unshare --mount` plugin replacement, simulated reboot | amd64 and arm64, from that arch's image |
| `sanity` (matrix) | csi-sanity against the binary | amd64 and arm64 |
| `e2e` matrix `local`, `nfs3`, `nfs4` | kind cluster, real NFS server container (kernel server for `nfs3` and `nfs4`), fault injection: server restart, partition, plugin restart, force detach, surge rollout, holder kill with remote writers | amd64; `nfs3` also on arm64 |
| `manifest` | creates the multi-arch index `x-<hash>` from the digests the build legs ran, and verifies it lists exactly those two platforms; runs on every PR | amd64 |
| `publish` | aliases the commit sha to the verified index; later, signing; `main` only | amd64 |
| `ci-ok` | a single required check that depends on all of the above | nothing |

Rules that keep it honest:

- **One required check.** Branch protection requires only `ci-ok`. Path
  filters may skip heavy jobs for docs-only changes, but `ci-ok` still
  runs and still passes, so a skipped job can never masquerade as a pass
  and a filtered job can never block a merge.
- **No retries on failure.** A flaky test is a bug. Fault-injection tests
  use explicit hooks (a debug endpoint that pauses renewals, a controllable
  clock) rather than sleeps, and every wait polls a condition with a
  deadline.
- **Build once per architecture, test everywhere.** Each architecture's
  image is built at most once per content hash and every downstream job for that
  architecture pulls `x-<hash>-<arch>` from the registry, so smoke, sanity
  and e2e all test the bytes that would ship, on the hardware they would
  ship to.
- **Superseded runs are cancelled** for pull requests: the next push to the
  same PR cancels the previous run. Runs on `main` are keyed by commit and
  never cancelled. Job timeouts are set, and on failure every job uploads mountinfo, `losetup`,
  dm state, kernel log excerpts and the plugin's logs as artifacts.
- `make check` runs the same targets locally with the same flags, so a
  green local run predicts CI.
- Secret scanning runs as git hooks locally (`pre-commit`, installed by
  `mise install`) with the same scripts CI uses, fed the config from an
  environment variable instead of the secret, and refuses to run without it.
- On `main` and nightly: `manifest` runs on every PR; `main` runs `publish`
  (the sha alias) and, later, signing and release. Also any variants that
  were path-filtered, and the release pipeline (PR-driven: cut-release opens a
  release PR, merging it tags, runs the two native builds and the manifest
  job, signs and publishes).

## 8. Non-goals

- Mounting one image on two nodes at once; many-node access to an image is
  only ever through the holder's export (4.9).
- Online expansion, snapshots, clones, block mode, fsTypes other than ext4.
- Any cross-node locking guarantee on directory items beyond what `mkdir`
  and `O_EXCL` give.
- Nodes that cannot reach the NAS from the host network.

## 9. Decisions

Recorded:

1. **Driver name**: `miami.csi.greghaskins.com`. One name; item kind is a
   StorageClass parameter. (The owner's first choice was an `image.`
   prefix, which only made sense with a second driver name; confirm.)
2. **Controller shape**: option A, stateless and privileged, never formats.
3. **Volume ID**: opaque; all config in the volume context; PV lookup for
   delete and expand. Chosen for flexibility and compatibility.
4. **Migration**: rename tool with static PVs (4.11).
5. **License**: GPL-3.0. The sidecars are separate images and the Go
   dependencies (gRPC, CSI spec, kernel-interface libraries) are
   Apache-2.0 or BSD, which flow into GPLv3 without conflict.

6. **FUSE-backed object volumes**: rejected in favour of directory items
   plus a locking wrapper (4.8, 4.10).
7. **No Kubernetes Lease** beside the transport-clock holder; timed
   takeover requires both the transport-clock age and the taker's own
   observation period (4.5).
8. **Driver-owned mount options**: one fixed NFSv3 option set, no `hard` variant (4.5; M1 plan section 8).

9. **Active failed-volume probe**: yes, in addition to passive detection
   (4.6).
10. **Directory items are the first feature** and ship in the first
    deployment; they prove the transport, intent and reconciler plumbing
    before loop devices enter the picture, and they replace a separate NFS
    CSI driver.
11. **Kernel NFS server** for RWX images, with the security conditions in
    4.9 and no change to consumer manifests.
12. **Scratch is a StorageClass location**, not an item kind (5.1).
    **Node-local persistent** is an image item on the `local` transport
    with topology and a move operation (5.2), scheduled after RWX images.

13. **Image**: one multi-stage Dockerfile built with `buildx`: `scratch`
    plus the static Go binary, `e2fsprogs`, `cryptsetup` and their library
    closure from a digest-pinned builder; no shell, no mount helpers, no
    separately maintained base image. Built natively per architecture on
    amd64 and arm64 runners, never emulated, combined by a manifest job
    (7.1, 7.3).
14. **Syscalls and ioctls over shell-outs** for NFS mounts, loop devices,
    fencing and probes (7.1), each proven on real NFSv3 in CI.
15. **CI shape**: parallel jobs on every PR including smoke, sanity and
    the kind e2e matrix, one required `ci-ok` check, no retries, build once
    (7.3).

16. **`e2fsck -p` stays on the stage path** and `mkfs.ext4` and
    `resize2fs` are the real tools; no embedded template, no fsck-free
    mounts (7.1).
17. **LUKS2 through `cryptsetup`**, not a Go implementation (7.1).
18. **Merge commits, not squash**, on `main`: the merge commit message is
    the pull request title only; squash and rebase merges are disabled.
    Squash forces a rebase of every stacked branch on each merge; merge
    commits keep branch history and make stacking cheap. The PR body never
    enters git history either way.
19. **Content-addressed images**: CI tags images by a hash of the build
    inputs (the Dockerfile, its allowlisted context, the module files, and
    the scripts that hash and build). A pull request builds and pushes
    `x-<hash>-<arch>` only when that tag does not exist, every run proves
    the image starts, and the multi-arch index `x-<hash>` is created from
    exactly the two digests the runs started and verified. A push to `main`
    normally only adds the commit sha as an alias of that index; when
    `main`'s content hash was never built (two PRs merged in sequence can
    produce a tree no PR run built), the `main` run builds it like any other
    run. The binary's version string is the content hash, so identical
    inputs have one identity across commits. Fork pull requests are
    unsupported because the push needs package write access. Trust
    boundary, accepted: anyone who can push a branch can publish bytes under
    a content tag, which is the same set of people who can merge; a `main`
    run re-runs the image before aliasing, and the alias is created from the
    index that `main`'s own run verified. Provenance describes the run that
    built the tag, which may be a pull request merge ref.
20. **One directory per volume**: a directory item lives at
    `<transport>/<subdir>/<name>`, where `subdir` is an optional per-class
    prefix built from `${pvc.namespace}` and `${pvc.name}` and `name` is
    always the volume's own name from the volume ID. No template can make
    two volumes share a directory or nest one inside another: elements
    shaped like a PV name are rejected. The plan's `<transport>/<subdir>`
    with a `${pv.name}` token is withdrawn.
21. **IPv4 only**: transports are reached over IPv4; an IPv6 server
    literal is rejected and a host name must resolve to an IPv4 address.
    The target environments have no IPv6 and the driver carries no IPv6
    code path.
22. **Volume names are provisioner-shaped**: a volume name is always
    `pvc-<uuid>`; the driver refuses any other name at every boundary, so no
    subdir element can ever be a volume and no volume can nest in another.
    Static PVs (migration, adoption) keep or receive such names.
23. **Local volumes are created by the node**: the controller runs on one
    node and a `local` transport's directory belongs to another, so for
    `local` the controller's `CreateVolume` validates and returns the volume
    context and the node topology and touches no file system; the node
    creates the item directory at first stage when it is missing.
    `DeleteVolume` for a `local` volume cannot reach the node's disk: it
    succeeds, logs at warning, and the directory remains on the node until
    the node-local tier (M4) deletes it, a documented limitation. `nfs3`
    volumes are created and deleted by the controller.
24. **Deploy shape (M1, T9)**: the controller has no resizer until M2, so
    its sidecars are provisioner, attacher and livenessprobe. Every driver and
    sidecar container drops all capabilities except the controller's `miami`
    container, which adds `SYS_ADMIN`; all set `allowPrivilegeEscalation:
    false` except the privileged node container, where the field cannot
    be false. The node's `miami` container is `privileged: true` with
    `readOnlyRootFilesystem: true` and `seccompProfile: RuntimeDefault` (the
    API server accepts it; the runtime ignores the profile for a privileged
    container). The sidecars run as root so they can open the socket the driver
    creates. An nfs3 mount works with `allowPrivilegeEscalation: false` on the
    controller (verified by the T10 e2e: CreateVolume and DeleteVolume make and
    remove the directory on a kernel NFS export through it). The namespace carries
    the `privileged` Pod Security level. The deploy check runs on kind with a
    control plane and two workers (the shape the end-to-end tests need), from a
    node image pinned by digest; the DaemonSet tolerates every taint, so it
    runs on all three nodes.

25. **A directory item makes no stage mount**: the NFS client reports `/` as
    the root of every mount of an export, a bind of a subdirectory included
    (it carries the directory in the mount source instead). Before it calls
    `NodeUnstageVolume`, kubelet lists every other mount with the same
    device and root as the staging path and refuses while one exists, so a
    bind of the item directory at the stage path matched the transport and
    every other volume's bind and the stage could never be unstaged (the
    VolumeAttachment stayed, the PV delete hung). Kubelet's search finds no
    references for a staging path that is not itself a mount point, and the
    CSI attacher calls stage and unstage without checking that the path is
    mounted. So `NodeStageVolume` still records the volume (transport
    mounted, item directory present) but mounts nothing at the stage path,
    and each publish target is bound directly from the volume's directory in
    the transport. The item reports `PublishSource` for this: the directory
    for a directory item, the stage path for an image item (M2). Anything
    mounted at a directory volume's stage path is `ErrStageConflict`, reported
    and left, since the driver cannot show it made it. The transport model
    (one host-visible mount per server, adoption, deadlines, garbage rules)
    is unchanged. A stage bind from before this decision is recognised as
    the driver's own and removed on the first pass.

26. **Kind end-to-end tests (M1, T10)**: the tests in `test/e2e` are Go
    (build tag `e2e`), driven through the API with client-go against a
    cluster the harness (`hack/e2e-kind.sh`, reached from `make e2e
    E2E_MODE=kind`) has created; they never create one. The cluster is the deploy
    check's: one control plane and two workers from the digest-pinned node
    image, the e2e overlay, the driver image loaded and never pulled. For
    `nfs3` the kernel NFS server of `hack/nfs-server` runs on the docker
    network the kind nodes are on and its IPv4 address goes into the overlay.
    Test pods use one image, busybox from the Kubernetes end-to-end test
    images, pinned by the digest of its multi-arch index. **The reboot
    stand-in** is `docker restart` of a kind worker (one the controller is not
    on): every mount in the node's namespace is gone, the kubelet and
    containerd start from nothing and the pods' containers are recreated,
    while the node's disk survives, as a machine's does; it exercises the
    kubelet's and the driver's recovery without a VM. The tests shell out to
    docker for exactly four things: the restart, a node's mount table
    (`docker exec <node> cat /proc/1/mountinfo`), a node's files (the intent
    record, a local volume's directory) and a directory on the NFS server
    container's export. **A docs-only change** is
    detected by the `changes` job, which lists the pull request's changed files
    from the API (renames count both names) and says "code" unless every file
    is under `docs/`, is a Markdown file or is `LICENSE`; a push to `main`, an
    empty list or a possibly truncated one is "code". On a docs-only change
    `envtest`, `sanity`, `smoke`, `e2e`, `e2e-kind`, `build`, `manifest` and
    `deploy` are skipped (`static` and `unit` still run) and `ci-ok` accepts
    exactly those as skipped; on code, a skipped, cancelled or failed job
    fails `ci-ok`.

27. **Deploy portability (M1, T13)**: eight settings that real clusters need.
    (a) *The kubelet directory is one parameter.* The node pod's containers
    carry `KUBELET_DIR`; every in-container path in their args derives from it
    by `$(KUBELET_DIR)` expansion, and the kubelet directory is mounted whole
    (the one hostPath), so the socket directory and the registry directory are
    subdirectories of it; only the `miami` container mounts the whole
    directory, and each sidecar mounts just its `subPath` (`plugins/<driver>`
    at `/csi`, plus `plugins_registry` at `/registration` for the registrar),
    so the sidecars never see other pods' volumes. The fields that cannot expand (env value, hostPath
    path, mount path) come from one `replacements` entry whose source is the
    ConfigMap literal `kubeletDir`; kustomize runs a base's replacements
    before an overlay's generators, so an overlay merges the literal and
    repeats that entry, and writes no patch (the snippet is in the base
    kustomization and a test renders it). (b) *The controller's `miami`
    container is AppArmor `Unconfined`*: containerd's default profile on
    Ubuntu denies `mount(2)` in a container that is not privileged, even with
    `CAP_SYS_ADMIN`. The kind end-to-end tests cannot show it (the nfs3
    transport test runs under `unshare`, and kind nodes do not enforce the
    host's profile), so it rests on reasoning about the profile. Capabilities,
    `allowPrivilegeEscalation: false` and the seccomp profile are unchanged.
    (c) *The node's transports root defaults to
    `<kubelet-dir>/plugins/miami.csi.greghaskins.com/transports`*, derived like
    `--state-dir`; an explicit flag wins. The kubelet requires its directory to
    be a shared mount, so Bidirectional propagation is guaranteed there and
    not under an arbitrary hostPath on every distribution; it also removes one
    hostPath. The controller keeps `/var/lib/miami`, an emptyDir. (d) *The
    namespace sets `enforce: privileged` and `enforce-version: latest` only*;
    warn and audit are the cluster's choice. (e) *Release images pin the CPU
    baselines* `GOAMD64=v1` and `GOARM64=v8.0` (older x86 without SSE4.2,
    Raspberry Pi 4). (f) *`fsGroupPolicy` is `ReadWriteOnceWithFSType`*:
    fsGroup applies only to volumes that have an fsType and are RWO or RWOP.
    Directory items have no fsType, so the kubelet never changes their
    ownership (no error per file on a squashing export, no recursive change of
    a large share); M2 image items (ext4, RWO) still get fsGroup. The field is
    immutable on a live CSIDriver, so a cluster that already created the
    object deletes and recreates it. (g) *The provisioner runs with
    `--timeout=60s`*: the default 15s is shorter than a first `CreateVolume`
    that pays for an NFS mount. (h) *`ENODEV` from `mount(2)`* is reported as
    "the kernel has no NFS client; load the nfs module".

28. **Transport health and directory modes (M1, T14)**: the worst storage
    outages of clusters on shared NFS are an NFS server restart (minutes of stale
    handles), mounts detached by an external watchdog, and pods that cannot write
    to directories created as root on a squashing export. M1 reports the first
    and does not repair it, and fixes the third.
    (1) **`mode` StorageClass parameter**: the mode of a directory item
    (up to four octal digits, `0755` by default, for example `0777` or `2775`;
    the setgid and sticky bits are allowed, the leading digit is 0 to 3, and
    setuid is refused because it has no meaning on a directory),
    read by `volume.NewContext`, carried in the volume context as `miami/mode`
    only when set (so existing contexts and the golden intent record are
    unchanged) and in the intent. The controller applies it to the directory it
    creates on nfs3, and the directory item to the one the node creates at the
    first stage of a local volume, with `fchmod` on the new directory's own
    descriptor so that the umask does not decide it. Only the volume's own
    directory takes it, never the subdir elements above it (0755), and an
    existing directory is adopted without a chmod. An invalid value is
    `InvalidArgument` at `CreateVolume`. Between the `mkdirat` and the `openat2`
    that follows, both create paths have a window in which a directory (a
    symlink or a mount is refused) renamed into place by someone with write
    access to the parent would receive the `fchmod`; that someone already
    controls the parent and could make the directory with any mode themselves,
    so the window is accepted, and a uid check would gain nothing and would
    break on a root-squashing export, where the created directory is not owned
    by our euid. **Upgrade order:** a node plugin from before this change
    refuses a volume context carrying `miami/mode` (unknown `miami/` keys are
    `InvalidArgument`), so `NodeStageVolume` on a not-yet-upgraded node fails,
    and an old node that rewrites a record (`clearEnsureItem`, a prune) drops
    the field, after which a repeated stage is `AlreadyExists` until the volume
    is unstaged: upgrade every node plugin before setting `mode` on a class.
    (2) **A stale, timed-out or busy stat of the item directory is the
    transport's condition**: ESTALE (the server restarted and its file handles
    are gone), `mount.ErrTimeout` and `mount.ErrBusy` from the stat are
    `ErrTransportUnavailable` (also `item.ErrTransportUnavailable`), event reason
    `TransportUnavailable`, never `Internal` and never a reason to unmount;
    ESTALE and busy are `Unavailable` to CSI (a timeout stays
    `DeadlineExceeded` by the precedence table), and a stale handle on a volume
    stats call is `Unavailable` too. The next pass retries.
    (3) **`miami_transport_up{kind,id}`**: each pass, for every transport the
    intents reference, a deadline-bounded `statfs` of its mount directory
    (statfs always asks the server, where a stat can be answered from the
    attribute cache): 1 when it answers, 0 when it times out, is busy or fails,
    and 0 for a transport that is not mounted. It runs once per transport per
    pass in the transport step under the hung-transport rule, and its change is
    logged once at info level.
    (4) **`miami_volume_condition{condition}`**: the number of volumes in each
    condition after the pass, `ok` or the key events already use
    (`unresponsive`, `transport-unavailable`, `transport-conflict`,
    `stage-conflict`, `publish-conflict`, `item-missing`, `item-not-directory`,
    `failed`), every value present at zero and no volume ID in a label. A
    transport that does not answer is `transport-unavailable` however it is
    first reported, ahead of the timeout it wraps; `unresponsive` is a timeout
    or busy that is not traced to the transport.
    **The NFS server restart e2e** (`test/e2e`, nfs3) stops and starts the
    server container, whose export is now a docker volume instead of a tmpfs, so
    the files and the file handles survive, and checks that its address is the
    same. It is the observation that must be short: the node plugin's pass runs
    every 30 seconds and a pass that probes the dead server takes its 10 second
    deadline, and waiting for the timer holds the server down for about as long
    as a soft mount takes to give up (about 40 seconds), when a write in flight
    fails with ETIMEDOUT. So the test starts a pod during the outage, whose
    publish request wakes the loop at once, and the outage is about 15 seconds.
    What it observed about a file held open is in 4.6.

Pending: none that block M1.

## 10. Milestones

1. **M1, directory items on NFSv3 and local**: the binary, controller and
   DaemonSet; transports on the host mount table with driver-owned
   profiles via `mount(2)`; intent store and reconciler; opaque IDs with PV
   lookup; metrics; the `miami lock` helper; the image build and the full CI shape
   of 7.3 with the `local` and `nfs3` e2e variants.
   Proves the plumbing with nothing but `mkdir` and bind mounts.
2. **M2, image items**: loop, ext4, LUKS2, offline expand, holder plus
   attach exclusivity, fencing, the active and passive probes, the shared
   remediation package and VAP, re-stage via the publish secret, canaries,
   the migration tool.
3. **M3, RWX images** via holder-node re-export (4.9): `export`
   transport, client markers, promotion on takeover, e2e with a holder
   kill while remote clients write.
4. **M4, node-local persistent** with `miami move` (5.2).
5. **Later**: the S3 service (5.3).
