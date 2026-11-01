# M1 implementation plan: directory items on NFSv3 and local

**Status:** Draft
**Created:** 2026-10-31
**Builds on:** `2026-10-30-architecture-brief.md` (decisions 1 to 17)

M1 ships a working CSI driver that provisions `directory` items (a
subdirectory of a transport bound into pods, `ReadWriteMany`) on `nfs3` and
`local` transports, with the node reconciler, the stateless controller, the
host-table transport mounts, the scratch image and the full CI shape. It
contains no loop devices, no encryption and no exclusivity; those are M2.
The point of M1 is to prove the plumbing that every later item kind rides
on, with the simplest item there is.

## 1. Exit criteria

M1 is done when all of the following hold on a kind cluster in CI and on a
real cluster by hand:

1. A PVC with `storageClassName` naming an `nfs3` transport binds, its pod
   runs, and a second pod on another node sees the first pod's writes.
2. The same with a `local` transport on one node.
3. With an `nfs3` transport, deleting the PVC removes the directory on the
   transport, and deleting after a controller restart still removes it. With
   a `local` transport, deleting the PVC deletes the PV and the directory
   remains on its node (decision 23); the e2e for `local` asserts exactly
   that, and does not expect the directory to be gone.
4. Restarting the node plugin pod while pods are running changes nothing:
   the transport stays mounted on the host, bind mounts survive, and the
   next stage and unstage on that node succeed.
5. A node reboot: pods come back, the transport is remounted on first use,
   nothing is leaked.
6. `kubectl get --raw /metrics` on the node plugin shows the metrics in
   section 10.
7. `csi-sanity` passes against both roles.
8. `make check` is green locally and `ci-ok` is green on the PR.
9. The image is `FROM scratch` plus one binary, built natively per
   architecture and combined by the manifest job.

## 2. Repository layout

```
cmd/miami/                   one binary: `miami node`, `miami controller`, `miami lock`
internal/csi/                gRPC servers: identity, controller, node (thin)
internal/volume/             volume ID and context: encode, decode, validate
internal/transport/          Transport interface, nfs3, local; profiles; registry
internal/item/               Item interface; directory (M1); image (M2)
internal/intent/             intent records: on-disk format, load, save, list
internal/observe/            mountinfo parser; (M2: losetup, dm, holder)
internal/reconcile/          the node converge loop and garbage collection
internal/mount/              mount(2), bind, umount2 wrappers with deadlines
internal/kube/               PV and Node reads through client-go (no informers)
internal/metrics/            Prometheus registry and collectors
internal/lock/               the `miami lock` helper (mkdir/O_EXCL on a transport)
deploy/kustomize/base/       namespace, CSIDriver, RBAC, controller, node, StorageClass examples
deploy/kustomize/e2e/        overlay for the kind cluster
hack/                        smoke.sh, sanity.sh, e2e.sh, kind.yaml, nfs-server/, secret-scan/
test/e2e/                    kubelet-driven tests (build tag e2e)
Dockerfile, Makefile, mise.toml, .golangci.yml, .github/workflows/
```

`internal/` rather than `pkg/`: nothing here is a library for others.

## 3. Core types

Sketches, not final code. Names are the contract.

```go
// transport
type Spec struct {              // what a StorageClass describes
    Kind    string              // "nfs3" | "local"
    Server  string              // nfs3
    Export  string              // nfs3: absolute export path
    Path    string              // local: absolute host path
}
func (s Spec) ID() string       // short hash of the canonical form
func (s Spec) MountPoint(root string) string   // <root>/transports/<ID>

type Transport interface {
    Spec() Spec
    Mount(ctx) error            // idempotent: no-op if mountinfo shows it
    Unmount(ctx) error
    Mounted(ctx) (bool, error)  // from mountinfo, under a deadline
    Dir() string                // the mount point
}

// item
type Item interface {
    Kind() string               // "directory"
    Converge(ctx, Intent, Observed) error   // one idempotent step toward desired
    Teardown(ctx, Intent, Observed) error
}

// intent: what should be true on this node
type Intent struct {
    VolumeID   string
    Kind       string
    Transport  transport.Spec
    Name       string           // the item's name inside the transport
    StagePath  string
    Publishes  []Publish        // target paths with readOnly flag
    MountFlags []string
    AccessMode string
    Version    int              // record format version, 1
}

// observed: what the kernel says
type Observed struct {
    Mounts []observe.Mount      // every mount under the plugin's roots, from mountinfo
}
```

The reconciler holds a map of `Intent` by volume ID, loaded from the intent
store at start and mutated only by CSI calls, and runs:

```
for each intent:        transport.Mount → item.Converge → publishes present
for each observed mount under <root>/stage no volume's stage path covers,
and each publish target no intent claims that was named by an Unpublish or
claimed by the previous pass (and is a bind of what it should be):
                        unmount, in reverse depth order
for each transport mount with no intent referencing it and idle > TTL:
                        unmount (TTL default: never; off unless configured)
```

One goroutine runs the loop; CSI calls enqueue a request and wait on a
per-call channel with the RPC deadline. A per-volume mutex serializes stage
and unstage of the same ID across calls.

## 4. Volume identity and context

- `volume_id` = `<kind>:<name>`, where `name` is `pvc-<uuid>` as the
  provisioner names it (`--extra-create-metadata` is on so the PVC name and
  namespace are available for subdir tokens). Opaque; never parsed for a
  locator. The name is always of that shape, lower-case hex, and any other is
  refused at every boundary (decision 22), so no subdir element can be a
  volume and no volume can nest in another; a static PV keeps or receives
  such a name.
- `volume_context` (set by `CreateVolume`, stored on the PV, sent on every
  node call):

| key | value |
|---|---|
| `miami/kind` | `directory` |
| `miami/transport.kind` | `nfs3` or `local` |
| `miami/transport.server` | nfs3 server |
| `miami/transport.export` | nfs3 export path |
| `miami/transport.path` | local host path |
| `miami/subdir` | optional prefix under the transport, tokens expanded; omitted when empty. The item directory is `<subdir>/<name>` (decision 20) |
| `miami/mode` | optional mode of the item directory when the driver creates it, four octal digits (`0755`); omitted when the StorageClass sets none (decision 28) |

- Delete and expand get only the ID, so the controller fetches the PV by
  name (`pvc-<uuid>` is the PV name) and rebuilds the `Spec` from its
  `volumeAttributes`. No PV: delete returns success without deleting and
  logs at warning (Kubernetes keeps the PV until delete returns since 1.31;
  older clusters can leak, documented). A PV whose attributes do not parse:
  `Unavailable`, never delete on doubt.

## 5. StorageClass parameters (M1)

| parameter | required | meaning |
|---|---|---|
| `kind` | yes | `directory` |
| `transport.kind` | yes | `nfs3` or `local` |
| `transport.server` | nfs3 | IPv4 literal or host name resolved to IPv4 (decision 21) |
| `transport.export` | nfs3 | export path |
| `transport.path` | local | absolute host path |
| `subdir` | no | optional prefix under the transport with `${pvc.namespace}` and `${pvc.name}` tokens; default none. The volume's own name is always appended (decision 20) |
| `mode` | no | the mode of the volume's directory when the driver creates it: an octal number of up to four digits, `0755` by default; `0777` (any user may write, for an export that squashes root) and `2775` (setgid) are the documented examples (decision 28) |

Validation: `subdir` must be relative, must not contain `..` as a path
element, a NUL or any control character, and no element may be longer than
255 bytes or shaped like a PV name (`pvc-<uuid>`), so a directory can never
nest inside another volume's. A template that cleans to `.` after resolution
means no prefix, as does an empty or omitted `subdir`. Each token value must
be a single non-empty path element. An unresolved or unknown token is
`InvalidArgument`. Unknown parameters are `InvalidArgument` so a typo cannot
silently mean the default.

`mode` is `InvalidArgument` unless it is one to four octal digits (a sign, a
prefix such as `0o`, a digit above 7 or more than four digits is refused); the
setgid and sticky bits are the leading digit (0 to 3); setuid (a leading 4 to
7) is refused because it has no meaning on a directory, and nothing above four
digits is allowed. Upgrade every node plugin before setting `mode` on a class:
an older node refuses a volume context with `miami/mode`, and one that rewrites
a record drops the field. It is the mode of the volume's own directory only: the `subdir`
elements above it are made 0755, and a directory that already exists is adopted
as it is and never chmod'ed. The controller applies it for `nfs3` and the node at
the first stage of a `local` volume, with `fchmod` so that the umask does not
decide it.

A given server is named one way across all StorageClasses on a cluster (the
normal form already collapses case). The kernel reports a shared NFS
superblock under the spelling of whichever mount came first, so naming one
server by both a host name and an address is unsupported and shows as a
mount conflict.

The item directory is always `<transport mount>/<subdir>/<volume name>`,
with the name taken from the volume ID and never from a parameter.

## 6. CSI surface (M1)

- **Identity**: `CONTROLLER_SERVICE`, `VOLUME_ACCESSIBILITY_CONSTRAINTS`.
  Driver name `miami.csi.greghaskins.com`.
- **Controller**: `CREATE_DELETE_VOLUME`, `PUBLISH_UNPUBLISH_VOLUME`
  (accept-all for directory items; exists so M2 needs no capability
  change), `LIST_VOLUMES` (walk the transports the controller currently has
  mounted; `Aborted` on a starting token, as before).
- **Node**: `STAGE_UNSTAGE_VOLUME`, `GET_VOLUME_STATS` (`statfs` on a
  publish target, in a goroutine with a deadline).
- **Access modes**: `SINGLE_NODE_WRITER`, `SINGLE_NODE_MULTI_WRITER`,
  `MULTI_NODE_MULTI_WRITER`, `MULTI_NODE_READER_ONLY` for directory items.
- **fsType**: none accepted for directory items (`InvalidArgument` if set).
- **Topology**: `local` transports return the node as accessible topology
  (key `miami.csi.greghaskins.com/node`); `nfs3` returns none.

Error mapping: `InvalidArgument` for bad parameters; `NotFound` for an
unknown volume; `Unavailable` for a transport that cannot be mounted or a
PV that cannot be read; `DeadlineExceeded` from any observation that hits
its deadline; `FailedPrecondition` for anything mounted at a directory volume's stage
path, or at a publish target from something other than the volume's directory. Never `panic` in a handler.

Choices made in T8 where the plan was silent:

- A volume ID that does not parse is one this driver could not have issued.
  `ValidateVolumeCapabilities` and `ControllerPublishVolume` answer
  `NotFound`; `ControllerUnpublishVolume` and `DeleteVolume` answer success
  (the CSI specification's idempotent reading), and delete deletes nothing.
- `ListVolumes` lists directories two levels below each transport the
  controller has mounted whose names are PV names (pvc-<uuid>), enters the
  other directories as subdir elements, ignores `max_entries`, and answers
  `Aborted` to any starting token. It has a deadline of its own and leaves out
  a transport that does not answer. It is for operators and the sanity suite,
  not for completeness.
- The controller remembers, in memory and bounded, what each `CreateVolume`
  name answered, so a repeat with other parameters or capacity is
  `AlreadyExists`. A restart forgets it; nothing else depends on it.
- `DeleteVolume` treats a 404 for the PV as "no PV" only when the API server
  itself answered it (a `Status`); a 404 from a proxy or load balancer in
  front of a wrong endpoint is an unknown answer, `Unavailable`. It also
  refuses a PV whose reclaim policy is not `Delete`.
- Removal of a directory is relative to descriptors (`openat2` with
  `RESOLVE_BENEATH`, `RESOLVE_NO_SYMLINKS`, `RESOLVE_NO_XDEV`), so it needs
  Linux 5.6; there is no fallback to removing by path.
- File system operations against one transport are bounded (one per
  directory, eight per transport); beyond that a call is `Unavailable` at
  once, so a hung transport costs a bounded number of threads.
- A second request for a volume whose first is still running is `Aborted`.
- `mount_flags` in a capability are ignored: the driver owns the mount options.

Choices made in T7 where the plan was silent:

- A volume ID that does not parse is `InvalidArgument` for stage, unstage and
  unpublish (a request the driver could never have been given), and `NotFound`
  for publish and volume stats (a volume that is not staged here).
- A repeated `NodeStageVolume` is compared with the intent record before the
  reconciler is called: another volume context or staging path is
  `AlreadyExists`. A publish whose `staging_target_path` is not the staged
  record's is `FailedPrecondition`.
- The directory of a local volume is created only while the intent has
  `ensureItem` (first stage; a retried stage keeps it, and the reconciler's
  Stage never gives it back once the loop has cleared it). The flag is
  informational in the record format (omitted when false), so it needs no
  version bump.
- `NodeGetVolumeStats` answers for the publish targets of a staged volume
  (`NotFound` otherwise, the stage path included: nothing is mounted there,
  and kubelet asks with the pod's path; decision 25). A statfs that fails or times out fails
  the call with the code of its class (`DeadlineExceeded` for a timeout,
  `Unavailable` for a busy path, `Internal` for an unexpected errno) and a
  warning in the log, so a wedged transport is not counted as a success. The
  CSI specification this driver is built against (v1.13) has removed
  `VolumeCondition` from the response, so there is no field for an abnormal
  condition beside an answer.
- `NodeUnpublishVolume` removes the target directory with `rmdir(2)` after the
  unmount, which csi-sanity asks of a plugin that created it (the reconciler
  makes it at publish when kubelet has not). A directory that cannot be removed
  does not fail the call.
- Events go to the PV named by the volume ID, through the dynamic client
  rather than client-go's recorder and typed events client: a small recorder
  that queues, so the converge loop never waits for the API server, and names
  an event by PV, PV UID, reason and message so that a repeat bumps its count
  and a PV made again under the same name does not (the node needs `events` get,
  create and patch).
- The controller serves metrics on `:9811` and the node on `:9810`, both on
  the host network; an empty `--metrics-addr` turns the endpoint off.

## 7. Controller

- Stateless. Mounts transports with the same `internal/transport` code
  into its own mount namespace under `--transports-root` (an `emptyDir`;
  statelessness is the point, nothing must survive a restart).
- `CreateVolume`: validate, resolve `subdir`. For `nfs3`: mount the
  transport, `mkdir -p <transport>/<subdir>/<name>` (idempotent: an existing
  directory is adopted; the new directory gets the `mode` parameter, 0755 by
  default), return ID and context. For `local` (decision 23):
  return ID, context and the node topology and touch no file system, because
  the directory is on the disk of the node the volume is for, not of the node
  the controller runs on; the node creates it at first stage if it is
  missing. Capacity is echoed back; a directory has no size.
- `DeleteVolume`: PV lookup (section 4). For `nfs3`: mount, then remove the
  directory relative to descriptors (no symlink or mount crossed). Idempotent:
  a missing directory is success. For `local` the controller cannot reach the
  node's disk: success, logged at warning, nothing deleted; the directory
  remains on the node until M4's node-local tier removes it (a documented
  limitation, as on a cluster older than 1.31 without a PV).
- `ControllerPublishVolume` and unpublish: accept and record nothing.
- Needs `hostNetwork` on clusters whose NAS export ACLs admit node
  addresses only; `privileged: false` with `SYS_ADMIN` only, read-only root
  filesystem, `RuntimeDefault` seccomp.

## 8. Transports

**`nfs3`.** `mount(2)` with `fstype=nfs`, source `<server>:<export>`, and a
kernel option string. The driver resolves `addr=` itself (an IPv4 literal as
it is, or a host name resolved by the host network resolver to its first
IPv4 address; decision 21), so no helper and no `/etc/services` are
needed. There is one profile; it is code, not configuration. The data
string of `mount(2)` is

```
vers=3,proto=tcp,softerr,timeo=100,retrans=17,nolock,lookupcache=positive,actimeo=30,addr=<ip>
```

and `noatime`, `nosuid` and `nodev` are the generic mount flags `MS_NOATIME`, `MS_NOSUID` and `MS_NODEV`, not data options (nosuid and nodev: architecture brief, decision 34).

| option | reason |
|---|---|
| `vers=3,proto=tcp` | the protocol the target NAS speaks; TCP so a socket reset is a real fence (M2) |
| `softerr,timeo=100,retrans=17` | bounded failure: a 180 second major timeout, `timeo` x (`retrans`+1) (over TCP the retry interval grows linearly and the major timeout is capped there), so it is a constant the fence math (M2) can use. 180 seconds is about 1.6 times the longest measured server restart (about 2m20s), so writes in flight wait a restart out instead of failing; the cost is that blocked I/O waits up to 180 seconds on a dead server, and anything that waits on the volume must budget above that (architecture brief, decision 30). `softerr` returns `ETIMEDOUT` instead of `EIO`, so a NAS stall is distinguishable from a disk error. Needs kernel 5.3+; on `EINVAL` the mount fails with an explicit message, never a silent fallback to `soft` or `hard` |
| `nolock` | locks are node-local; cross-node coordination uses `mkdir` and `O_EXCL`, which are server round-trips regardless; no NLM, no lockd state to hang |
| `lookupcache=positive` | never cache a negative lookup, so a file or lock directory created on another node is seen on the next lookup |
| `actimeo=30` | attribute cache; file contents are unaffected because close-to-open revalidates on every `open` (also how M2 reads another node's holder mtime fresh); directory listings lag by at most 30s |
| `noatime` | believed inert on an NFS client; kept because the option set known to work on the target NAS has it |

`hard` is deliberately absent: an unbounded wait is the D-state node and
the shutdown hang, and it makes fencing impossible, so a `hard` volume
could only ever be taken over by a taint. Resuming after an outage is
handled instead by bounded failure plus automatic re-stage (M2).

Two empirical checks belong to the first real-cluster deployment, not CI:
whether the NAS accepts the set without `actimeo` and `noatime` ("protocol
not supported" usually means a missing `vers=3`), and whether `softerr`
mounts on every node kernel. Either outcome changes the profile by a code
change, with a version bump.

**Adoption.** After a restart the transport is still on the host table, and
`Mounted` answers true only for a mount that is exactly this transport: file
system type `nfs`, the source `<server>:<export>`, per-mount `nosuid`, `nodev` and `noatime`, and
these super options among those the kernel reports: `rw`, `vers=3`,
`proto=tcp`, `softerr`, `timeo=100`, `retrans=17`, `nolock`, `local_lock=all`,
`lookupcache=pos`, `acregmin=30`, `acregmax=30`, `acdirmax=30`. The kernel
reports the profile in its own words (`actimeo=30` as the `ac*` options, with
a default such as `acdirmin=30` left out; `lookupcache=positive` as
`lookupcache=pos`), and newer kernels add options (`softreval`,
`fatal_neterrors`) that are not required. The list was taken from a real
mount by the T4 e2e run. Adoption is the profile and nothing that changes
its meaning, not a byte-exact comparison (`rsize` and `wsize` are
negotiated, newer kernels add tokens, the MOUNT request adds `mount*`): a
mount that also has `nocto`, `nosharecache`, `noresvport`, `soft`, `hard`,
`ro`, an `acdirmin`, `sec`, `lookupcache` or `local_lock` with another
value, or any per-mount option beyond `rw`, `nosuid`, `nodev` and `noatime`, is not adopted.
That includes a mount made with an earlier profile (`retrans=3`): it is
reported as a transport conflict and left alone (decisions 30 and 34, which adds the same for a mount without `nosuid` and `nodev`).
The source must match exactly, which is why a server is named one way across
StorageClasses (section 5). Anything else at the mount point is a conflict,
reported and never stacked over.

**`local`.** The host path, bind-mounted to `<root>/transports/<ID>` so
that every transport looks the same to the item and observer code. It
must exist and be a directory; the driver never creates it.

**Mount point on the host table.** The DaemonSet mounts the kubelet
directory with `mountPropagation: Bidirectional`; the node's transports root
(`--transports-root`) defaults to
`<kubelet-dir>/plugins/miami.csi.greghaskins.com/transports` (decision 27)
and the controller's to `/var/lib/miami`, an emptyDir. A transport mounted under it is visible
to the host and to the next plugin pod. On start the reconciler reads
mountinfo and adopts what is there; it never assumes.

**Deadlines.** Every `stat`, `statfs`, mount and unmount runs in its own
goroutine with a deadline (default 10s, flag) because a hung NFS mount
ignores context cancellation. A timed-out observation is reported as
unavailable, and the goroutine is abandoned, never joined.

## 9. Node agent

**Intent store.** `--state-dir` (default
`<kubelet-dir>/plugins/miami.csi.greghaskins.com/intents`), one JSON file
per volume named by a filesystem-safe form of the ID, written by
temp-file-and-rename with `fsync` on the file and the directory. Records
carry `version: 1`. Loading an unreadable record logs it and keeps the file
for inspection. The volume's intent is then unknown, not absent: its mounts
are left alone and the failure is surfaced as a metric and a log line, and
nothing is collected anywhere on the node while any record is unreadable
(an explicit `NodeUnpublishVolume` still unmounts the target it names). The
log line says what a person should do: repair the file or, if the volume is
gone, remove it; removing the record of a volume still in use unmounts it, as
its mounts are then garbage. A
request that would have to read or rewrite that record (`NodeStageVolume`,
`NodeUnstageVolume`, `NodePublishVolume`, `NodeUnpublishVolume`) fails as
unavailable and writes and unmounts nothing; collection resumes on its own
once every record reads, whether a person repaired the file or a transient
read error passed.

**Observer.** `/proc/self/mountinfo` through a maintained parser; filtered
to mounts under `--transports-root`, the stage root, and target paths named
by intents. No `findmnt`. For `GET_VOLUME_STATS`, `statfs` under a deadline.

**Converge, for a directory item.**

1. Transport mounted (adopt or mount), then asked whether it answers: a
   `statfs` of its mount directory under the stat deadline, once per transport
   per pass, reported as `miami_transport_up` and logged once per change. A
   timeout or busy is noted as a hung transport (one deadline per transport per
   pass, the other volumes on it wait for the next pass). The transport is
   reported and never repaired: nothing is unmounted or remounted because it
   does not answer.
2. `<transport>/<subdir>/<name>` exists and is a directory (adopt). For
   `nfs3` the controller created it, and a missing directory at stage is
   `NotFound`; for `local` the controller creates nothing (decision 23), so
   the node creates the directory here, with mode 0755, at first stage. The
   intent carries `ensureItem`, set by `NodeStageVolume` when it writes the
   first record of a local volume and cleared by the reconciler once the item
   has converged; a directory missing without it is `NotFound`, as for `nfs3`,
   so a disk that is not mounted, or a reboot, never gives a pod an empty
   volume. A directory the node creates takes the volume's `mode` (0755 by
   default). The stat of the directory that returns ESTALE, times out or is
   busy is `ErrTransportUnavailable`, not a failure of the item: the NFS server
   has restarted or is not answering, the volume's event is `TransportUnavailable`
   and the next pass looks again (decision 28).
3. Nothing is mounted at the stage path (decision 25). A bind of the item
   directory there is the stage bind of a build before the decision, and is
   unmounted; anything else mounted there is `FailedPrecondition`, reported and
   left, since the driver makes no mount there and cannot show it made that
   one.
4. Each publish target is a bind of the item directory with the requested
   read-only flag (adopt if mountinfo shows that directory, compared the NFS
   way; remount read-only if needed).

**Garbage.** Any mount strictly under the stage root that no volume's stage
path covers is unmounted, deepest first (the driver makes none at a stage
path, so a mount at exactly a live stage path, or inside it, is the volume's
conflict and is left), and so is a mount at a publish target
that no intent claims but that an `Unpublish` named or that the previous pass
saw claimed; a publish target is unmounted only if it is a bind of the
volume's directory in its transport (a bind of it as the kernel reports binds, the same device and root path or on
NFS the directory in the mount source, also for a directory that has since been
removed), once, never a stack. The one wider
case is an explicit `NodeUnpublishVolume` for a volume with no intent: kubelet
named the path and nothing identifies the volume, so a bind of any directory
in one of the driver's transports is accepted. A transport with no referencing intent is unmounted only
past the idle TTL, which is off by default (never).

**CSI mapping.** `NodeStageVolume` writes the intent (without publishes)
and waits for converge; `NodePublishVolume` adds a publish and waits;
`NodeUnpublishVolume` removes it and waits; `NodeUnstageVolume` unmounts the
publishes (a directory volume has nothing mounted at the stage path to take
down), and only then removes the intent, so a failed unmount keeps the intent and is retried. All four are idempotent by
construction.

**Kubelet directory.** `--kubelet-dir` (default `/var/lib/kubelet`) because
distributions differ; the registrar's `--kubelet-registration-path` and
the DaemonSet's hostPaths derive from the same value in the kustomize base
through a single replacement.

## 10. Metrics

On the node (`--metrics-addr`, default `:9810`, served on the host network):

- `miami_intents{kind}` gauge; `miami_transports{kind,id}` gauge whose value
  is 1 when the transport was found mounted and 0 when not (a `mounted` label
  would leave a stale series at every change).
- `miami_transport_up{kind,id}` gauge: 1 when a deadline-bounded `statfs` of the
  transport's mount directory answered in the pass, 0 when it timed out, was
  busy or failed, or the transport is not mounted (statfs always asks the
  server, where a stat can come from the attribute cache). One series per
  transport the intents reference.
- `miami_volume_condition{condition}` gauge: the number of volumes in each
  condition after the pass; `condition` is `ok` or one of `unresponsive`,
  `transport-unavailable`, `transport-conflict`, `stage-conflict`,
  `publish-conflict`, `item-missing`, `item-not-directory`, `failed`, every
  value present at zero. No volume ID in a label. A volume is in one condition:
  its stage's if that failed, else its first failing publish's.
- `miami_reconcile_duration_seconds` histogram; `miami_reconcile_errors_total{step}`
  (steps: `load`, `transport`, `probe`, `item`, `publish`, `teardown`, `garbage`,
  `observe`).
- `miami_observation_timeouts_total{op}`; `miami_garbage_unmounts_total`.
- `miami_intent_load_failures`: a gauge, the number of records unreadable in
  the latest pass (it falls when a person repairs one), not the counter the
  first draft named.
- `miami_csi_requests_total{method,code}` and a duration histogram, from a
  gRPC interceptor, on both roles.

Events: `MountFailed`, `TransportUnavailable`, `GarbageUnmounted` on the
PersistentVolume named by the volume ID, where `kubectl describe pv` looks.

## 11. The `miami lock` helper

`miami lock acquire|release|run --dir <path> --name <lock> [--stale 5m]`:
`mkdir` the lock directory (atomic on NFSv3), write `holder` and `since`
inside it, renew `since` every `--renew` (default 30s) while `run` holds
it, and let an acquirer break a lock whose `since` is older than `--stale`
on the server's clock (compared against a probe file's mtime, never the
local clock). Exit codes: 0 held, 1 contended, 2 error. This is the
documented way to coordinate writers on a directory item; `flock` is not.

## 12. Deploy

`deploy/kustomize/base/`: Namespace; CSIDriver (`attachRequired: true`,
`podInfoOnMount: false`, `fsGroupPolicy: File`, `volumeLifecycleModes:
[Persistent]`); ServiceAccounts, ClusterRoles and bindings (controller:
PV get/list/watch/create/patch/delete (the provisioner creates and deletes PVs
and the attacher patches them; added in T9 from the pinned sidecars' own RBAC),
PVC get/list/watch/update, StorageClass get/list/watch, VolumeAttachment
get/list/watch/update/patch and its status patch, Node get/list/watch, CSINode
get/list/watch, events list/watch/create/update/patch, leases get/create/update/delete;
node: PV get, events get, create and patch: the recorder reads an event before it bumps its
count); the controller Deployment
(provisioner with `--extra-create-metadata --feature-gates=Topology=true`,
attacher, resizer kept out until M2, livenessprobe); the node DaemonSet
(registrar, livenessprobe, `Bidirectional` kubelet dir and transports
root, host `/dev` not needed in M1); an example StorageClass per transport.
Sidecars pinned by digest. Resource requests and limits on every container.
`deploy/policy_test.go` asserts the RBAC rule set is exactly the list above.
The sidecars are csi-provisioner v6.4.0, csi-attacher v4.13.0,
csi-node-driver-registrar v2.18.0 and livenessprobe v2.20.0. The kubelet
directory is one literal in the node manifest, kept in agreement by a render
test. The e2e overlay and `hack/e2e-overlay.sh` set the driver image and the
NFS server address per run; `hack/kind.sh` (`make deploy-check`, the `deploy`
CI job) is the deploy check on a control plane and two workers.

## 13. Build

- `Dockerfile`: two stages, `golang` builder (digest-pinned) and `FROM
  scratch` with the binary. The M2 tools stage is added then.
- `Makefile`: `build`, `test`, `test-race`, `cover`, `vet`, `fmt-check`,
  `fmt`, `lint`, `tidy`, `tidy-check`, `smoke`, `sanity`, `e2e`
  (`E2E_TRANSPORT=local|nfs3`), `image`, `check` (everything CI runs,
  minus the per-arch and manifest steps). `make check` is what a PR must
  pass locally.
- `mise.toml` grows: `go` (pinned; `go.mod` carries the same `go`
  directive and `GOTOOLCHAIN=local` keeps that toolchain the only one),
  `golangci-lint`, `kind`, `kubectl`, `csi-sanity`, `setup-envtest`.

## 14. CI

Workflows, all pinned by SHA with least permissions:

- `ci.yml` on every PR and push to `main`:
  - `changes`: whether the change is more than documentation (`docs/`,
    Markdown files, `LICENSE`); on a push to `main` always "code".
  - `static`, `unit`: every change.
  - `envtest`, `build` (matrix amd64 on `ubuntu-latest`, arm64 on the arm
    runner; content-addressed per-arch tags pushed from the PR, sha alias on
    `main`), `manifest` (every PR: the multi-arch index from the digests the
    build legs ran), `deploy` (the deploy check on kind): skipped when the
    change is documentation only.
  - `smoke` (matrix amd64, arm64; root, under `unshare --mount`: transport
    survives a plugin restart, bind adoption, garbage unmount, simulated
    reboot via a fresh mount namespace) and `sanity` (matrix amd64, arm64):
    skipped when documentation only.
  - `e2e` (`make e2e`, transport mode): the transport package's test against
    the kernel NFSv3 server, nfs3 on amd64; skipped when documentation only.
  - `e2e-kind` (`make e2e E2E_MODE=kind`): `local` and `nfs3` on amd64 and
    `nfs3` on arm64, each from that architecture's `x-<hash>-<arch>` image on
    a kind cluster of one control plane and two workers; exit criteria 1 to 6
    as the Go tests of `test/e2e`; skipped when documentation only.
  - `publish` (`main` only: the sha alias).
  - `ci-ok`: needs every job; passes when `changes` succeeded and said "true"
    or "false", `static` and `unit` succeeded, each filterable job succeeded
    (or, only when `changes` said "false", was skipped) and `publish` ran on
    `main` and was skipped elsewhere.
- `secret-scan.yml` and `secret-scan-text.yml`: already present.
- Path filters skip the heavy jobs for docs-only changes (the `changes` job);
  `ci-ok` still runs and still passes, and never accepts a skipped job on a code
  change.
- No retries. Failure uploads mountinfo, plugin logs and kind logs; the kind
  end-to-end tests also dump the failed test's YAML and events.

**e2e NFS server.** `hack/nfs-server/` runs the kernel NFS server in a
privileged container on the runner's docker network, exporting a docker volume
(so that its files and the clients' file handles survive a restart of the
container) with `fsid=0,rw,no_root_squash,insecure` for NFSv3, and the kind
nodes mount it by container IP. The `nfs3` variant is what proves `mount(2)` without a
helper; it must exist before the first node-agent PR merges.

## 15. Work breakdown

Each task is one PR, small enough to review in one sitting, with its
acceptance check. Order matters only where stated.

| # | Task | Accept when |
|---|---|---|
| T1 | Scaffold: module, `cmd/miami` with subcommands that print version, Makefile, mise.toml, golangci config, `ci.yml` with `static`, `unit`, `build` (both arches), `manifest`, `ci-ok` | `ci-ok` green on an empty driver |
| T2 | `internal/volume`: ID and context encode/decode/validate; `internal/transport.Spec` with ID and canonical form; subdir token resolution | unit tests, including the `InvalidArgument` cases |
| T3 | `internal/mount` and `internal/observe`: `mount(2)` wrappers with deadlines, mountinfo parser, bind and read-only remount | unit tests against fixture mountinfo; smoke under `unshare --mount` |
| T4 | `internal/transport`: `local` and `nfs3` with the one option set; adopt-from-mountinfo | smoke: mount, restart the process, adopt, unmount; e2e harness `hack/nfs-server` and `make e2e E2E_TRANSPORT=nfs3` mounting by hand |
| T5 | `internal/intent`: record format, atomic write, load with failure accounting | unit tests including a corrupt record |
| T6 | `internal/reconcile` with the `directory` item: converge, garbage, per-volume serialization, request queue | unit tests with fake observers; smoke: plugin restart with mounts live, reboot simulation |
| T7 | `internal/csi` node server over the reconciler; `internal/metrics`; gRPC interceptor | `csi-sanity` node half; metrics scraped in smoke |
| T8 | `internal/kube` (PV read) and the controller server; `CreateVolume`, `DeleteVolume` with PV lookup, publish/unpublish, list | envtest for the PV paths; `csi-sanity` for the identity and controller services (full run in T10) |
| T9 | `deploy/kustomize/base` and `e2e` overlay; `deploy/policy_test.go`; Dockerfile from scratch | `kubectl apply -k` on kind; both pods Ready |
| T10 | `test/e2e`: the exit criteria 1 to 6 as tests, `local` and `nfs3` variants; `smoke` and `sanity` and `e2e` jobs wired into `ci.yml` | `ci-ok` green with the full matrix |
| T11 | `miami lock` and its test against the `nfs3` e2e transport | two pods on two nodes serialize through it; a stale lock is broken |
| T12 | README: install, StorageClass examples, the lock contract, limitations | reviewed |
| T14 | the `mode` parameter; ESTALE and deadlines on the item directory as the transport's condition; `miami_transport_up` and `miami_volume_condition`; the NFS server restart e2e on a docker-volume export | `ci-ok` green with the restart test in the nfs3 legs |

T1 first, T2 to T5 in any order, T6 after T3 to T5, T7 after T6, T8 after
T2 and T4, T9 after T7 and T8, T10 after T9, T11 and T12 last.

## 16. Risks and the check that retires each

- **`mount(2)` NFSv3 without a helper, and `softerr`** on the target
  kernels: retired by the `nfs3` e2e variant in T4, before any node-agent
  code depends on it, and by the first real-cluster mount.
- **`Bidirectional` propagation and adoption across a plugin restart**: the
  reason the transports root is under the kubelet directory's hostPath; adoption across a process
  restart is retired by the T4 smoke (one private mount namespace, so it
  does not exercise propagation); propagation is proven by the e2e on a real
  cluster in T10 and by exit criterion 4.
- **Kubelet path differences across distributions**: one ConfigMap
  literal and one replacement in the kustomize base, checked by a render test
  in T9 and by a throwaway overlay in T13.
- **A hung NFS mount blocking the reconciler**: every observation has a
  deadline and its own goroutine; retired by a T6 unit test with a blocking
  fake and by an e2e that stops the NFS server mid-stage.
- **`attachRequired: true` with `ReadWriteMany` directory items**: the
  attacher creates one VolumeAttachment per node; retired by exit criterion
  1 (two nodes, one PVC).
- **A directory bind at the stage path cannot be unstaged on NFS**: the T10
  end-to-end test on kind found that the NFS client reports `/` as the root of
  every mount of an export, so kubelet's reference check (same device and root
  as the staging path) found the transport and every other volume's bind and
  refused to unstage. Resolved by decision 25 (T10a): a directory item makes
  no stage mount and publishes bind the item directory. The nfs3 e2e on the
  T10 branch is rerun after T10a merges.
- **client-go binary size on small nodes**: accepted; measured in T1 and
  recorded in the README.

## 17. Out of scope for M1

Image items, loop devices, encryption, exclusivity, fencing, expansion,
failed-volume detection (M1 reports a transport that does not answer through
`miami_transport_up`, `miami_volume_condition` and a `TransportUnavailable`
event, and does not remediate it: no unmount, remount or pod deletion),
remediation, the migration tool, RWX images,
node-local tiers, the S3 service. Each has its milestone in the brief.
