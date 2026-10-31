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
3. Deleting the PVC removes the directory on the transport; deleting after
   a controller restart still removes it.
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
for each observed mount under <root>/stage or a target path with no intent:
                        unmount, in reverse depth order
for each transport mount with no intent referencing it and idle > TTL:
                        unmount (TTL default: never)
```

One goroutine runs the loop; CSI calls enqueue a request and wait on a
per-call channel with the RPC deadline. A per-volume mutex serializes stage
and unstage of the same ID across calls.

## 4. Volume identity and context

- `volume_id` = `<kind>:<name>`, where `name` is `pvc-<uuid>` as the
  provisioner names it (`--extra-create-metadata` is on so the PVC name and
  namespace are available for subdir tokens). Opaque; never parsed for a
  locator.
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

Validation: `subdir` must be relative, must not contain `..` as a path
element, a NUL or any control character, and no element may be longer than
255 bytes or shaped like a PV name (`pvc-<uuid>`), so a directory can never
nest inside another volume's. A template that cleans to `.` after resolution
means no prefix, as does an empty or omitted `subdir`. Each token value must
be a single non-empty path element. An unresolved or unknown token is
`InvalidArgument`. Unknown parameters are `InvalidArgument` so a typo cannot
silently mean the default.

A given server is named one way across all StorageClasses on a cluster (the
normal form already collapses case). The kernel reports a
shared NFS superblock under the spelling of whichever mount came first, so
naming one server by both a host name and an address is unsupported and
shows as a mount conflict.

The item directory is always `<transport mount>/<subdir>/<volume name>`,
with the name taken from the volume ID and never from a parameter.

## 6. CSI surface (M1)

- **Identity**: `CONTROLLER_SERVICE`, `VOLUME_ACCESSIBILITY_CONSTRAINTS`.
  Driver name `miami.csi.greghaskins.com`.
- **Controller**: `CREATE_DELETE_VOLUME`, `PUBLISH_UNPUBLISH_VOLUME`
  (accept-all for directory items; exists so M2 needs no capability
  change), `LIST_VOLUMES` (walk the transports the controller currently has
  mounted; `Aborted` on a starting token, as before).
- **Node**: `STAGE_UNSTAGE_VOLUME`, `GET_VOLUME_STATS` (`statfs` on the
  stage path, in a goroutine with a deadline).
- **Access modes**: `SINGLE_NODE_WRITER`, `SINGLE_NODE_MULTI_WRITER`,
  `MULTI_NODE_MULTI_WRITER`, `MULTI_NODE_READER_ONLY` for directory items.
- **fsType**: none accepted for directory items (`InvalidArgument` if set).
- **Topology**: `local` transports return the node as accessible topology
  (key `miami.csi.greghaskins.com/node`); `nfs3` returns none.

Error mapping: `InvalidArgument` for bad parameters; `NotFound` for an
unknown volume; `Unavailable` for a transport that cannot be mounted or a
PV that cannot be read; `DeadlineExceeded` from any observation that hits
its deadline; `FailedPrecondition` for a stage path mounted from something
other than this volume's transport directory. Never `panic` in a handler.

## 7. Controller

- Stateless. Mounts transports with the same `internal/transport` code
  into its own mount namespace under `--transports-root` (an `emptyDir`;
  statelessness is the point, nothing must survive a restart).
- `CreateVolume`: validate, resolve `subdir`, mount the transport, `mkdir
  -p <transport>/<subdir>/<name>` (idempotent: an existing directory is adopted), return ID and
  context, and for `local` the node topology. Capacity is echoed back; a
  directory has no size.
- `DeleteVolume`: PV lookup (section 4), mount, `rm -r` the directory.
  Idempotent: a missing directory is success.
- `ControllerPublishVolume` and unpublish: accept and record nothing.
- Needs `hostNetwork` on clusters whose NAS export ACLs admit node
  addresses only; `privileged: false` with `SYS_ADMIN` only, read-only root
  filesystem, `RuntimeDefault` seccomp.

## 8. Transports

**`nfs3`.** `mount(2)` with `fstype=nfs`, source `<server>:<export>`, and a
kernel option string. The driver resolves `addr=` itself (an IPv4 literal as
it is, or a host name resolved by the host network resolver to its first
IPv4 address; decision 21), so no helper and no `/etc/services` are needed. There is one profile; it is code, not configuration. The data
string of `mount(2)` is

```
vers=3,proto=tcp,softerr,timeo=100,retrans=3,nolock,lookupcache=positive,actimeo=30,addr=<ip>
```

and `noatime` is the generic mount flag `MS_NOATIME`, not a data option.

| option | reason |
|---|---|
| `vers=3,proto=tcp` | the protocol the target NAS speaks; TCP so a socket reset is a real fence (M2) |
| `softerr,timeo=100,retrans=3` | bounded failure: retries at 10, 20, 30 and 40 seconds, about 100s in all, so the major timeout is a constant the fence math (M2) can use. `softerr` returns `ETIMEDOUT` instead of `EIO`, so a NAS stall is distinguishable from a disk error. Needs kernel 5.3+; on `EINVAL` the mount fails with an explicit message, never a silent fallback to `soft` or `hard` |
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
system type `nfs`, the source `<server>:<export>`, per-mount `noatime`, and
these super options among those the kernel reports: `rw`, `vers=3`,
`proto=tcp`, `softerr`, `timeo=100`, `retrans=3`, `nolock`, `local_lock=all`,
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
value, or any per-mount option beyond `rw` and `noatime`, is not adopted.
The source must match exactly, which is why a server is named one way across
StorageClasses (section 5). Anything else at the mount point is a conflict,
reported and never stacked over.

**`local`.** The host path, bind-mounted to `<root>/transports/<ID>` so
that every transport looks the same to the item and observer code. It
must exist and be a directory; the driver never creates it.

**Mount point on the host table.** The DaemonSet mounts a `hostPath`
directory (`--transports-root`, default `/var/lib/miami`) with
`mountPropagation: Bidirectional`. A transport mounted under it is visible
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
for inspection; the volume is treated as having no intent, which means its
mounts are garbage, so the load failure is also surfaced as a metric.

**Observer.** `/proc/self/mountinfo` through a maintained parser; filtered
to mounts under `--transports-root`, the stage root, and target paths named
by intents. No `findmnt`. For `GET_VOLUME_STATS`, `statfs` under a deadline.

**Converge, for a directory item.**

1. Transport mounted (adopt or mount).
2. `<transport>/<subdir>/<name>` exists and is a directory (adopt; create only in
   the controller; a missing directory at stage is `NotFound`).
3. Stage path is a bind mount of that directory (adopt if mountinfo shows
   exactly that source; `FailedPrecondition` if it shows something else;
   bind if absent).
4. Each publish target is a bind of the stage path with the requested
   read-only flag (remount read-only if needed).

**Garbage.** Any mount under the stage root or at a recorded target path
whose volume has no intent is unmounted, deepest first. A transport with
no referencing intent is unmounted only past the idle TTL.

**CSI mapping.** `NodeStageVolume` writes the intent (without publishes)
and waits for converge; `NodePublishVolume` adds a publish and waits;
`NodeUnpublishVolume` removes it and waits; `NodeUnstageVolume` removes the
intent and waits for the garbage pass. All four are idempotent by
construction.

**Kubelet directory.** `--kubelet-dir` (default `/var/lib/kubelet`) because
distributions differ; the registrar's `--kubelet-registration-path` and
the DaemonSet's hostPaths derive from the same value in the kustomize base
through a single replacement.

## 10. Metrics

On the node (`--metrics-addr`, default `:9810`, served on the host network):

- `miami_intents{kind}` gauge; `miami_transports{kind,id}` gauge with a
  `mounted` label.
- `miami_reconcile_duration_seconds` histogram; `miami_reconcile_errors_total{step}`.
- `miami_observation_timeouts_total{op}`; `miami_garbage_unmounts_total`.
- `miami_intent_load_failures_total`.
- `miami_csi_requests_total{method,code}` and a duration histogram, from a
  gRPC interceptor, on both roles.

Events: `MountFailed`, `TransportUnavailable`, `GarbageUnmounted` on the PVC
or pod where a human would look.

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
PV get/list/watch, PVC get/list/watch/update, StorageClass get/list/watch,
VolumeAttachment and its status, Node get/list/watch, CSINode get/list/watch,
events, leases; node: Node get, events); the controller Deployment
(provisioner with `--extra-create-metadata --feature-gates=Topology=true`,
attacher, resizer kept out until M2, livenessprobe); the node DaemonSet
(registrar, livenessprobe, `Bidirectional` kubelet dir and transports
root, host `/dev` not needed in M1); an example StorageClass per transport.
Sidecars pinned by digest. Resource requests and limits on every container.
`deploy/policy_test.go` asserts the RBAC rule set is exactly the list above.

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

- `ci.yml` on every PR and push to `main`: `static`, `unit`, `envtest`,
  `build` (matrix amd64 on `ubuntu-latest`, arm64 on the arm runner;
  content-addressed per-arch tags pushed from the PR, sha alias on `main`), `smoke` (matrix, root, under
  `unshare --mount`: transport survives a plugin restart, bind adoption,
  garbage unmount, simulated reboot via a fresh mount namespace),
  `sanity` (matrix), `e2e` (matrix `local`, `nfs3` on amd64; `nfs3` also
  on arm64), `manifest` (every PR: the multi-arch index from the digests
  the build legs ran), `publish` (`main` only: the sha alias), `ci-ok`.
- `secret-scan.yml` and `secret-scan-text.yml`: already present.
- Path filters skip heavy jobs for docs-only changes; `ci-ok` still runs.
- No retries. Failure uploads mountinfo, plugin logs and kind logs.

**e2e NFS server.** `hack/nfs-server/` runs the kernel NFS server in a
privileged container on the runner's docker network, exporting a tmpfs with
`fsid=0,rw,no_root_squash,insecure` for NFSv3, and the kind nodes mount it
by container IP. The `nfs3` variant is what proves `mount(2)` without a
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
| T8 | `internal/kube` (PV read) and the controller server; `CreateVolume`, `DeleteVolume` with PV lookup, publish/unpublish, list | envtest for the PV paths; `csi-sanity` full |
| T9 | `deploy/kustomize/base` and `e2e` overlay; `deploy/policy_test.go`; Dockerfile from scratch | `kubectl apply -k` on kind; both pods Ready |
| T10 | `test/e2e`: the exit criteria 1 to 6 as tests, `local` and `nfs3` variants; `smoke` and `sanity` and `e2e` jobs wired into `ci.yml` | `ci-ok` green with the full matrix |
| T11 | `miami lock` and its test against the `nfs3` e2e transport | two pods on two nodes serialize through it; a stale lock is broken |
| T12 | README: install, StorageClass examples, the lock contract, limitations | reviewed |

T1 first, T2 to T5 in any order, T6 after T3 to T5, T7 after T6, T8 after
T2 and T4, T9 after T7 and T8, T10 after T9, T11 and T12 last.

## 16. Risks and the check that retires each

- **`mount(2)` NFSv3 without a helper, and `softerr`** on the target
  kernels: retired by the `nfs3` e2e variant in T4, before any node-agent
  code depends on it, and by the first real-cluster mount.
- **`Bidirectional` propagation and adoption across a plugin restart**: the
  reason the transports root is a hostPath; adoption across a process
  restart is retired by the T4 smoke (one private mount namespace, so it
  does not exercise propagation); propagation is proven by the e2e on a real
  cluster in T10 and by exit criterion 4.
- **Kubelet path differences across distributions**: one replacement in
  the kustomize base, checked by a render test in T9.
- **A hung NFS mount blocking the reconciler**: every observation has a
  deadline and its own goroutine; retired by a T6 unit test with a blocking
  fake and by an e2e that stops the NFS server mid-stage.
- **`attachRequired: true` with `ReadWriteMany` directory items**: the
  attacher creates one VolumeAttachment per node; retired by exit criterion
  1 (two nodes, one PVC).
- **client-go binary size on small nodes**: accepted; measured in T1 and
  recorded in the README.

## 17. Out of scope for M1

Image items, loop devices, encryption, exclusivity, fencing, expansion,
failed-volume detection, remediation, the migration tool, RWX images,
node-local tiers, the S3 service. Each has its milestone in the brief.
