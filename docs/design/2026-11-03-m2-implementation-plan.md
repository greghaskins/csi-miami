# M2 implementation plan: chunk-backed volumes

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Status:** Active. T1 to T20 are merged into `m2/design` (T15 was dropped, decision 53);
T21 (e2e) is PR #71 and T22 (docs) is the PR that carries this edit. Gate G1
(criterion 10) waits for the owner's Raspberry Pi run. The tables in sections 1
and 5 say where each criterion and task stands.
**Created:** 2026-11-03
**Spec:** `2026-11-03-chunk-backed-volumes-design.md` (the plan argues from
it; executors read both). **Reads with:** `2026-10-30-architecture-brief.md`
(decisions 1 to 50), `2026-10-31-working-mode.md`, `2026-10-31-m1-implementation-plan.md`
(the code the tasks extend).

**Goal:** a `volume` StorageClass kind: ext4 over LUKS2 over an nbd device
served from a node-local chunk cache, with point-in-time generations on the
NFSv3 transport, strong RWO through the holder and a fencing token, online
growth, a coreutils restore, and the e2e and benchmark gates of the spec's
section 7.

**Architecture:** the node plugin's reconciler gains a second `item.Item`
whose `Converge` drives seven observed layers (holder, host binary, transient
unit, nbd device, LUKS mapping, ext4 mount, size); a new subcommand `miami
blockd` runs per volume as a transient systemd service and owns the cache,
the nbd backend, dirty tracking and publishing; the chunk store and manifest
format are one package shared by the server, the controller, `restore` and
`import`. Everything the kernel or systemd can answer is observed, never
cached (brief section 3).

**Tech stack:** Go 1.27 (`golang.org/x/sys/unix`, `github.com/mdlayher/genetlink`
and `netlink` for nbd device creation, `github.com/coreos/go-systemd/v22/dbus`
for transient units, the existing CSI, gRPC, client-go and Prometheus
dependencies); `cryptsetup` and `e2fsprogs` as the two tools in the image
(decision 13, built in T1 since M1 shipped the bare binary); the nbd protocol
implemented in-repo (the one maintained Go library lacks flush, trim and FUA).

## Global constraints

- Nothing environment-specific enters the repository (working mode section
  5): no hostnames, addresses, export paths, node names, backup targets,
  prior project names. Node names in tests are `kind-worker` and friends.
- Commit messages: imperative subject, a body that says why, no
  co-author trailer and never a session link (secret scanning rejects both). PR text is scanned; it says what changed and what was verified.
- One branch, one PR: every task is a branch `m2/tNN-<slug>` from `m2/design`
  (or from an unmerged predecessor when a wave needs it, said in the merge
  message), merged into `m2/design` with a merge commit by the orchestrator
  after review; `m2/design` is PR #41. Never rebase, never force-push.
- `make check` green before every push; `ci-ok` green before every merge
  into `m2/design`; no retries to make a flaky test pass.
- The driver execs exactly two tools, `cryptsetup` and the e2fsprogs
  binaries, through one `exec` seam (`internal/tool`, T1); everything else is
  syscalls, netlink or D-Bus (decision 14).
- Every observation of a hang-prone path runs under a deadline in its own
  goroutine and is abandoned, never joined (brief 4.2); every new mount is
  `nosuid,nodev`.
- Events carry a fixed text per reason and never an error string (decision
  33); metrics carry no path, address or hash of one. The generation and
  server families carry the volume name as a label (a `pvc-<uuid>`, not a
  path), a plan-level call recorded in T19.
- Keys live only in memory and in the kernel's mapping: never in an intent
  record, a log, a unit's command line or the control socket's replies.
- Tool and dependency pins are the latest stable at the time of the PR that
  adds them, each pin in its own commit.
- Spec values, verbatim: chunk size a power of two from 1 MiB to 256 MiB,
  default the smallest of at least 4 MiB keeping `size / chunk <= 65536`;
  sizes round up to whole chunks; `generations.keep` default 3;
  `generations.interval` default 5 m; `generations.maxAge` default 1 h;
  `generations.dirtyWatermark` default half the cache bound; `cacheSize`
  default the volume size; cipher default `xchacha12,aes-adiantum-plain64`,
  key size 256, sector size 4096; takeover timeout default 5 m; ext4 mounted
  `discard,errors=remount-ro`; nfs3 profile gains `noacl`.

## Review focus

Input classes the spec implies but no task's tests would otherwise exercise;
each line's test is added to the owning task.

1. A manifest whose chunk count disagrees with its `size` and `chunk`
   header (truncated upload, hand edit): `chunk.ParseManifest` must reject
   it, and `restore` must refuse before writing a byte (T2, T18).
2. A chunk file whose content does not hash to its name (bit rot, a mirror
   that copied mid-write): the cache fetch must fail that read with
   `chunk.ErrCorrupt` and the device must return an I/O error for that
   request only, not wedge (T4, T8).
3. The holder file re-acquired by another node between a publisher's check
   and its rename of `current`: the publisher's next check must see the
   foreign epoch and refuse, and `current` must never move backwards in
   epoch (T8, T9).
4. `NodeUnstageVolume` arriving while a generation is in flight or the
   transport is down: the final generation is attempted once under its
   deadline, then unstage proceeds and succeeds, with the discarded bytes in
   the event (T11).
5. A `NodeStageVolume` for a `volume` on a node whose prerequisites probe
   failed (no nbd, no systemd socket): `FailedPrecondition` with the named
   reason, no unit started, no cache directory created (T11, T17).

---

## 1. Exit criteria

M2 is done when, on the kind e2e cluster over the nfs3 transport:

1. A `miami-remote-vol-rwo` claim mounts in a pod with ext4 semantics (exec
   bit, `flock` exclusive across two processes, `mmap`, a 100755 blob from
   git), and the NAS directory holds only chunks whose names are their
   hashes, manifests and `current`.
2. A pod writes a numbered sequence; the pod is deleted and rescheduled to
   the other worker; the old node's unstage publishes and verifies a final
   generation before releasing (decision 52); the new pod finds the whole
   sequence and `e2fsck` clean.
3. The node plugin is deleted under a served volume; the pod's writes
   continue with no error; the new plugin adopts the unit, the device, the
   mapping and the mount with no change in mount IDs.
4. The block server is killed (`SIGKILL`) under a served volume; systemd
   restarts it, it re-points the same nbd device (decision 51), and the
   pod's writes continue with no error, no pod deletion and no change in the
   device index or mount ID; its unpublished writes are present.
5. The NFS server is held down for longer than the major timeout while the
   pod writes: no `blocked for more than` message in the node's kernel log,
   the writes succeed throughout, generations resume when the server returns.
6. The holder node's plugin is stopped; the pod, recreated on the other
   worker, is NOT given the volume (it waits `holder-contended`) until an
   operator acts (decision 52). The node is then marked out of service (and,
   in a second run, the PV annotated with the holder instead); the pod mounts
   from `current` on the other worker; the old node, resumed, refuses to
   publish and its unstage succeeds, discarding.
6a. A holder node whose pod moved while it was down returns and drains the
   volume on its own: it publishes its dirty chunks, verifies, releases, and
   the waiting pod on the other worker then mounts with them.
7. A node is restarted (`docker restart`) with a served volume: the cache
   survives; the next stage on that node reuses it because `current` still
   names its base; the dirty chunks are published.
8. `miami restore` and the five-line coreutils drill both reconstruct the
   volume from the NAS directory and the sequence reads back.
9. `ControllerExpandVolume` grows a mounted volume online; the pod sees the
   new size without restart.
10. `miami bench` on a Raspberry Pi 4 worker shows the nbd stack within 1.5×
    the loop stack on every phase (gate G1, run outside CI by the owner).
11. The M1 e2e suite, sanity, smoke, envtest and the deploy check still pass;
    directory items are unchanged.

**Where the criteria stand.** The e2e tests are T21's (`test/e2e/volume_*_test.go`),
run in the nfs3 variant on both architectures; the local variant skips them
with a reason.

| # | Status | Test |
|---|---|---|
| 1 | covered by T21, PR #71 | `TestVolumeExt4Semantics` |
| 2 | covered by T21 | `TestVolumeSurvivesReschedule` |
| 3 | covered by T21 | `TestVolumePluginRestart` |
| 4 | covered by T21 | `TestVolumeServerKill` |
| 5 (G2) | covered by T21 | `TestVolumeNASOutageNoHungTask` |
| 6 | covered by T21, with a gap in kind (below) | `TestVolumeTakeover`, both modes |
| 6a | covered by T21 | `TestVolumeOrphanDrain` |
| 7 | covered by T21 | `TestVolumeNodeRebootReusesCache` |
| 8 | covered by T21 | `TestVolumeRestoreDrill` |
| 9 | covered by T21 | `TestVolumeExpandOnline` |
| 10 | **pending** the owner's Pi run (gate G1); `miami bench` and `docs/bench.md` are merged (T20) and CI runs the harness as a smoke on amd64 | `miami bench` |
| 11 | covered by CI on every PR: `static`, `unit`, `envtest`, `sanity` and `smoke` on both architectures, `deploy`, and the M1 e2e suite | `ci-ok` |

**The kind gap in criterion 6.** Kind nodes share the runner's kernel, where a
device-mapper name and an nbd device are single objects, so the old node's
unstage after a takeover cannot close the mapping the new holder uses (EBUSY).
The old-node cleanup ("its unstage succeeds, discarding") is therefore a
skipped subtest in kind (`TestVolumeTakeover/old-node-unstage-discards`), which
asserts instead that the returning old node changes nothing of the new
holder's; the volume item's fenced-unstage unit tests cover the cleanup, and a
cluster with two kernels runs it in full. The attach-detach controller's forced
detach (about six minutes) is likewise stood in for by deleting the
VolumeAttachment in the takeover and drain tests; that is cluster behaviour,
not the driver's.

## 2. Repository layout

New or changed, by package:

| Path | Responsibility |
|---|---|
| `internal/tool` | the one exec seam: `Runner`, `cryptsetup`, `mkfs.ext4`, `e2fsck`, `resize2fs` argv builders and exit-code mapping |
| `internal/chunk` | chunk store on a directory: hashes, chunk files, manifests, `current`, retention |
| `internal/nbd` | nbd protocol server, netlink device client, sysfs observer |
| `internal/cache` | node-local chunk cache with index, dirty set, boundary (copy-on-write), eviction |
| `internal/blockd` | the block server program: backend over cache and store, control RPC, publishing, resume |
| `internal/systemd` | transient unit start, stop, status over the private socket |
| `internal/crypt` | LUKS2 format, open, close, resize, rotate through `internal/tool` |
| `internal/ext4` | mkfs, fsck, online resize, freeze and thaw |
| `internal/holder` | holder file with epoch, renewal, release-if-mine, age on the transport's clock, takeover judgement inputs |
| `internal/item/volume` | the `volume` item: seven converge layers, teardown, generation scheduling hooks |
| `internal/remediate` | delete pods on this node that publish a failed volume, UID precondition, rate limit |
| `internal/kube` | add node status reads (Ready, `volumesInUse`, out-of-service taint), PV annotation read, pod delete |
| `internal/volume`, `internal/intent`, `internal/csi`, `internal/reconcile`, `internal/metrics`, `internal/transport` | the kind, its parameters and context keys; new intent fields; secrets, expansion, capabilities; the item registry, conditions and generation timers; new families; `noacl` |
| `cmd/miami` | `blockd`, `restore`, `import`, `bench` subcommands; new node flags; prerequisite probe |
| `Dockerfile`, `hack/image-closure.sh` | the tools stage of decision 13 |
| `deploy/kustomize` | hostPaths, resizer sidecar, RBAC, admission policies, the `vol` example classes |
| `hack/bench.sh`, `hack/smoke.sh`, `hack/e2e-kind.sh` | the gate harnesses |
| `test/e2e` | the eleven exit criteria as tests |

## 3. Core types and interfaces

Signatures the tasks share. A task's Interfaces block repeats only what it
consumes or produces.

### 3.1 `internal/chunk`

```go
const (
    MinChunkSize = 1 << 20; MaxChunkSize = 256 << 20; DefaultMinChunk = 4 << 20
    MaxManifestLines = 65536; FormatVersion = 1
)
type Hash [32]byte                      // SHA-256
func (h Hash) String() string           // 64 lower-case hex
func ParseHash(s string) (Hash, error)
func Sum(b []byte) Hash
func ZeroHash(chunkSize int64) Hash     // the hash of chunkSize zero bytes, memoised
func DefaultChunkSize(size int64) int64 // smallest power of two >= 4 MiB with size/chunk <= 65536
func CheckChunkSize(size, chunk int64) error // power of two in range, size/chunk <= 65536
func RoundUp(size, chunk int64) int64

type Manifest struct {
    Volume string; Epoch uint64; Sequence uint64; Size, Chunk int64
    Time time.Time; Chunks []Hash
}
func (m Manifest) Name() string         // "<epoch>.<sequence>"
func ParseManifestName(s string) (epoch, seq uint64, err error)
func (m Manifest) Marshal() []byte      // "# format 1\n# volume ...\n# epoch ...\n# sequence ...\n# size ...\n# chunk ...\n# time RFC3339\n" then one hash per line
func ParseManifest(r io.Reader) (Manifest, error) // ErrManifestFormat on a header or count mismatch

type Store struct{ /* root directory of the volume on the transport */ }
func Open(root string) (*Store, error)  // root must exist; creates chunks/ and manifests/ when absent
func (s *Store) ChunkPath(h Hash) string // chunks/<aa>/<hash>
func (s *Store) Has(h Hash) (bool, error)
func (s *Store) Put(ctx context.Context, h Hash, r io.Reader, n int64) error // temp name beside, rename; no-op when present; verifies the hash while copying, ErrCorrupt on mismatch
func (s *Store) Get(ctx context.Context, h Hash, w io.Writer) error // verifies, ErrCorrupt on mismatch
func (s *Store) Current(ctx context.Context) (name string, ok bool, err error) // fresh open every call
func (s *Store) ReadManifest(ctx context.Context, name string) (Manifest, error)
func (s *Store) WriteManifest(ctx context.Context, m Manifest) error // temp name, rename; refuses to replace
func (s *Store) Publish(ctx context.Context, name string) error      // temp file + rename over current
func (s *Store) Manifests(ctx context.Context) ([]string, error)     // sorted by epoch, sequence
func (s *Store) Prune(ctx context.Context, keep int) (removedManifests, removedChunks int, err error)
var ErrCorrupt, ErrManifestFormat, ErrNoCurrent error
```

### 3.2 `internal/nbd`

```go
// Backend is what the server serves. Offsets and lengths are byte values the
// kernel already bounded by the advertised block sizes.
type Backend interface {
    ReadAt(p []byte, off int64) (int, error)
    WriteAt(p []byte, off int64, fua bool) (int, error)
    Flush() error
    Trim(off, n int64) error
    Size() int64
}
type ServeOptions struct{ ReadOnly bool; BlockSize uint32; MaxRequest uint32 } // BlockSize 4096, MaxRequest 32 MiB
// Serve speaks the fixed-newstyle handshake then transmission until NBD_CMD_DISC or ctx ends.
func Serve(ctx context.Context, conn net.Conn, b Backend, o ServeOptions) error

const (FlagSendFlush = 1 << 2; FlagSendFUA = 1 << 3; FlagSendTrim = 1 << 5) // transmission flags the server advertises
type ConnectOptions struct {
    Index int            // -1: allocate
    Size int64; BlockSize uint32
    Timeout, DeadConnTimeout time.Duration // seconds on the wire
    ServerFlags uint64   // the three flags above, plus HAS_FLAGS
    Backend string       // written to /sys/block/nbdN/backend
}
func Connect(ctx context.Context, sock *os.File, o ConnectOptions) (index int, err error) // genetlink NBD_CMD_CONNECT
func Reconfigure(ctx context.Context, index int, size int64) error                        // NBD_CMD_RECONFIGURE with NBD_ATTR_SIZE_BYTES
func Disconnect(ctx context.Context, index int) error                                      // NBD_CMD_DISCONNECT
type Device struct{ Index int; Pid int; Backend string; Size int64 }
func Devices(sysfs string) ([]Device, error) // /sys/block/nbd*/{pid,backend,size}; Pid 0 when unconfigured
func Available() error                        // genetlink family "nbd" resolvable, else ErrNoNBD
type Devicer interface{ Devices() ([]Device, error); Available() error; Connect(context.Context, *os.File, ConnectOptions) (int, error); Reconfigure(context.Context, int, int64) error; Disconnect(context.Context, int) error }
func Real(sysfs string) Devicer               // the kernel; tests use a fake
```

### 3.3 `internal/cache`

```go
type Index struct {
    Volume string; Base string /* manifest name, "" for a new volume */; Epoch uint64
    Size, Chunk int64; Dirty []uint32 /* chunk indexes */; Boundary []uint32 /* in-flight set, nil when none */
}
type Cache struct{ /* dir, index, LRU of clean chunks, bound */ }
func Open(dir string, bound int64) (*Cache, error)     // reads index.json or ErrNoIndex
func Create(dir string, idx Index, bound int64) (*Cache, error)
func (c *Cache) Index() Index
func (c *Cache) Has(i uint32) bool
func (c *Cache) Read(i uint32, buf []byte) error        // ErrMiss when absent; buf len == chunk
func (c *Cache) Write(i uint32, buf []byte) error       // marks dirty; copies first if i is in Boundary
func (c *Cache) Insert(i uint32, buf []byte) error      // a clean chunk from the store; evicts clean LRU to stay under bound; ErrFull when dirty alone exceeds bound
func (c *Cache) SetBoundary() []uint32                  // snapshot the dirty set; later writes to it copy-on-write
func (c *Cache) BoundaryChunk(i uint32, buf []byte) error // the frozen copy
func (c *Cache) ClearBoundary(published bool)           // published: dirty -= boundary; else merge back
func (c *Cache) MarkZero(i uint32) error                // trim
func (c *Cache) Grow(size int64) error
func (c *Cache) Sync() error                            // index + chunk files durable
func (c *Cache) Close() error
func Remove(dir string) error                           // the whole directory
var ErrNoIndex, ErrMiss, ErrFull, ErrBoundaryActive error
```

Chunk files are `<dir>/c/<index>` (decimal), opened O_DIRECT with 4096-aligned
buffers; the boundary copies are `<dir>/b/<index>`; the index is `index.json`
written by temp-and-rename.

### 3.4 `internal/blockd`

```go
type Config struct {
    Volume string; StoreDir, CacheDir, ControlSocket string
    Size, Chunk, CacheBound int64; Epoch uint64; Node string
    ReadOnly bool; Timeout, DeadConnTimeout time.Duration
    Holder holder.Checker // re-reads the holder file before publishing
}
func Run(ctx context.Context, cfg Config, log *slog.Logger) error // the whole program: open cache (resume or create from current), connect device, serve, answer control RPCs, until Stop

// Control is the net/rpc service on the unix socket; the node plugin is the client.
type Status struct{ Index int; Size int64; Dirty, Cached int64 /* bytes */; Base string; LastPublish time.Time; LastPublishError string; Refused bool; BinaryHash string }
type PublishArgs struct{ Trigger string /* interval|watermark|request|unstage */ }
type PublishReply struct{ Name string; Uploaded int64; Refused bool }
type Control interface {
    Status(args struct{}, reply *Status) error
    Boundary(args struct{}, reply *struct{}) error          // step 2 of the spec's 4.5, called between FIFREEZE and FITHAW
    Publish(args PublishArgs, reply *PublishReply) error     // step 3 and 4; ErrNoBoundary if none set
    Grow(args struct{ Size int64 }, reply *struct{}) error
    Stop(args struct{}, reply *struct{}) error
}
type Client struct{ /* rpc.Client */ }
func Dial(ctx context.Context, socket string) (*Client, error)
func (c *Client) Status(ctx) (Status, error); Boundary(ctx) error; Publish(ctx, trigger string) (PublishReply, error); Grow(ctx, size int64) error; Stop(ctx) error
var ErrNoBoundary, ErrRefused error
```

### 3.5 `internal/systemd`

```go
type Unit struct{ Name, Description string; ExecStart []string; Environment []string }
type Manager interface {
    Start(ctx context.Context, u Unit) error        // transient service; CollectMode=inactive-or-failed, KillMode=control-group, StandardOutput=journal, Type=simple
    Stop(ctx context.Context, name string) error    // StopUnit "replace", then ResetFailed; nil when absent
    State(ctx context.Context, name string) (State, error) // ActiveState, SubState, MainPID
}
type State struct{ Active, Sub string; MainPID int; Present bool }
func New(socket string) (Manager, error)            // "/run/systemd/private"; ErrNoSystemd when the dial fails
func UnitName(volume string) string                 // "miami-blockd-<volume>.service"
```

### 3.6 `internal/tool`, `internal/crypt`, `internal/ext4`

```go
// tool
type Runner interface{ Run(ctx context.Context, argv []string, extra []*os.File) (stdout, stderr []byte, code int, err error) }
func Real() Runner
type Result struct{ Code int; Stderr string }
// crypt
type Params struct{ Cipher string; KeySize int; SectorSize int } // defaults: xchacha12,aes-adiantum-plain64, 256, 4096
func Format(ctx, r tool.Runner, dev string, key []byte, p Params) error // luksFormat --type luks2 --pbkdf pbkdf2 --pbkdf-force-iterations 1000 --batch-mode --key-file /dev/fd/3
func Open(ctx, r, dev, name string, key []byte) error                   // open --type luks2 --key-file /dev/fd/3
func Close(ctx, r, name string) error
func Resize(ctx, r, name string) error
func Rotate(ctx, r, dev string, oldKey, newKey []byte) error            // luksChangeKey --key-file /dev/fd/3 <dev> /dev/fd/4
func IsLUKS(ctx, r, dev string) (bool, error)                            // isLuks exit code
func MapperPath(name string) string                                      // /dev/mapper/<name>
func Name(volume string) string                                          // "miami-<volume>"
var ErrBadKey error // exit 2
// ext4
func Mkfs(ctx, r tool.Runner, dev string) error                 // mkfs.ext4 -q -F -E lazy_itable_init=0,lazy_journal_init=0
func Fsck(ctx, r, dev string) error                             // e2fsck -p; codes 0,1,2 ok; >=4 ErrFsck
func Resize(ctx, r, dev string) error                           // resize2fs <dev>, online growth
func Freeze(fd int) error; func Thaw(fd int) error              // FIFREEZE 0xC0045877, FITHAW 0xC0045878
const MountOptions = "discard,errors=remount-ro"
```

### 3.7 `internal/holder`

```go
type Info struct{ Node string; Epoch uint64; Renewals uint64; Age time.Duration /* on the transport's clock */; Present bool }
type Holder struct{ /* dir, name, node, epoch */ }
func Path(dir, name string) string // <dir>/<name>.holder, beside the volume directory
func Read(ctx context.Context, dir, name string) (Info, error)                 // fresh open; age = probe mtime - holder mtime (reuses lock's probe)
func Acquire(ctx context.Context, dir, name, node string, judge Judgement) (*Holder, error) // O_EXCL create when absent; takeover when judge says so; ErrHeld otherwise
func (h *Holder) Epoch() uint64
func (h *Holder) Renew(ctx context.Context) error        // write temp, rename over; ErrLost when the file is no longer ours
func (h *Holder) Release(ctx context.Context) error      // remove only if still ours; nil otherwise
type Checker interface{ Mine(ctx context.Context) (bool, error) } // *Holder implements it
type Judgement struct{ Takeover bool; Reason string }          // from reconcile (4.6 conditions): out-of-service taint; Ready and not in volumesInUse and stale 30 s; not Ready and older than T with the two-clock rule
type Acquirer interface{ Read(ctx context.Context, dir, name string) (Info, error); Acquire(ctx context.Context, dir, name, node string, judge Judgement) (*Holder, error) }
var ErrHeld, ErrLost error
```

### 3.8 The `volume` kind in `internal/volume`, `internal/intent`, `internal/csi`

```go
const KindVolume Kind = "volume"
// parameters (StorageClass): chunkSize, cacheSize, encryption ("on"|"off", default on), cipher, keySize,
// generations.interval, generations.keep, generations.maxAge, generations.dirtyWatermark, takeoverTimeout (duration, optional override)
// context keys: miami/chunkSize, miami/cacheSize, miami/encryption, miami/cipher, miami/keySize, miami/size (bytes, set by CreateVolume and ControllerExpand),
// miami/generations.interval, miami/generations.keep, miami/generations.maxAge, miami/generations.dirtyWatermark
type Context struct{ ...; Volume *VolumeParams } // nil for directory
type VolumeParams struct{ ChunkSize, CacheSize, Size int64; Encryption bool; Cipher string; KeySize int; Generations GenerationParams }
type GenerationParams struct{ Interval, MaxAge time.Duration; Keep int; DirtyWatermark int64 }
// intent: Intent gains Volume *VolumeParams (omitempty) and Epoch uint64 (omitempty); Version stays 1; golden bytes unchanged
// csi: NodeStageVolume reads Secrets["key"] and Secrets["previousKey"] into the in-memory request only; NodePublishVolume the same (re-stage);
// capabilities add EXPAND_VOLUME (controller), ONLINE expansion, EXPAND_VOLUME (node); volume accepts SINGLE_NODE_WRITER and SINGLE_NODE_MULTI_WRITER only in M2
```

## 4. The `volume` item's converge layers

`internal/item/volume.Item.Converge(ctx, in, obs)` runs these in order, each
`observe → converge` against the kernel, systemd or the transport, stopping
at the first error, which maps to a condition:

| # | Layer | Observe | Converge | Condition on failure |
|---|---|---|---|---|
| 1 | prerequisites | `nbd.Available`, `/dev/mapper/control`, an AF_ALG bind of `adiantum(xchacha12,aes)` succeeds, systemd socket dials, cache dir writable | nothing; refuse | `prerequisite-missing` (`FailedPrecondition`) |
| 2 | holder | `holder.Read` | `Acquire` with the reconciler's `Judgement`; renew on every pass | `holder-contended` (`Unavailable`) |
| 3 | binary | `<bin-dir>/miami-<hash>` present and same size | copy `/proc/self/exe`, temp and rename, mode 0755 | internal |
| 4 | unit and device | `systemd.State` active, `nbd.Devices` has one with `Backend == volume`, and the control socket's `Status` names that device (decision 51: sysfs `pid` is not liveness) | start the unit with `blockd` args (3.4); blockd reconfigures an existing device or connects a new one; wait for `Status` under the deadline | `server-down` (`Unavailable`) |
| 5 | LUKS mapping | `/sys/block/dm-*/dm/name == crypt.Name(volume)` | `IsLUKS` false and no `current`: `Format`; then `Open` with the request's key; `ErrBadKey` and a `previousKey` that opens: `Rotate(previous, key)` then `Open` (one chunk re-uploads at the next generation); no key in hand: `key-required` | `key-required` (`FailedPrecondition`), `bad-key` |
| 6 | filesystem and mount | mountinfo at the stage path, source the mapper (or the nbd device when plain) | `Fsck`; `Mkfs` when the device has no superblock (first stage); mount `ext4` with `MountOptions`, `nosuid,nodev` | `failed` on fsck >= 4 |
| 7 | size | `BLKGETSIZE64` of the mapper vs `in.Volume.Size` | `Client.Grow`, `crypt.Resize`, `ext4.Resize` | internal |

`PublishSource` returns the stage path. `Teardown` (decision 52): thaw
(EINVAL is fine), unmount, `Close`, wait out any publish in flight, then the
final generation (`Publish("unstage")`), re-read `current` and require it to
name that manifest (or the cache's base when nothing was dirty) with
`Status` showing nothing dirty; only then the Stop RPC (the only call that
disconnects the device), `Stop` the unit, `cache.Remove`, `Release`. Any
failure before the Stop RPC is a retryable error and leaves the server and
cache serving. A refused publish (fenced: the operator abandoned this holder)
discards, reports `GenerationLost` with the bytes, and succeeds.
Generation timers (4.5 of the spec) live in the reconciler per staged volume
(T12) and call layers 6 and 4's clients: `Freeze` → `Boundary` → `Thaw` →
`Publish`.

## 5. Work breakdown

Each task is one branch and one merge into `m2/design`, small enough to
review in one sitting, with its acceptance check. Reviews: the orchestrator
reviews every task; an independent reviewer on the strongest model reads
T2, T3, T4, T8, T9, T11, T13, T15 and T18 (data path, exclusivity, deletion,
RBAC).

| # | Task | Accept when |
|---|---|---|
| T1 | Image tools stage and the exec seam | smoke runs `cryptsetup --version` and `mkfs.ext4 -V` from the built image on both arches |
| T2 | `internal/chunk` | unit tests incl. corrupt chunk, bad manifest, prune keeps `keep`; fuzz on `ParseManifest` |
| T3 | `internal/nbd` | smoke: device created by netlink, 64 MiB served from a memory backend, `dd` round trip, flush, trim, disconnect; sysfs observer lists it |
| T4 | `internal/cache` | unit tests incl. boundary copy-on-write, eviction, `ErrFull`, resume from index, corrupt chunk file |
| T5 | `internal/systemd` | smoke on the runner: start a transient unit that sleeps, see it in `State`, stop it, `ResetFailed` after a failing one |
| T6 | `internal/crypt`, `internal/ext4` | smoke on a loop file: format, open, mkfs, mount, freeze/thaw, resize online, rotate, close; unit tests on argv through a fake `Runner` |
| T7 | The `volume` kind: parameters, context, intent, controller `CreateVolume`/`DeleteVolume`/caps, `noacl` | unit tests incl. chunk-size derivation table, rounding, every `InvalidArgument`; golden intent bytes unchanged; sanity green |
| T8 | `internal/blockd` and `miami blockd` | unit tests with an in-memory store and cache: read miss fetches, write dirties, boundary, publish order and refusal, resume; smoke: real device under the program, fsync-heavy writes, kill -9 and resume |
| T9 | `internal/holder` | unit tests on a temp dir with a fake transport clock: acquire, renew, lost, release-if-mine, epoch increments, takeover judgement cases; e2e-transport test on nfs3 |
| T10 | Secrets, node flags, prerequisite probe | unit tests: key read from stage and publish secrets, never logged; `miami_node_capability` families; `FailedPrecondition` text per missing prerequisite |
| T11 | `internal/item/volume` | reconcile unit tests with fakes for every layer: first stage, re-stage after plugin restart (adopt), key-required, server-down, teardown order, unstage succeeds on refusal; smoke: full stack on the runner |
| T12 | Generations in the reconciler | unit tests with the fake clock: interval, watermark, PV annotation request, unstage trigger, overdue condition, no publish when clean |
| T13 | Takeover: node status, judgement, restage from `current` | unit tests on the three conditions and the two-clock rule; envtest for node reads |
| T14 | Probe and conditions for volumes | unit tests: two consecutive failures mark failed; conditions exported; event per change |
| T15 | Remediation package, RBAC and admission policy | unit tests: UID precondition, rate limit, own-node only, opt-out flag; envtest: the policy admits exactly the allowed delete |
| T16 | Expansion: controller, node, resizer sidecar | unit tests; e2e criterion 9 |
| T17 | Deploy: hostPaths, policies, examples, policy tests | `deploy-check` green; `TestBaseGolden` updated; both pods Ready on kind with the new mounts |
| T18 | `miami restore`, `miami import`, the drill | unit tests on a temp store; e2e criterion 8 runs both the subcommand and the script |
| T19 | Metrics and events | families registered and scraped in smoke; label policy test |
| T20 | `miami bench` and `hack/bench.sh` (gate G1) | the harness runs on the amd64 runner in CI as a smoke (numbers logged, no threshold); the Pi run is the owner's and its table is attached to the PR |
| T21 | e2e: criteria 1 to 9 and 11 (gate G2 is criterion 5) | `ci-ok` green with the nfs3 legs on both arches |
| T22 | README and docs | matrix, class table, prerequisites, limitations, restore drill; decision-log amendments if any task changed a decision |

Where the tasks stand (PR numbers are on `m2/design`; follow-up PRs are listed
with their task):

| Task | State | PRs |
|---|---|---|
| T1 | merged | #43 |
| T2 | merged | #44 |
| T3 | merged | #42, follow-ups #47, #48 |
| T4 | merged | #45 |
| T5 | merged | #46 |
| T6 | merged | #50 |
| T7 | merged | #51 |
| T8 | merged | #52 |
| T9 | merged | #49, with #54 (holder `Adopt`) |
| T10 | merged | #53, with #55 (Adiantum by `AF_ALG` bind) |
| T11 | merged | #56 (T11a, blockd re-points the device), #57, follow-ups #60 (ext4 `fsType`), #61 |
| T12 | merged | #62 |
| T13 | merged | #63 (also #59, `DeleteVolume` refuses while any holder exists) |
| T13w | merged | #64 |
| T14 | merged | #65 |
| T15 | dropped (decision 53) | none |
| T16 | merged | #67 |
| T17 | merged | #68 |
| T18 | merged | #66 |
| T19 | merged | #69 |
| T20 | merged; gate G1 run pending | #70 |
| T21 | in review | #71 |
| T22 | in review | the PR carrying this table |

Waves (at most three subagents at a time):

| wave | tasks | why |
|---|---|---|
| 1 | T1, T2, T3 | independent foundations |
| 2 | T4, T5, T6 | independent; T4 needs T2's hash type |
| 3 | T7, T9 | T7 touches the kind registry; T9 needs nothing of M2 |
| 4 | T8 | needs T2, T3, T4, T9 |
| 5 | T10, T20 | T20 needs T8 (the bench drives `blockd`); **gate G1 runs here**: the owner runs `miami bench` on a Pi and the plan stops if it fails |
| 6 | T11, T12 | T11 needs T5 to T10; T12 needs T11 |
| 7 | T13, T14, T16 | need T11 |
| 8 | T15, T17, T18, T19 | T17 needs T15's policy and T16's sidecar |
| 9 | T21 | everything |
| 10 | T22 | last |

## 6. Tasks

Every task: branch `m2/tNN-<slug>` from `m2/design`; `make check` before
push; PR body (on the task branch's merge message) says what the acceptance
check proved. Steps are listed once per task as the test names and commands;
the implementer writes idiomatic code to the signatures in section 3.

### Task T1: Image tools stage and the exec seam

**Files:** modify `Dockerfile`; create `hack/image-closure.sh`,
`internal/tool/tool.go`, `internal/tool/tool_test.go`, `internal/tool/fake/fake.go`;
modify `hack/smoke.sh`, `.github/workflows/ci.yml` (smoke runs one step from
the image).

**Interfaces:** produces `tool.Runner`, `tool.Real()`, `tool/fake.Runner`
(records argv and extra fds, answers scripted results).

- [ ] Test `TestRealRunnerPassesExtraFilesAsFd3` (a pipe in `extra` is read
      by a `cat /dev/fd/3` child on the host; skip when `cat` is absent).
- [ ] Test `TestFakeRunnerRecordsArgv`.
- [ ] Implement `tool`: `Run` uses `exec.CommandContext`, `ExtraFiles`,
      captured stdout and stderr, exit code from `ExitError`; a context
      deadline kills the process group.
- [ ] Dockerfile: a `tools` stage from a digest-pinned Alpine that installs
      `cryptsetup` and `e2fsprogs`; `hack/image-closure.sh` copies
      `/sbin/cryptsetup`, `/sbin/mkfs.ext4`, `/sbin/e2fsck`, `/sbin/resize2fs`
      and their `ldd` closure into `/closure`; the final `scratch` stage
      copies `/closure/` and `/miami`. No shell in the final image.
- [ ] Smoke (`hack/smoke.sh`, new step `tools`): run the image with
      `--entrypoint /sbin/cryptsetup --version` and `/sbin/mkfs.ext4 -V`.
- [ ] Run `make check` and `make smoke`; commit.

**Accept when** the smoke job on both architectures prints both tool
versions from the image that would ship.

### Task T2: `internal/chunk`

**Files:** create `internal/chunk/{hash.go,manifest.go,store.go,prune.go}`
and tests; `internal/chunk/testdata/manifest.golden`.

**Interfaces:** produces everything in 3.1.

- [ ] Tests: `TestDefaultChunkSizeTable` (1 GiB→4 MiB, 256 GiB→4 MiB,
      512 GiB→8 MiB, 4 TiB→64 MiB), `TestCheckChunkSizeRejects` (3 MiB,
      512 KiB, 1 GiB, 1 TiB at 4 MiB), `TestRoundUp`, `TestZeroHashIsStable`,
      `TestManifestRoundTrip` (golden bytes pinned),
      `TestParseManifestRejectsCountMismatch` (review focus 1),
      `TestParseManifestRejectsUnknownFormat`, `TestPutVerifiesAndIsIdempotent`,
      `TestGetDetectsCorruptChunk` (review focus 2), `TestPublishReplacesCurrent`,
      `TestCurrentIsReadFresh`, `TestWriteManifestRefusesReplace`,
      `TestPruneKeepsNewestAndTheirChunks`, `TestManifestsSortByEpochThenSequence`,
      `FuzzParseManifest`.
- [ ] Implement 3.1. `Put` streams through a `sha256` writer to a temp file
      `chunks/aa/.tmp-<hash>-<rand>`, fsyncs, renames; `Publish` writes
      `.current-<rand>` then renames; `Prune` lists kept manifests, unions their
      hashes, removes the rest of `chunks/*/*` and older manifests, never
      `current`'s target.
- [ ] Run `go test ./internal/chunk -race`; commit.

**Accept when** the tests above pass and the fuzz target runs 30 s clean.

### Task T3: `internal/nbd`

**Files:** create `internal/nbd/{proto.go,server.go,netlink.go,sysfs.go}`,
tests, `internal/nbd/smoke_test.go` (tag `smoke`); add
`github.com/mdlayher/genetlink`, `github.com/mdlayher/netlink` to `go.mod`
(own commit); modify `hack/smoke.sh` to `modprobe nbd` and include the package.

**Interfaces:** produces 3.2. Consumes nothing of M2.

- [ ] Unit tests with a `net.Pipe` client that speaks the protocol:
      `TestHandshakeFixedNewstyle`, `TestReadWriteFlushTrim`, `TestFUAWrite`,
      `TestOversizedRequestIsRejectedNotServed`, `TestDiscEndsServe`,
      `TestBackendErrorMapsToEIO` (review focus 2: one request fails, the
      next succeeds), `TestDevicesParsesSysfsFixture` (fixture tree with
      `pid`, `backend`, `size`).
- [ ] Implement the server: newstyle handshake with `NBD_FLAG_FIXED_NEWSTYLE`
      and `NO_ZEROES`, `NBD_OPT_GO` with the export name, info blocks for
      block sizes; transmission loop with a bounded worker pool, structured
      reply not required; the three flags advertised.
- [ ] Implement netlink: family lookup, `CONNECT` with a socketpair's fd,
      `RECONFIGURE`, `DISCONNECT`; `Available`.
- [ ] Smoke `TestSmokeNBDRoundTrip`: memory backend, `Connect`, `dd` 64 MiB
      through `/dev/nbdN`, `blkdiscard` a range and read zeros, `Reconfigure`
      to grow and read the new size from `BLKGETSIZE64`, `Disconnect`; also
      `TestSmokeDeadServerReturnsEIO` (kill the serving goroutine, read fails
      within `DeadConnTimeout` + `Timeout`, no hang).
- [ ] Commit the pin, then the package.

**Accept when** the smoke runs on both architectures' runners with the
`nbd` module loaded, and the unit suite passes with `-race`.

### Task T4: `internal/cache`

**Files:** create `internal/cache/{cache.go,index.go,evict.go}` and tests.

**Interfaces:** consumes `chunk.Hash`; produces 3.3.

- [ ] Tests: `TestCreateThenOpenRoundTripsIndex`, `TestWriteMarksDirty`,
      `TestBoundaryCopiesOnWrite` (write after `SetBoundary` leaves
      `BoundaryChunk` unchanged), `TestClearBoundaryPublishedDropsDirty`,
      `TestClearBoundaryUnpublishedMergesBack`, `TestInsertEvictsCleanLRU`,
      `TestInsertFullWhenDirtyExceedsBound`, `TestMarkZero`, `TestGrow`,
      `TestOpenWithoutIndexIsErrNoIndex`, `TestCorruptIndexIsRejected`,
      `TestReadMissWhenAbsent`, `TestDirectIOAlignment` (buffers from
      `AlignedBuffer(n)`), `TestRemoveDeletesEverything`.
- [ ] Implement 3.3 with O_DIRECT files, an LRU over clean chunks, the index
      rewritten atomically on every dirty-set change (batched by a short
      timer, forced by `Sync`).
- [ ] Commit.

**Accept when** the suite passes under `-race` on a tmpfs and on a real
disk (O_DIRECT needs the latter: the test skips alignment cases on tmpfs
and says so).

### Task T5: `internal/systemd`

**Files:** create `internal/systemd/{systemd.go,fake.go}`, tests,
`smoke_test.go`; add `github.com/coreos/go-systemd/v22` (own commit).

**Interfaces:** produces 3.5 and `systemd/fake.Manager`.

- [ ] Unit tests on the fake: `TestUnitName`, `TestFakeRecordsStartProperties`.
- [ ] Smoke `TestSmokeTransientUnit`: start `/bin/sleep 30` as
      `miami-blockd-test-<rand>.service`, `State` shows active with a pid,
      `Stop`, `State.Present` false; `TestSmokeFailedUnitIsReset`: start
      `/bin/false`, wait, `Stop` leaves no failed unit.
- [ ] Implement with `NewSystemdConnectionContext`; properties: `Type=simple`,
      `ExecStart`, `Environment`, `Description`, `CollectMode=inactive-or-failed`,
      `KillMode=control-group`, `StandardOutput=journal`, `StandardError=journal`.
- [ ] Commit.

**Accept when** the smoke passes on the runner (systemd present) and
`New` returns `ErrNoSystemd` on a missing socket.

### Task T6: `internal/crypt` and `internal/ext4`

**Files:** create `internal/crypt/{crypt.go,crypt_test.go,smoke_test.go}`,
`internal/ext4/{ext4.go,freeze.go,ext4_test.go,smoke_test.go}`.

**Interfaces:** consumes `tool.Runner`; produces 3.6.

- [ ] Unit tests with `tool/fake`: `TestFormatArgv` (exact argv incl.
      `--pbkdf pbkdf2 --pbkdf-force-iterations 1000`), `TestOpenPassesKeyOnFd3`,
      `TestRotateUsesFd3AndFd4`, `TestExitCodeTwoIsErrBadKey`,
      `TestFsckCodes` (0,1,2 ok; 4,8 `ErrFsck`), `TestMkfsArgv`, `TestResizeArgv`.
- [ ] Smoke on a loop file (the runner has `losetup`; the test uses the
      `LOOP_CONFIGURE` ioctl directly to stay shell-free):
      `TestSmokeLUKSLifecycle` (format with Adiantum, open, mkfs, mount,
      write, `Freeze` then a write in a goroutine blocks until `Thaw`,
      `Resize` after growing the file and `LOOP_SET_CAPACITY`, `resize2fs`
      online shows the new size, `Rotate`, close, reopen with the new key,
      old key is `ErrBadKey`).
- [ ] Commit.

**Accept when** both smokes pass on both architectures (Adiantum is in the
runner kernels; the test fails, not skips, if the AF_ALG bind fails).

### Task T7: The `volume` kind, parameters, intent, controller, `noacl`

**Files:** modify `internal/volume/{kind.go,params.go,context.go}`,
`internal/intent/{intent.go,wire.go}`, `internal/csi/{controller.go,caps.go}`,
`internal/transport/nfs3.go`; tests beside each; add
`internal/intent/testdata/v1_volume.json`.

**Interfaces:** produces 3.8; `chunk.DefaultChunkSize`, `CheckChunkSize`,
`RoundUp` consumed.

- [ ] Tests: `TestVolumeParamsDefaults`, `TestVolumeParamsRejects`
      (table: chunk not power of two, cache below chunk, keep 0, interval
      under 1 m, maxAge under interval, cipher not in the allowed list
      `xchacha12,aes-adiantum-plain64` | `aes-xts-plain64`, kind volume with
      local transport (spec 4.1)), `TestContextRoundTripsVolume`,
      `TestDecodeRejectsVolumeKeysOnDirectory`, `TestIntentGoldenBytesUnchanged`,
      `TestVolumeIntentLoads`, `TestCreateVolumeRoundsUpAndRecordsSize`,
      `TestCreateVolumeMakesOnlyTheDirectory`, `TestDeleteVolumeRefusesLiveHolder`
      (a holder file younger than T), `TestVolumeAcceptsSingleNodeModesOnly`,
      `TestNFS3ProfileHasNoacl`, `TestNFS3AdoptsMountWithoutNoacl` (logged
      once, not foreign).
- [ ] Implement: `KindVolume` in `Valid`, `ID.Validate` text, `ParseParameters`,
      `NewContext` (derive chunk size, require `encryption` default on),
      `Encode`/`DecodeContext` with the `miami/` keys of 3.8, `Intent.Volume`
      and `Intent.Epoch` as `omitempty`, `CreateVolume` for the kind (round
      up, `miami/size`), `DeleteVolume` reads the holder, `capabilityProblem`
      per kind, `noacl` appended to `nfs3Profile` and removed from the
      required-token set for adoption with one info log on drift.
- [ ] Run `make check`, `make sanity`; commit.

**Accept when** the golden intent bytes are unchanged, every new
`InvalidArgument` has a test, and csi-sanity is green for both kinds.

### Task T8: `internal/blockd` and `miami blockd`

**Files:** create `internal/blockd/{backend.go,control.go,publish.go,run.go,client.go}`,
tests, `smoke_test.go`; create `cmd/miami/blockd.go`.

**Interfaces:** consumes 3.1 to 3.3, `holder.Checker`; produces 3.4 and
the subcommand `miami blockd --volume --store --cache-dir --control --size
--chunk-size --cache-bound --epoch --node [--read-only] [--timeout 30s]
[--dead-conn-timeout 60s]`.

- [ ] Unit tests (in-memory `chunk.Store` on a temp dir, real cache on a
      temp dir): `TestReadMissFetchesAndInserts`, `TestReadOfZeroChunkNeedsNoFetch`,
      `TestWriteDirtiesAndFlushSyncs`, `TestTrimMarksZero`, `TestPublishOrder`
      (chunks before manifest before current, asserted by a store wrapper
      that records calls), `TestPublishRefusedWhenHolderNotMine`
      (review focus 3: no manifest written, dirty kept, `Refused` true),
      `TestPublishRefusedWhenCurrentHasHigherEpoch`, `TestPublishNothingWhenClean`,
      `TestResumeFromIndexKeepsDirty`, `TestCreateFromCurrentManifest`,
      `TestCorruptChunkFailsThatReadOnly` (review focus 2),
      `TestGrowExtendsWithZeroChunks`, `TestPruneAfterPublish`,
      `TestStatusReportsBinaryHash`.
- [ ] Implement: `backend` over `cache` with fetch-on-miss from the store
      (bounded concurrency, a prefetch goroutine walking the manifest when
      `CacheBound >= Size`), `Flush` = `cache.Sync`, `WriteAt` with `fua` syncs
      that chunk; `publish` = for each boundary chunk `Put` under its hash,
      `WriteManifest`, holder check, `current` check, `Publish`, `ClearBoundary(true)`,
      `Prune(keep)`; on any failure `ClearBoundary(false)`.
- [ ] `Run`: open or create the cache (3.3), `nbd.Connect` with the three
      flags, `Serve` on the socketpair, `net/rpc` on the control socket
      (0600), stop on `Stop` or signal: `Disconnect`, `cache.Sync`, exit 0.
- [ ] Smoke `TestSmokeBlockdServesAndResumes`: run the program on a temp
      store, mkfs and mount the device, write with fsync in a loop, `kill -9`
      the program, restart it, mount again, data present; then `Boundary`,
      `Publish("request")`, and `restore`-free check that the store has a
      manifest whose chunks hash correctly.
- [ ] Commit.

**Accept when** unit and smoke pass with `-race`, and a `go vet`-clean
hot path (`ReadAt`/`WriteAt`) allocates nothing per request in a benchmark
(`BenchmarkReadHit` reports 0 allocs/op).

### Task T9: `internal/holder`

**Files:** create `internal/holder/{holder.go,judge.go}`, tests,
`e2e_transport_test.go` (tag `e2e`, nfs3 transport mode).

**Interfaces:** produces 3.7; reuses `internal/lock`'s probe-file clock
(export `lock.ProbeClock(dir) (time.Time, error)` if unexported today).

- [ ] Tests on a temp dir with an injected clock: `TestAcquireCreatesWithEpochOne`,
      `TestAcquireRefusesLiveHolder`, `TestTakeoverIncrementsEpoch`,
      `TestRenewRewritesAtomically`, `TestRenewDetectsLoss`,
      `TestReleaseOnlyIfMine`, `TestReadAgeUsesProbeClock`,
      `TestJudgementTable` (out-of-service → takeover; Ready, not in use,
      stale 30 s → takeover; not Ready, older than T, renewals unchanged for
      T on the local clock → takeover; not Ready, older than T, renewals
      changed → no), `TestEpochNeverDecreases`.
- [ ] Implement 3.7: holder content is JSON `{node, epoch, renewals}`,
      renewal writes `<name>.holder.tmp-<rand>` and renames; `Mine` re-reads
      and compares node and epoch.
- [ ] e2e-transport test `TestE2ENFS3HolderTwoClients`: two holders on two
      mounts of the export; the second cannot acquire; after the first
      stops renewing and the judgement says stale, the second takes over
      with epoch 2 and the first's `Renew` is `ErrLost`.
- [ ] Commit.

**Accept when** the suite and the nfs3 transport e2e pass.

### Task T10: Secrets, node flags, prerequisite probe

**Files:** modify `internal/csi/node.go`, `internal/reconcile/{reconciler.go,options.go}`,
`cmd/miami/node.go`; create `internal/prereq/prereq.go` and tests;
`internal/metrics` family `miami_node_capability{feature}`.

**Interfaces:** `reconcile.Stage`/`Publish` requests gain
`Key, PreviousKey []byte` (in memory, zeroed after use); `prereq.Probe(ctx,
Options{Sysfs, SystemdSocket, CacheDir}) Report` with `Report.Missing()
[]string`; node flags `--cache-dir` (default `<kubelet>/plugins/<driver>/cache`, so the
one kubelet hostPath covers it), `--bin-dir` (default
`<kubelet>/plugins/<driver>/bin`), `--systemd-socket` (default
`/run/systemd/private`), `--shared-dir` (repeatable; a directory other than
the kubelet's that the container and host see at one path), `--remediate`
(false until T15). A cache or bin dir outside those, or a cache dir whose
control socket path exceeds the 107-byte unix limit, makes the `cache-dir`
prerequisite missing.

- [ ] Tests: `TestNodeStageReadsKeyFromSecrets`, `TestNodePublishCarriesKeyForRestage`,
      `TestKeyNeverInLogsOrIntent` (a logging interceptor capture and the
      store file contain no key bytes), `TestProbeReportsEachMissingFeature`
      (fixture sysfs without nbd; unreachable socket; read-only cache dir),
      `TestCapabilityGauges`.
- [ ] Implement; the probe runs at startup and on every pass (cheap), and
      its report is what layer 1 of section 4 reads.
- [ ] Commit.

**Accept when** the tests pass and `miami node --help` lists the flags
with the defaults above.

### Task T11: `internal/item/volume`

**Files:** create `internal/item/volume/{volume.go,layers.go,observe.go,teardown.go}`,
tests with fakes for `tool`, `systemd`, `nbd` (an interface over `Devices`,
`Connect`), `blockd.Client` (interface), `holder`; `smoke_test.go`; modify
`internal/reconcile/{reconciler.go,converge.go,errors.go}` to register the
kind and the new conditions (`prerequisite-missing`, `holder-contended`,
`server-down`, `key-required`, `bad-key`, `generation-overdue`,
`publish-refused`).

**Interfaces:** consumes 3.2 to 3.7, T10's request keys; produces
`volume.New(Config) (*Item, error)` with `Config{Root, BinDir, CacheDir,
Sysfs string; Tools tool.Runner; Systemd systemd.Manager; NBD nbd.Devicer;
Dial func(socket) (blockd.Client, error); Holder holder.Acquirer; Mounter
mount.Mounter; Timeout time.Duration; Node string; Judge func(volumeID)
holder.Judgement}` and `(*Item).Generation(ctx, in) error` (freeze, boundary,
thaw, publish) for T12.

- [ ] Reconcile-style unit tests (`internal/item/volume/item_test.go` with
      the fakes): `TestFirstStageFormatsAndMounts` (layer order asserted
      from the fakes' call logs), `TestRestageAfterPluginRestartAdoptsEverything`
      (unit active, device present, mapping present, mount present: no
      calls but observations), `TestNoKeyIsKeyRequired`, `TestBadKey`,
      `TestPreviousKeyRotatesThenOpens`,
      `TestServerDownWhenUnitFailed`, `TestPrerequisiteMissingRefusesBeforeAnything`
      (review focus 5: no unit, no cache dir), `TestHolderContended`,
      `TestGrowRunsThreeSteps`, `TestTeardownOrder`,
      `TestTeardownSucceedsWhenPublishRefused` (review focus 4),
      `TestTeardownSucceedsWhenTransportDown`, `TestRebootSurvivingCacheReusedOnlyIfCurrentMatches`,
      `TestPublishSourceIsStagePath`, `TestGenerationSequence` (freeze →
      boundary → thaw → publish, thaw always runs even when boundary fails).
- [ ] Implement section 4; the unit's `ExecStart` is
      `[<bin-dir>/miami-<hash>, "blockd", flags...]`; binary hash is the
      build's version string.
- [ ] Smoke `TestSmokeVolumeStack`: the item against the real kernel, a
      temp store, the runner's systemd: stage, write, generation, unstage;
      then stage again on the same store from `current`, data present, cache
      directory absent after unstage.
- [ ] Commit.

**Accept when** the suite passes with `-race` and the smoke passes on both
architectures.

### Task T12: Generations in the reconciler

**Files:** modify `internal/reconcile/{loop.go,options.go}`; create
`internal/reconcile/generation.go`, tests; `internal/kube` gains
`PVAnnotation(ctx, pv, key) (string, error)`.

**Interfaces:** consumes `(*volume.Item).Generation`, `blockd.Status` via
the item; `Options.Generations` (interval floor 1 m); the PV annotation
`miami.csi.greghaskins.com/publish` (any new value requests one).

- [ ] Tests with the fake clock: `TestIntervalTriggersGeneration`,
      `TestWatermarkTriggersEarly`, `TestAnnotationTriggersOnce`,
      `TestNoGenerationWhenClean`, `TestOverdueConditionAfterMaxAge`,
      `TestGenerationFailureRetriesNextTick`, `TestGenerationsSerializedPerVolume`.
- [ ] Implement: per staged volume a timer on `Options.Now`; the generation
      runs in its own goroutine under a deadline and never blocks the pass.
- [ ] Commit.

### Task T13: Emergency detach and orphan drain (decision 52)

Decision 52 removes the timed takeover: no node takes over a holder because
it is old or its node is NotReady.

**Files:** modify `internal/kube/kube.go` (+`NodeStatus(ctx, name) (NodeInfo,
error)` with `Ready bool; OutOfService bool` and a PV annotation read),
`internal/reconcile/holderjudge.go` (new), `internal/reconcile` orphan drain,
tests, `internal/csi/envtest_test.go`.

**Interfaces:** produces the `Judge` func for `volume.Config` (the item keeps
its own self-fenced rule); consumes `holder.Judgement`.

- [x] Judge: Takeover only on (a) the PV annotation
      `miami.csi.greghaskins.com/force-detach` equal to the holder file's
      `<node>@<epoch>`, or (b) the `node.kubernetes.io/out-of-service` taint
      on the holder's node; never on age or readiness. An event names the
      abandoned epoch. Tests: `TestJudgeAnnotationMatchingHolder`,
      `TestJudgeAnnotationStaleEpochIgnored`, `TestJudgeOutOfServiceTaint`,
      `TestJudgeNeverOnAgeOrNotReady`, `TestJudgeNodeReadErrorIsNoTakeover`;
      envtest `TestNodeStatusReads`.
- [x] Orphan drain: at start and on every pass, a holder file naming this
      node for a volume with a surviving cache that is not staged here is
      drained: start the server (no key, no mount), publish, verify as in
      decision 52, stop, delete the cache, release. Failure retries next
      pass. Tests with the item fakes, including a drain refused by the
      fence (abandoned by the operator: discard and release nothing).
- [x] Holder renewal by touching, not rename-over: `Renew` keeps its
      read-and-check, then `futimens` (`UTIME_NOW`, so the NFS server sets
      the time) on the descriptor opened when the holder was loaded; ESTALE
      or a changed inode is `ErrLost`. A takeover renames a new inode in, so
      a stale node's late renewal can no longer regress the file to its own
      `<node>@<epoch>`. A forced detach aims at a live, renewing node, which
      made that race routine. Test: a renewal racing a takeover leaves the
      taker's file intact.
- [x] The force-detach annotation is never removed by the driver; the judge
      re-applies it idempotently while it matches the holder file.
- [x] The drain takes the stage's cache-reuse decision (`current`'s epoch ==
      the index epoch, or `current` names the index base, or no `current` and
      no base) before it serves the cache; otherwise it discards and
      releases nothing. A drained generation is a crash image (flushed writes,
      no freeze): restore replays the journal.
- [x] `DeleteVolume` refuses while any holder file exists (the timed
      `refuseLiveHolder` rule goes); the emergency detach is the way through.
      Landed in branch `m2/delete-refuses-any-holder`.
- [x] The `takeoverTimeout` StorageClass parameter and the node's
      `-takeover-timeout` flag no longer mean anything: reject the parameter
      for new classes with a message naming decision 52 and remove the flag
      (T22 documents that a NotReady node is never taken over).
- [x] RBAC: node ClusterRole gains `nodes` `get` and `persistentvolumes`
      `get` (annotation read).
- [x] Commit.

Landed in T13 (the node's wiring is T13w): `reconcile.NewEmergencyJudge`
(`Judge` is `volume.Config.Judge`, `TookOver` is `volume.Config.Takeover`; both
take the holder the item read, so the judge needs no path); `item.Drainer`
(`Orphans`, `Drain`), implemented by the volume item over a drain record it
keeps beside each cache (`<cache dir>/<name>.drain.json`: the intent without
its publishes, written before a server starts) because the cache directory
alone does not say where the volume is; the reconciler drains at its start
pass and on timer passes. `holder.Judge` (the timed rules) is removed. A cache
left by a build from before this change has no drain record and is not drained
until it is restaged (nothing is deployed yet). The drain applies decision 52's
proof rule: a holder file naming another node, a store that moved past the
cache, or a deleted volume (the PV is NotFound; a missing store directory alone is not proof) discards with `GenerationLost`; an absent holder
without proof keeps the cache and warns once.

### Task T13w: Node wiring of the volume kind

No earlier task constructs the `volume` item in the node binary: T11 built
and registered the kind in the reconciler, T12 and T13 produce the
generation timers, the judge and the orphan drain, all behind seams.

**Files:** modify `cmd/miami/node.go` (and its flags/tests),
`internal/reconcile` options as needed.

- [ ] Construct `volume.New(Config)` with the real clients: the tool runner,
      the systemd private-socket manager, nbd, `blockd.Dial`, the holder,
      the mounter, `BinDir`, `CacheDir`, `Root`, `Sysfs`, the node name,
      the timeouts; register it for the `volume` kind.
- [ ] `Config.VolumeExists` = `kube.Client.PVExists` (T13; the drain's only proof of a deleted volume).
- [ ] `Config.Judge` = T13's judge (node + PV reads); `Config.Event` = the
      reconciler's event recorder; T12's PV annotation reader; T13's orphan
      drain enabled.
- [ ] Refuse to start the `volume` kind (not the whole node) when the
      prerequisite probe (T10) fails, reporting `prerequisite-missing`.
- [ ] Tests: flag parsing; a fake-backed construction test asserting every
      Config field is set (no nil seam reaches production); smoke covers the
      real stack via T11's `TestSmokeVolumeStack` and the e2e (T21).
- [ ] Commit.

### Task T14: Probe and conditions for volumes

**Files:** create `internal/probe/probe.go`, tests; modify
`internal/reconcile/converge.go` (conditions), `internal/kube/events.go`
(reasons `ServerRestarted`, `PublishRefused`, `GenerationOverdue`,
`TakeoverStarted`, `KeyRequired`, `GenerationLost`).

- [ ] Tests: `TestProbeReadsFirstSectorDirect`, `TestTwoFailuresMarkFailed`,
      `TestProbeTimeoutCountsAsFailure`, `TestConditionsIncludeNewOnes`,
      `TestEventTextFixedPerReason`.
- [ ] Commit.

### Task T15: Remediation — dropped from M2 (decision 53)

Failures are reported, not remediated: T14's condition, event and probe
metric carry it, with a message saying to restart the pod once the store is
back. No `internal/remediate`, no pod `list`/`delete` RBAC, no
`ValidatingAdmissionPolicy`, no `--remediate` flag. T17 ships no
`policy-remediate.yaml`.

### Task T16: Expansion

**Files:** modify `internal/csi/{controller.go,node.go,caps.go}`,
`deploy/kustomize/base/deployment-controller.yaml` (csi-resizer, digest
pinned, own commit), tests.

- [ ] Tests: `TestControllerExpandRoundsUpAndRecordsSize`,
      `TestControllerExpandRefusesShrink`, `TestNodeExpandGrowsStack`,
      `TestExpandCapabilities`.
- [ ] Commit.

### Task T17: Deploy

**Files:** modify `deploy/kustomize/base/daemonset-node.yaml` (hostPaths:
`/dev` HostToContainer, `/run/systemd`, the cache dir `DirectoryOrCreate`,
`/sys` read-only), RBAC from T13 (T15 dropped, decision 53),
examples `miami-remote-vol-rwo.yaml` and `miami-remote-vol-rwo-plain.yaml`
(node-stage and node-publish secret templates on `${pvc.namespace}`,
`allowVolumeExpansion: true`), `deploy/policy_test.go`, `deploy/testdata/base.golden.yaml`,
`overlays/e2e`.

- [ ] The example `volume` classes set `csi.storage.k8s.io/fstype: ext4`:
      under `fsGroupPolicy: ReadWriteOnceWithFSType` kubelet applies a pod's
      `fsGroup` only when the PV carries an fsType, and the provisioner sets
      it only from that parameter (I4 makes the driver accept it). The
      `directory` classes must not set it. Test it, and have the e2e assert a
      pod's `fsGroup` owns the mounted root of a `volume`.
- [ ] Tests: `TestNodeHostPaths`, `TestNodeRBACExact` (no pod list/delete, decision 53),
      `TestExampleVolClassesSetBothSecretRefs`, `TestExampleVolClassesSetExt4FsType`,
      golden regenerated.
- [ ] `make deploy-check` on kind: both pods Ready; `hack/kind.sh` asserts
      the node pod sees `/run/systemd/private`.
- [ ] Commit.

### Task T18: `miami restore`, `miami import`, the drill

**Files:** create `cmd/miami/restore.go`, `cmd/miami/import.go`,
`internal/chunk/assemble.go`, tests; `docs/restore.md` (the five-line
drill, verbatim from the spec 5.1); `hack/restore-drill.sh` (the drill as a
script the e2e runs).

- [ ] Tests: `TestRestoreVerifiesBeforeWriting` (review focus 1 and 2: a
      bad chunk aborts with no output file), `TestRestoreUsesNamedManifest`,
      `TestRestoreBoundedMemory` (a 1 GiB sparse volume on a tmp store at
      peak RSS under 64 MiB, measured via `runtime.MemStats`),
      `TestImportChunksImageAndPublishesEpochZero`, `TestImportSkipsZeroChunks`,
      `TestDrillScriptMatchesRestore` (both outputs byte-identical).
- [ ] Commit.

### Task T19: Metrics and events

**Files:** modify `internal/metrics/{families.go,registry.go}`, tests.

Families: `miami_volumes_served{state}` (serving, outdated-binary, down),
`miami_generation_age_seconds{volume}`, `miami_generation_dirty_bytes{volume}`,
`miami_generation_publish_duration_seconds`, `miami_generation_publishes_total{result}`
(ok, refused, failed), `miami_cache_bytes{volume,state}` (clean, dirty),
`miami_chunk_fetch_duration_seconds`, `miami_chunk_fetch_errors_total`,
`miami_node_capability{feature}`, `miami_remediations_total{result}`.

- [ ] Tests: `TestVolumeLabelIsAPVName` (rejects anything not matching
      `IsPVName`), `TestFamiliesRegistered`; smoke scrape in T11's smoke.
- [ ] Commit.

### Task T20: `miami bench` and gate G1

**Files:** create `cmd/miami/bench.go`, `internal/bench/{phases.go,stack.go}`,
`hack/bench.sh`; CI smoke step on amd64 logging the table.

**Interfaces:** `miami bench --stack nbd|loop|none --dir <local dir> --store
<store dir> --size 8Gi --json` sets up the stack (nbd: a chunk store at
`--store`, `blockd` as a child process, LUKS, ext4; loop: a file at `--dir`,
loop, LUKS, ext4; none: `--dir` as is), runs the phases of spec 3.2 that
need no external tool (create 2000 × 1 KiB; 5000-file tree write; stat
sweep; 1000 × create+fsync; 1000 × 4 KiB in-place+fsync; 512 MiB sequential
write and read; rm -rf), two rounds, prints a table and the ratio
`nbd/loop`.

- [ ] Tests: `TestPhasesRunOnTmpDir`, `TestTableFormat`.
- [ ] `hack/bench.sh`: runs `nbd` and `loop` on the host as root and prints
      both tables and the ratios; CI logs it (no threshold in CI).
- [ ] **Gate G1 (owner, outside CI):** run the image's `miami bench --stack
      nbd` and `--stack loop` on a Raspberry Pi 4 worker with `--store` on a
      local directory and again with `--store` on the nfs3 transport; attach
      both tables to PR #41. Pass: every phase within 1.5×. On failure, stop
      and report per spec section 7.
- [ ] Commit.

### Task T21: e2e

**Files:** `test/e2e/volume_test.go`, `volume_outage_test.go`,
`volume_takeover_test.go`, `volume_restore_test.go`, `volume_expand_test.go`;
modify `hack/e2e-kind.sh` (`modprobe nbd`, the cache dir on kind nodes,
the Secret for the key in the test namespace), `overlays/e2e`, `ci.yml`.

- [ ] Tests, one per exit criterion 1 to 9 and 6a (`TestVolumeExt4Semantics`,
      `TestVolumeSurvivesReschedule`, `TestVolumePluginRestart`,
      `TestVolumeServerKill`, `TestVolumeNASOutageNoHungTask` (gate G2:
      server stopped for major timeout + 30 s; assert no `blocked for more
      than` in `docker exec <node> dmesg`), `TestVolumeTakeover`,
      `TestVolumeOrphanDrain`,
      `TestVolumeNodeRebootReusesCache`, `TestVolumeRestoreDrill`,
      `TestVolumeExpandOnline`), all in the nfs3 variant; the `local`
      variant skips them with a reason.
- [ ] Commit.

**Accept when** `ci-ok` is green with the nfs3 legs on both architectures,
and criterion 11 (M1 suites) still passes.

### Task T22: README and docs

**Files:** `README.md` (the matrix row for volumes, parameters table,
prerequisites, limitations, the restore drill pointer, the `-plain`
naming), `docs/design/2026-10-30-architecture-brief.md` (append any
decision a task amended; never rewrite), this plan's status.

- [x] Fold decisions 51 and 52 and the `--disable-keyring` note into the
      spec's section 4 (4.4 restart model, 4.6 exclusivity, the failure
      table: a NotReady node is never taken over). Done, with 53, the size
      record, the zero chunk and the drain; decisions 54 to 58 record where
      the merged code differs from the design's text.
- [x] Operator runbook (`docs/operating-volumes.md`) for a volume that cannot leave its node (a dead
      node, or an unstage failing permanently, e.g. a corrupt cache index):
      delete the pod (force if needed); set the PV annotation
      `miami.csi.greghaskins.com/force-detach` to the holder's
      `<node>@<epoch>` named in the warning; wait for the attach to move,
      which depends on the attach-detach controller's forced detach after
      its unmount wait (about six minutes, also on a healthy node once the
      pod object is gone); the new node's judge takes over, the old node's
      next unstage retry finds the holder taken, reports `GenerationLost`
      and finishes. The out-of-service taint does the same for every
      volume of a dead node. State plainly what is lost: whatever the old
      holder had not published. Also: a surviving cache with no holder file
      and no proof it was given up is kept and logged once per process; the
      removing the cache directory by hand gives it up (the force-detach annotation
      acts on a holder file, and there is none); and a cache
      from a build before the drain record existed is not drained until
      restaged.
- [ ] Reviewed by the orchestrator; commit.

## 7. Risks and the check that retires each

| Risk | Check |
|---|---|
| nbd netlink device creation differs on the runner kernel | T3 smoke on both runner kernels, first wave |
| The transient unit cannot reach the kubelet dir path on kind nodes | T11 smoke runs on the runner's systemd; T21's first test on kind |
| O_DIRECT on the cache dir's filesystem (overlay on kind nodes) | T4 says so when unsupported; the e2e overlay puts the cache dir on a kind `extraMount` to the host |
| Adiantum missing in a runner kernel | T6 fails loudly; the plan's fallback is `aes-xts-plain64` in CI only, never as the default |
| `RECONFIGURE` size change not applied | T3 smoke asserts `BLKGETSIZE64` after grow; fallback the ioctl path |
| Generation hashing cost on a slow node | T20's table includes publish duration; the era target remains the named optimisation |
| Gate G1 fails | stop; the spec names `ublk` as the fallback and the owner decides |

## 8. Out of scope for M2

RWX by re-export (M3), snapshots and clones (M4), read-only followers,
encrypted shared directories, SMB, the era target, `ublk`, any migration of
real consumers (a deployment's own spec).
