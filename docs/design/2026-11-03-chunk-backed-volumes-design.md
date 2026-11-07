# Chunk-backed volumes: the design for M2 and the re-plan

**Status:** Approved by the owner on 2026-11-03; the plan is
`2026-11-03-m2-implementation-plan.md`
**Created:** 2026-11-03
**Reads with:** `README.md`, `docs/operating-volumes.md`,
`2026-10-30-architecture-brief.md` (decisions 1 to 35 and, after this
document, 36 to 59), `2026-11-01-goals-ahead-of-the-design.md`
**Supersedes:** the brief's section 1 (purpose), 4.7 (encryption), 4.8's
single-node limits, 5.2 (node-local persistent), 5.3 (S3 service), lesson 8
(FUSE) and section 10 (milestones). The brief's text is kept; the decision log
records what replaced it.

This document is the output of the M2 design pass. It records the measurements
that overturned parts of the brief, the design they led to, the decisions in
the form the brief's log uses, and the milestone re-plan. A builder should be
able to plan M2 from it without going back to the owner.

Nothing in it is specific to one environment. "The NAS" is whatever NFSv3
server a cluster points a transport at; "the offsite copy" is whatever the
NAS's own backup job writes.

## 1. Purpose, restated

The README states the goal: make bare-metal hardware behave like a proper
Kubernetes storage layer, with flexibility across item kinds and locations,
strong RWO and RWX semantics when a workload needs them, and encryption at
rest, all mostly transparent to off-the-shelf workloads. The owner's ideal, in
one sentence from the design pass: *an encrypted disk that can safely and easily
be backed up to offsite storage and plays nice with workloads; the NAS used as
if it were a cloud block store.*

The brief's opening gave a narrower reason to exist: NFSv3 loses POSIX mode bits
and locking, so give each pod a real ext4 over a loop device whose backing file
lives on the NAS. Section 3 below shows that premise is false on the NAS
evaluated, and that the loop-over-NFS stack is the one with the worst failure
shape. The reason images are still needed is a different one, metadata write
throughput, and the design that follows gives that without putting any kernel
filesystem on top of NFS.

Three principles join the brief's section 3:

- **Transparency is a requirement, not a nicety.** Any semantics a workload
  must know about is a cost the design has to justify in writing. This design
  has exactly one such cost (section 4.10).
- **For a single-writer volume, the NAS is a backup target, not a data path.**
  The live data is on the node; the NAS holds consistent generations of it.
  Nothing the NAS does during an outage can stall or corrupt a running pod.
- **One pattern.** Every RWO and RWX volume is the same kind underneath. The
  README's three item types become two implementations (section 4.1), and
  blobs become a documented layout rather than a kind.

## 2. Goals, non-goals, anti-goals

**Goals**

1. Encryption at rest by default on every volume, with the NAS and the offsite
   copy seeing only ciphertext, and the key never leaving the node's kernel.
2. An offsite copy that is incremental (unchanged data is never re-uploaded)
   and point-in-time (every restorable state is one the workload could have
   crashed at), produced by the NAS's own file-level backup job with no cluster
   component in the backup path.
3. A restore drill that needs coreutils and cryptsetup on any Linux machine.
4. Strong RWO: a workload that asks for RWO never shares its data with another
   writer, including across node partitions.
5. Mobility: any volume can be staged on any node; a node's death loses at
   most one generation interval of writes, never consistency.
6. Metadata write throughput at local-disk speed for RWO volumes.
7. Volumes larger than a node's disk.
8. The M1 directory item kept as it is, for shared plain directories.

**Non-goals**

- An S3 or any object API in the driver. A content-addressed blob store is a
  layout on a volume (section 5.4), served by whatever application wants it.
- Encrypted shared directories through a FUSE layer (section 3.4 records why).
- SMB as a transport (section 3.5).
- Read-only replicas that follow generations (section 8, unscheduled).
- Node-local persistent volumes with a move operation: the pattern makes every
  volume mobile, so the brief's 5.2 has no consumer left.

**Anti-goals** (what would make this a failure even if it worked)

- A kernel filesystem stacked over NFS in any pod's data path.
- A daemon whose restart restarts consumers.
- A backup whose restore needs the driver's binary.
- A second on-disk ciphertext format.

## 3. Facts the design rests on

Each fact is environment-neutral here; the measurements were taken on a real
NAS and a real cluster of Raspberry Pi 4 and amd64 workers on 2026-11-02, and
the raw results are kept where they were taken.

### 3.1 POSIX fidelity of a plain NFSv3 directory

On the NAS evaluated, with the M1 nfs3 profile: exec bits set by `chmod`
survive and the script runs; modes 600 to 777, setgid 2775 and sticky 1777 are
kept; `open(O_CREAT, 0777)` as git does for a 100755 blob keeps 755; symlinks,
hard links (same inode) and FIFOs work; rename over existing and a second
`O_EXCL` create behave; a git clone shows zero phantom mode diffs with
`core.fileMode=true`. Every created file is owned by the export's mapped
identity whatever uid wrote it, and `chown`, even to self, is EPERM. The NAS
answers the NFSv3 ACL sideband with EPERM instead of "not supported", which
makes GNU coreutils `install -m`, `sed -i` and `cp` with ACL copying fail while
plain `chmod` never touches the sideband; the `noacl` mount option turns that
into the chmod fallback.

**Consequence.** The brief's founding premise ("NFSv3 loses mode bits") is
false. The one POSIX gap of a directory item is uid fidelity. `noacl` joins the
profile (decision 46).

### 3.2 Metadata throughput: directory, loop image, local disk

On a Raspberry Pi 4 worker, round-2 seconds:

| Phase | local disk | NFSv3 directory | loop image on NFSv3 |
|---|---|---|---|
| create 2000 × 1 KiB files | 5.9 | 78.2 | 6.9 |
| git init + add + commit, 2000 files | 2.3 | 156.3 | 1.7 |
| branch + 200 edits + two checkouts | 1.7 | 44.3 | 2.4 |
| untar 5000 small files | 1.6 | 183.9 | 1.6 |
| warm git status | 0.04 | 0.12 | 0.03 |
| 1000 × create, write 4 KiB, fsync | 4.4 | 45.5 | not measured |
| 1000 × overwrite 4 KiB in place, fsync | 3.9 | 42.4 | not measured |

**Consequence.** A directory on this NAS pays about 38 ms per file create and
about 42 to 45 ms per fsync, whatever creates it; a loop image is
indistinguishable from local disk. Images exist for one reason, metadata write
throughput, and reads on a directory are fine.

### 3.3 What the NAS backup job does

The NAS's own backup job is a file-level incremental mirror: one object per
file under the file's own name, changed files re-uploaded whole, unchanged files
untouched, deletions not propagated in its default mode, no encryption of its
own. The path components between the share and the selected folder are
dropped, so the selection must be fixed once (the share, or one folder per
namespace with unique basenames) and never changed. A 50 MiB image rewritten
every 30 s produced one full-size object version per run; 1,522 files with
128-character encrypted names arrived intact with a 243-character longest key.

**Consequence.** Per-file ciphertext at stable names is the shape the job
rewards; a changing image is the shape it punishes. Backup membership is
decided by which export a class points at (decision 12 confirmed). Anything
frequently rewritten and small (a holder file) costs one object version per
run and is accepted.

### 3.4 A FUSE encryption layer over NFSv3, measured

gocryptfs (XChaCha20-Poly1305, 184 MB/s single-thread on the Pi against
52 MB/s for AES-GCM without AES instructions) mounted over the same NFSv3
directory and over local disk, Pi 4, round-2 seconds:

| Phase | NFS dir | NFS dir + gocryptfs | local | local + gocryptfs |
|---|---|---|---|---|
| create 2000 files | 79.2 | 91.8 | 6.8 | 12.5 |
| git add + commit | 169.8 | 125.0 | 1.5 | 8.5 |
| untar 5000 files | 165.8 | 200.8 | 0.5 | 8.2 |
| SQLite, 100 transactions | 12.9 | 17.9 | 0.8 | 1.6 |
| 1000 × create + fsync | 45.5 | 49.8 | 4.4 | 6.8 |
| 1000 × 4 KiB in-place + fsync | 42.4 | 45.4 | 3.9 | 6.2 |
| 512 MiB sequential write / read | 7.7 / 5.1 | 11.5 / 8.9 | 7.8 / 4.3 | 8.2 / 7.5 |

**Consequence.** Over NFS the FUSE layer is affordable (the NAS round trips
dominate); over local disk it costs five to fifteen times on metadata-heavy
work, because local disk is fast enough for the FUSE hop to dominate. A
FUSE layer is therefore the wrong tool for a local store, and once the design
moved the live data to the node (section 4) it had no remaining use. It also
carries the daemon-lifetime cost (section 3.6). rclone's crypt format was
ruled out on a different ground: it cannot rewrite a block in place without
reusing a nonce, so its mount re-uploads a whole file on every flush, which is
the wrong shape for a database.

### 3.5 SMB on the NAS evaluated

Anonymous NEGOTIATE requests: SMB 3.1.1 offered, with 2.0.2 to 3.0.2 behind
it; SMB1 off; leasing, directory leasing and multichannel yes; AES-128-GCM
encryption yes; persistent handles no; SMB3 POSIX extensions no. Measured
through the kernel cifs client on the Pi: file creates and git commits about
five times faster than NFSv3, fsync-per-file identical (46 s per 1000, the
NAS's per-sync floor on either protocol), SQLite failed on its first statement
with "database is locked", a recursive delete left part of the tree behind in
both rounds, and the fidelity check failed on exec bit, symlink and FIFO.

**Consequence.** SMB cannot carry a directory item, and without persistent
handles its server-side locks do not survive a server restart, so it cannot
replace the holder design either. Closed for this milestone; the transport
layer remains the extension point if a server with the POSIX extensions ever
appears.

### 3.6 What a hung task on a loop-over-NFS stack actually was

The kernel's hung-task detector reports only tasks in plain uninterruptible
sleep; killable waits are excluded. A loop device's flush is an `fsync` of the
backing file, which waits on page-cache writeback in plain uninterruptible
sleep, and ext4's journal thread and the application's own `fsync` queue behind
it. On a *hard* NFS mount that wait is the outage's length. On a *soft* mount
it is bounded by the major timeout, which the M1 profile sets at 180 s
(decision 30), above the detector's 120 s default. So any kernel filesystem
over a loop over this NFS profile reports hung tasks during an outage longer
than two minutes, and over a hard mount it wedges until the server returns.
The incident that first raised this was on hard mounts; the postmortem that
attributed it to soft ones has been corrected.

**Consequence.** No kernel filesystem goes over NFS in this design. The anti-
goal in section 2 is this fact.

### 3.7 What a FUSE daemon or block server needs to outlive the plugin pod

A process started from the plugin container cannot outlive the container by
cgroup means alone: `systemd-run --scope` (the trick kubelet uses for mount
helpers) moves the process to its own cgroup but not out of the container's
PID namespace, and when the container's PID 1 exits the kernel kills the
namespace. The two honest ways out are `hostPID` on the DaemonSet, or starting
the daemon as a transient systemd *service* over the host's private socket
(`/run/systemd/private`, root only, no D-Bus daemon needed), in which case PID 1
runs the binary from a path on the host and the daemon has no tie to the
container at all. A dead FUSE or nbd server leaves its mount answering
`ENOTCONN` or I/O errors; nothing mainline hands a live connection to a new
process.

**Consequence.** The block server of section 4.3 is a transient service, and
its upgrade rule is "at the next stage of that volume" (decision 41).

## 4. Architecture

### 4.1 Item kinds

Two implementations behind one driver:

| Kind | What it is | Sharing | Encryption | Where the live data is |
|---|---|---|---|---|
| `directory` | M1's item: a subdirectory of the transport bound straight into the pod | RWX (and RWO by attach only, as M1) | none | on the NAS |
| `volume` | this document: ext4 over dm-crypt over a chunk-backed block device | RWO strong; RWX by re-export (M3) | LUKS2, on by default | on the node, with generations on the NAS |

The README's "opaque blobs" row becomes a layout on a `volume` (section 5.4).
The brief's "image" item (ext4 over a loop over a file on the NAS) is not
built; every reason to want it is served by `volume`.

A `volume` requires a remote transport (`nfs3` today): a chunk store on a
`local` transport would be a backup of node-local data on the same node, which
is meaningless, so `kind: volume` with `transport.kind: local` is
`InvalidArgument` at `CreateVolume`. The `local` transport stays for directory
items.

StorageClass names keep decision 35's shape, `miami-<location>-<type>-<sharing>`,
with type `dir` or `vol`. Encryption is the default for `vol`, so the suffix
flips: a `vol` class with `encryption: "off"` carries `-plain`
(`miami-remote-vol-rwo-plain`); a `dir` class has no encryption capability and
no suffix. Decision 35's `-crypt` token is withdrawn before it was ever used.

### 4.2 The chunk store on the transport

A `volume` has one directory on its transport, at decision 20's path
`<transport>/<subdir>/<name>/`, holding:

```
<name>/
  chunks/<aa>/<sha256>     one chunk of the ciphertext device, named by its SHA-256
  manifests/<epoch>.<n>    one generation: header lines, then one chunk hash per line in device order
  current                  the name of the latest complete manifest, replaced by rename
<name>.holder              the holder file of brief 4.5, beside the directory
<name>.holder.epoch-<N>    the durable claim of epoch N (the two newest are kept)
<name>.size                the grown size in bytes, beside the directory; written only by expand
```

- **Chunks** are fixed-size pieces of the block device as dm-crypt wrote
  them: ciphertext, never plaintext. The chunk size is a class parameter
  (`chunkSize`, a power of two from 1 MiB to 256 MiB) fixed at create and
  recorded in every manifest. When the class does not set it, `CreateVolume`
  picks the smallest power of two of at least 4 MiB that keeps the manifest
  to at most 65,536 lines (4 MiB up to 256 GiB, 64 MiB at 4 TiB), so a
  manifest never exceeds about 4 MB and a generation's fixed cost stays
  bounded; a class value that would exceed the line cap is `InvalidArgument`.
  A requested size is rounded up to a whole number of chunks and the rounded
  capacity is reported, on create and on expand. The size record (`<name>.size`,
  decimal bytes and a newline) exists because a PV's attributes are immutable:
  expand raises it before answering and it only grows, a stage takes the
  largest of the context's size, the record and the size in the manifest
  `current` names, and a record that cannot be parsed stops the stage rather
  than be repaired (a smaller size must never come from a record that could
  not be read). A chunk is written to a temporary
  name beside its final name and renamed into place; a chunk that already
  exists is never rewritten. The all-zero chunk has one well-known name and is
  stored once per volume, so unwritten and discarded regions cost nothing. The
  store guarantees it (`EnsureZero`): every writer (the block server before it
  publishes a manifest that names it, `miami import`) stores it as a sparse file
  of one chunk first, so the restore drill of 5.1 finds it like any other
  chunk and a prune cannot leave a manifest naming a chunk that is missing.
  The two-character fan-out keeps directories small for a terabyte volume.
- **A manifest** is plain text: `#`-prefixed header lines (`volume`, `generation`,
  `size` in bytes, `chunk` size, `holder` epoch, `time`, `format 1`), then
  exactly `size / chunk` hash lines in order. Its name is
  `<holder epoch>.<sequence>`, and generations are ordered by epoch first.
- **`current`** holds one manifest name and is replaced by writing a
  temporary file and renaming over it. A reader opens it fresh every time, so
  the attribute cache never hides a change.
- **Retention.** The driver keeps the last `generations.keep` manifests
  (default 3) and removes chunks that no kept manifest references, on the
  node that holds the volume, after a successful publish. The offsite copy's
  retention is the backup job's and the bucket's; the mirror's default mode
  never deletes, so history accumulates there until a lifecycle rule ages it.
- **Integrity.** A chunk's name is its hash, so a restore verifies every chunk
  before assembling anything, and a truncated or corrupt object is caught.
- **Ordering on publish.** Chunks, then the manifest, then `current`. A
  mirror that runs mid-publish can copy a manifest whose chunks it has not
  listed yet; they arrive on its next run. A restore from offsite therefore
  takes the newest manifest whose chunks are all present, and the drill says
  so.

NFSv3 makes `O_EXCL` create, `link(2)` and rename over existing atomic on the
server (RFC 1813), which is all the store needs; it has no conditional rename,
which is why the fencing token of 4.6 exists.

### 4.3 The block server

`miami blockd` is a subcommand of the one binary. One instance per staged
volume serves the volume as an nbd device backed by a local chunk cache:

- **Lifetime.** It runs as a transient systemd service on the host, started
  over `/run/systemd/private` (mounted into the node pod), executing a copy of
  the driver's own binary that the node plugin places at
  `<kubelet-dir>/plugins/<driver>/bin/miami-<content-hash>` at startup. The
  service's lifetime is the volume's stage, not the plugin pod's. The plugin can
  be upgraded, crash or be rescheduled without the device noticing. The unit is
  started with `Restart=on-failure` and a one-second delay, its output goes to
  the host journal, and its name is `miami-blockd-<volume>.service`. A server
  that dies is restarted by systemd and re-points the same device (4.4, decision
  51); a server that is stopped on request (unstage) disconnects it. The
  server's own binary is upgraded only when the volume is next staged; the
  reconciler reports volumes served by an older hash in a metric, and a
  rollout that must reach every server is a drain. A node without systemd
  cannot stage a `volume` (fail closed, reported like a missing kernel module).
- **The device.** The nbd kernel module; devices are created through the
  netlink interface so the count is not bounded by the module's static
  default; the connection carries a dead-connection timeout and a request
  timeout so a dead server gives I/O errors, never an unbounded wait. The
  reconciler observes devices through sysfs (the backend string, which names
  the volume) and the unit through systemd; the kernel and systemd are the
  truth, as the brief's principle says. The sysfs pid is the connecting
  process's and survives a re-point, so it is not a liveness signal: liveness
  is the unit being active and its control socket naming the volume's device.
- **The cache.** A directory on a node-local path (`-cache-dir` of `miami
  node`, default `<kubelet-dir>/plugins/<driver>/cache`; it must be on local
  disk, never on a transport, and the plugin's container and the host must see
  it at the same absolute path, so it lies under the kubelet directory or a
  `-shared-dir`, which is why it needs no hostPath of its own), one file per
  chunk, read and written with O_DIRECT where the file system allows it (not on
  tmpfs) so the chunk files are not cached twice beneath ext4's own page cache. Reads of a chunk not in the
  cache fetch it from the transport; a background prefetch walks the current
  manifest so a volume that fits fills its cache after stage. The cache bound
  is a class parameter (`cacheSize`, default the volume's size); clean chunks
  are evicted least-recently-used; dirty chunks are pinned until published.
  When a cache smaller than the volume fills with dirty chunks and a generation
  cannot be published, writes fail with an I/O error rather than block, so the
  filesystem goes read-only and the failure is visible (section 4.9), and no
  kernel thread waits uninterruptibly on the NAS.
- **Dirty tracking.** The server is the write path, so it knows the dirty
  set exactly: a small on-disk index (`index.json`) in the cache directory recording the
  base manifest the cache was built from and the dirty chunk list, so a server
  restarted on the same node after a crash resumes with its unpublished dirty
  chunks intact. This is why the brief's era target is not needed (decision
  44). A write that newly dirties a chunk is acknowledged only after an index
  naming that chunk is on disk (the chunk file's bytes, then its directory
  entry, then the index), so a block server restarted under a live device
  (decision 51) never drops a write the guest was told it accepted; writes to
  a chunk the index already names dirty rewrite nothing, and a flush syncs
  only the chunks written since the last one.
- **A cache that survives a node reboot.** The unit and the stage are gone
  but the directory remains. The next stage of that volume on that node reuses
  it when `current` on the transport proves nothing newer was published: it
  still names the manifest the cache was built from (its base), or its epoch
  is the cache's own (an epoch is issued to one node, so a generation at that
  epoch was published from this very directory), or there is no `current` and
  the cache descends from nothing. The epoch the node holds now is not the
  test: a late holder renewal can put an older claim back. The dirty chunks are
  then recovered into the next generation. If `current` moved on, the cache is
  discarded whole, dirty chunks included, since a newer lineage exists, and a
  `GenerationLost` event says so. A cache whose index cannot be read is not
  touched and no server is started over it: it may hold writes only a person
  can judge.
- **A cache with no stage (decision 52).** A cache is not garbage. A node
  keeps a small record beside each cache (`<name>.drain.json`, the volume's
  intent, written before the server first starts) so that a node which lost
  its intent records, or whose pod moved while it was down, finds the volume
  again and **drains** it: serve the cache without a key or a mount, publish,
  verify against the store as an unstage does, stop, delete the cache, release
  the holder. It runs at plugin start and on timer passes, retries with a
  growing delay, and raises `DrainStuck` after 30 minutes. A cache with no
  holder file and no proof it was given up (the holder names another node,
  `current` has moved past it, or the PV is gone) is kept and logged once; an
  operator who removes it by hand gives it up. A cache from a build before the
  record is not drained until restaged.
- **The transport it reads and writes through** is the node plugin's own
  transport mount under the kubelet plugin directory, visible on the host by
  propagation. A served volume has an intent record until unstage, and
  intents are what reference-count transports (brief 4.3), so the transport
  cannot be unmounted as idle while a server uses it; no new reference kind
  is needed.
- **Discard.** `TRIM` requests mark whole chunks zero, so a deleted file's
  chunks leave the next generation. The ext4 mount sets `discard`.
- **The hot path** holds no allocations: preallocated aligned buffers, one
  goroutine per request slot. Section 7's gate measures it against a loop
  device on a Pi.
- **Fallback named, not built.** If the gate fails and profiling blames
  scheduling or garbage collection rather than disk or network, the escape
  hatch is the io_uring based `ublk` interface, whose reference library is in
  Rust. The chunk store, manifests and generations do not change for it.

### 4.4 The device stack on the node

```
pod ──bind──▶ stage mount: ext4 (discard, errors=remount-ro)
                 ▲
             dm-crypt (LUKS2; cipher per class, Adiantum default; 4096-byte sectors)
                 ▲
             /dev/nbdN  ◀── miami blockd (transient systemd service) ◀── cache dir ◀── chunk store on the transport
```

- **LUKS2** is formatted at the first stage on the raw nbd device, so the
  header lives in the first chunk; cipher and key size are class parameters
  with `xchacha12,aes-adiantum-plain64` as the default (brief 4.7's reasoning
  for CPUs without AES instructions holds) or `aes-xts-plain64`; the key
  arrives from the StorageClass's node-stage Secret (the node-publish reference
  names the same Secret, so a restage from a publish has it) over `/dev/fd/N`
  and is never persisted or sent to the block server. When a stage finds that
  `key` does not open the volume and the Secret carries `previousKey`, the
  keyslot that opens with `previousKey` is changed to `key`
  (`cryptsetup luksChangeKey`) and the volume opens; the header chunks it
  touches go out with the next generation. The mapping is opened with
  `--disable-keyring`: the key is held by the kernel in the dm-crypt table, not
  in a keyring, because with a keyring key `cryptsetup resize` asks for a
  passphrase to reload it and a node-side expand has none. With the key in the
  table, resize needs no passphrase.
- **Restart model (decision 51).** A killed block server is restarted by systemd
  a second later and, finding its nbd device still present (matched by backend
  identifier), re-points that device with `NBD_CMD_RECONFIGURE` instead of
  connecting a new one. The device, the mapping and the ext4 mount survive: a
  restart needs no pod deletion, no `e2fsck` and no key, and the cache index
  resumes the unpublished writes. The kernel takes a socket only into a slot
  already marked dead, which its receive thread does at once on EOF; a
  reconfigure with no dead slot returns success and drops the socket, so the
  server confirms the device is served by its own socket (a read through the
  device reaching it) before it reports ready. Requests that arrive while the
  connection is dead wait up to the dead-connection timeout (60 s) and then
  fail with EIO. A device whose cache is gone is never re-pointed by building a
  new one; the volume waits for a person.
- **Plain variant** (`encryption: "off"`): ext4 directly on the nbd device.
  The chunk store then holds plaintext ext4 blocks; the class name says so.
- **Formatting and checking** stay the real tools of decision 16: `mkfs.ext4`
  once, `e2fsck -p` on every stage, `resize2fs` online for growth.
- **Growth** is online: `ControllerExpandVolume` records the new size in the
  size record beside the volume's directory in the store (4.2; a PV's
  attributes are immutable, so the volume context cannot carry it; the record
  only grows), and a stage takes the largest of the context's size, that record
  and the current manifest's, so no restage comes back smaller; the node grows
  the nbd device (the server extends its size, new chunks are zero),
  `cryptsetup resize`, then `resize2fs` on the mounted filesystem. Shrink is
  `OutOfRange`.
- **fsGroup** applies: the volume has an fsType and is RWO, so decision 27's
  `ReadWriteOnceWithFSType` policy covers it; uid fidelity is full because the
  ext4 is the node's own.

### 4.5 Generations

A generation is a point-in-time copy of the ciphertext device published to
the chunk store. It runs on the node that holds the volume:

1. **Trigger.** Every `generations.interval` (class parameter, default 5 m,
   at least 1 m) while the dirty set is not empty; when the dirty set reaches
   `generations.dirtyWatermark` (default half the cache bound); when the PV's
   `miami.csi.greghaskins.com/publish` annotation changes value (the node reads
   the PV directly, about once a minute; no controller relays it; a standing
   value after a plugin restart asks once more, which costs a freeze and
   publishes nothing on a clean volume); and always at unstage, after the file
   system is unmounted and the mapping closed. A generation with an empty
   dirty set publishes nothing.
2. **Freeze.** `FIFREEZE` on the stage mount: ext4 commits its journal and
   flushes to the device, writers block in uninterruptible sleep for the
   duration, which is milliseconds. The server marks the boundary: it snapshots
   the dirty set, and from then on a write to a chunk in that set first copies
   the cached chunk (copy-on-write at chunk granularity, inside the cache).
   `FITHAW`.
3. **Upload.** The boundary set's chunks go to the transport under their
   hashes, concurrently, then the manifest, then `current`, in that order.
   The manifest carries the holder epoch (4.6).
4. **Prune.** Manifests beyond `generations.keep` and the chunks only they
   referenced are removed.

A generation that cannot complete (transport unavailable, publish refused)
leaves everything as it was and is retried at the next trigger; the dirty set
only grows. The age of the newest published generation is a metric. A volume
that holds unpublished writes whose newest generation is older than
`generations.maxAge` (default 1 h, at least the interval) is **overdue**: the
`generation-overdue` condition, a `GenerationOverdue` event, and nothing else.
A clean volume is never overdue however long it idles, and overdue is not part
of the `VolumeCondition` (that is the active probe's, 4.9). It is reported, not
remediated.

**Loss window.** On an orderly unstage nothing is lost. When a holder is
abandoned by an operator (4.6) the volume restages elsewhere from `current`,
and the writes since it are gone: at most one interval plus the publish time
while the store was reachable, more if it was not. The restaged filesystem is
crash-consistent, since every generation was taken under a freeze.

### 4.6 Exclusivity, emergency detach and fencing

- **Attach** serialises as in brief 4.5: `attachRequired`, one node at a time.
  On a real cluster a pod on another node does not reach the driver at all
  until the attach-detach controller has moved the attachment.
- **The holder file** `<name>.holder` beside the volume's directory, and the
  transport-clock renewal, are brief 4.5 with one change: **no node takes a
  holder over because it is old or its node is NotReady** (decision 52; the
  brief's takeover conditions 3 and 4 and the timeout `T` are not built, and a
  class that sets `takeoverTimeout` is refused). Each acquisition increments an
  epoch stored in the holder file, claimed first by a durable per-epoch file
  so two holders never share one. Renewal touches the file with `futimens`
  (the server sets the time) after a read-and-check, rather than renaming over
  it.
- **A volume moves only after a verified final sync (decision 52).** The
  holder is released only by a clean unstage (4.7): the final generation
  published, `current` re-read from the store and found to name it, and the
  server reporting nothing dirty and no publish in flight. Anything short of
  that fails `NodeUnstageVolume` as retryable and leaves the server, the cache
  and the holder file as they were, so the volume stays with the node that
  holds the unsynced data. A pod scheduled elsewhere waits with the
  `holder-contended` condition, and the error names the holder and the way out.
- **Orphan drain.** A node that comes back with a held volume it no longer
  stages (a crash, then the pod moved) drains it by itself (4.3).
- **Emergency detach.** The only way a node's data is given up is an
  operator's act, in one of two forms, each abandoning exactly what the holder
  had not published: a PV annotation `miami.csi.greghaskins.com/force-detach`
  whose value is the holder file's `<node>@<epoch>` as read at that moment
  (ignored when it does not match, so a stale annotation can never take over
  its successor), or the standard `node.kubernetes.io/out-of-service` taint on
  the holder's node, which abandons every volume that node holds. The driver
  only reads them (the node's role has `get` on PVs and nodes, nothing else). A
  takeover raises an `EmergencyDetach` event naming the abandoned `<node>@<epoch>`
  and the trigger. `DeleteVolume` is refused while any holder file exists
  unless the same annotation names the holder. `docs/operating-volumes.md` has
  the runbook.
- **The fencing token.** Every manifest carries the publisher's holder epoch,
  and a publisher re-reads the holder file immediately before renaming
  `current`; if the holder is no longer its own, or `current` names a manifest
  with a higher epoch, it refuses, keeps its dirty data in the cache until
  unstage, and reports a `PublishRefused` event. So a node partitioned from
  the NAS, or one an operator detached while its pod kept writing, can never
  make its writes authoritative. Its pod continues on a volume that will be
  discarded at unstage; that is a liveness matter for the operator's detach,
  not a data-safety one. **When that node unstages**, a refused final
  generation is accepted as an abandonment only on proof from the store (the
  holder file names another node, or `current` is at an epoch above ours):
  then the cache is deleted with its dirty chunks (decision 43; a
  `GenerationLost` event), the holder is released only if it is still this
  node's, and the unstage succeeds. A refusal the store does not explain
  (decision 52) is retryable, like any other failure to sync: the node's data
  is not given up by guessing. A holder file that is absent proves nothing (the
  transport may not be mounted).
- **No self-fence of the pod is needed** for data safety. The brief's ext4
  shutdown ioctl and the TCP reset of the NFS socket (decision 8's
  `FenceAfter` and `ResetAfter`) exist to stop a stale holder's in-flight NFS
  writes from landing on the shared image; here nothing a stale holder writes
  reaches the shared store except through a token-checked publish. Nothing
  waits a timeout for a dead node any more; the only bound is the operator.
- **The remaining window.** A publisher that passed its holder check and is
  about to rename `current` while a taker that has just acquired the holder
  publishes its first generation can, in one round trip of bad luck, leave
  `current` on the older epoch until the taker's next generation; a third node
  restaging in that instant would start from the stale holder's last
  generation. Not a corruption, a fork of at most one interval, and
  documented.

### 4.7 Stage and publish

`NodeStageVolume` for a `volume`: prerequisites probed → transport mounted →
holder acquired → `current` read (or absent for a new volume) and the size
raised to the largest any record holds → the binary copy present on the host →
server started as a transient unit (or its running unit adopted) → nbd device
present → LUKS formatted (first stage) or opened → `e2fsck -p` (or `mkfs.ext4`
on a device that is blank and whose volume has never published) → mounted at
the stage path → resize if the recorded size grew. A device that holds data but
no LUKS header or file system signature is never formatted over. Each step is
one `Observe`/`Converge` layer in the existing reconciler; the kernel, sysfs
and systemd are observed, the intent record carries the desired state, and
garbage collection is the same loop with no intent (a served device with no
intent is torn down, and a cache with no intent is drained, 4.3).

Unlike a directory item (decision 25), a `volume` *does* mount at the stage
path: the ext4 mount is a real mount with its own device, so kubelet's
unstage reference check behaves as it does for every block-backed CSI driver.
Publish is a bind of the stage mount, read-only when the request or the access
mode says so.

`NodeUnstageVolume` (decision 52): thaw → unmount → LUKS closed → any publish
already in flight waited out → boundary and final generation → `current` read
back from the store and required to name it, the server required to report
nothing dirty and no publish running → server stopped (the one call that
disconnects the device) and the unit stopped → cache directory deleted →
holder released. Any step short of the verification fails the unstage as
retryable and leaves the server, the cache and the holder alone; a busy mount
is an error before anything past it is touched. The cache is deleted on every
clean unstage by design (the owner's call: a node's disk must not fill with
copies of volumes it no longer serves). Returning to the same node fetches the
volume again.

The controller for a `volume`: `CreateVolume` makes the directory and writes
nothing else (the first stage formats and publishes generation 1);
`DeleteVolume` is refused while any holder file exists beside the directory,
however old, and removes the directory, the holder files and the size record
once none does (or once the PV's force-detach annotation names the holder,
4.6); `ControllerExpandVolume` records the size.

### 4.8 RWX: re-export from the holder node (M3)

Brief 4.9 unchanged in mechanism, with `volume` in the place of the image:
the holder node stages the volume as above and exports the stage mount over
NFSv4 to the cluster's nodes; other nodes mount it as an `export` transport
and bind it into pods; client markers, promotion on takeover and the security
conditions of decision 11 apply. Readers that need no write mount the claim
read-only. The costs are the brief's: two network hops for remote writers and
the holder as the load-bearing node until takeover.

### 4.9 Failure and remediation

| Failure | What happens | What the driver does |
|---|---|---|
| NAS unreachable | pods keep running on local data; generations wait | reports (transport gauge, generation age); nothing else |
| NAS unreachable and the cache fills with dirty chunks | writes get I/O errors; ext4 goes read-only and stays so until remounted | reports (the probe's `VolumeCondition`, `VolumeUnhealthy`, `miami_volume_probe_ok`, `GenerationOverdue`, dirty bytes) with a message to restart the pod once the store is back and the cache has drained; nothing else (decision 53) |
| block server dies | requests wait while the connection is dead, up to the dead-connection timeout (60 s); systemd restarts the server within a second | systemd restarts it; it re-points the same nbd device from the cache index (dirty data intact); no pod deletion, no `e2fsck`; a `ServerRestarted` event (decision 51) |
| node dies | the pod and attachment stay with the dead node; the volume stays held | nothing by itself: a pod elsewhere waits as `holder-contended`. The operator's emergency detach (annotation or `out-of-service` taint, 4.6) abandons what the holder had not published and restages from `current`; the node, if it returns first, drains (4.3) |
| plugin pod restarts or upgrades | nothing: the server, device and mounts are the kernel's and systemd's | reconciles from observation |

Brief 4.6's active probe stays, reading the first sector of the device the file
system sits on (the mapper, or the nbd device of a plain volume) directly, and
failing a volume after two failures in a row. A full cache that still has the
first chunk resident serves that read, so the probe alone does not see every
full-cache case; the dirty-bytes metric and the overdue condition do. The
brief's remediation package, its pod-deletion admission policy and the
`--remediate` opt-in are not built (decision 53). Brief lesson 12's `ESTALE` on a
file held open does not arise, since no pod holds a file open on the NAS.

### 4.10 The one non-transparent semantic

A workload that holds a database open on a `volume` sees exactly local ext4;
`flock`, `fcntl`, `mmap` and exec bits are the node kernel's. The single thing
a consumer must know is in RWX (4.8): remote pods get NFSv4 close-to-open
semantics with real cross-node locking, as the brief already documents.
Nothing else in this design leaks into a pod manifest.

### 4.11 Observability

Added to brief 4.12's set: served volumes by server binary hash; generation
age, count and last publish duration; dirty bytes and cache fill per volume;
chunk fetch latency and failures; publishes refused by the token; prunes.
Events on the PV for publish refused, generation lost, emergency detach,
generation overdue, volume unhealthy, key required, server restarted, and a
stuck unstage or drain. The names and what they mean are in
`docs/operating-volumes.md`. While a block server is down, its per-volume
series are absent; the alert is on the server's state (`miami_volumes_served`).

### 4.12 Security posture

- **Threat model, stated.** Encryption at rest protects against the NAS, its
  snapshots and the offsite provider. The key is in the node kernel's mapping
  and plaintext is in the node's page cache, as with any dm-crypt volume; the
  node's own disk holds only ciphertext (the cache is chunks of the encrypted
  device). A compromised node is outside the model, as the brief's "nodes are
  trusted" boundary already says.
- **The access gate** for a volume is the namespace's Secret, referenced by the
  class's node-stage Secret template, plus the storage-isolation admission
  policy a cluster already applies to the transport subtree. Any pod can mount
  the export inline and read chunks; they are ciphertext.
- **New privileges**: the host's `/dev` (hostPath, `HostToContainer`, so the nbd
  and device-mapper nodes that appear after the container starts are seen),
  `/run/systemd` (read-only: connecting to the private socket needs write
  permission on the socket file, not a writable mount) and `/sys` (read-only,
  for the nbd entries); the nbd module; the cache directory and the binary copy
  under the kubelet plugin directory, which the container already shares with
  the host, so they need no hostPath of their own. No `hostPID`. The driver
  ships no admission policy for any of it (a cluster's own policies cover
  hostPaths), and the node's API access is `get` on PVs and nodes plus events.
- **The transient unit** runs the binary from the kubelet plugin directory,
  which only the node plugin writes; the unit's command line carries the
  volume name and paths, never the key, which the server never sees.
- **Every mount** the driver makes stays `nosuid,nodev`.

## 5. Operations

### 5.1 Restore drill

From the NAS share or from the offsite bucket pulled into a directory, on any
Linux machine with coreutils and cryptsetup:

```sh
cd pvc-<uuid>
m=manifests/$(cat current)
grep -v '^#' "$m" | sed 's|^\(..\)\(.*\)|\1\2  chunks/\1/\1\2|' | sha256sum -c --quiet
grep -v '^#' "$m" | sed 's|^\(..\)|chunks/\1/&|' | xargs cat > volume.img
cryptsetup open volume.img vol      # the namespace's key
mount /dev/mapper/vol /mnt
```

Line three verifies every chunk against its name, the all-zero chunk included:
every writer stores it before a manifest that names it (4.2). If a chunk is
missing from an offsite copy taken mid-publish, use the previous manifest in `manifests/`.
The driver ships `miami restore` doing the same with a bounded cache, for a
volume larger than the restoring machine's disk; the drill does not depend on
it. `docs/restore.md` carries the drill and `hack/restore-drill.sh` runs it; a
test keeps the three identical.

### 5.2 Layout and backup membership

The backed-up tier is one export the backup job selects whole, once, so the
job's path-dropping never bites and a new namespace needs no change; every
class bound to it is an encrypted one, so "on that export" and "ciphertext"
are the same statement. A scratch tier is a second export the job never
selects, for volumes whose generations are not worth uploading (agent session
workspaces). Both are decision 12.

Holder files are renewed every renewal interval, so every backup run uploads
one small object version per RWO volume; accepted (3.3).

### 5.3 Migration

From an existing file-backed driver's images: `miami import` reads a raw block
image file (the transport mounted, the workload scaled to zero) and writes it
as-is as manifest `0.1` of a new `volume` (epoch 0, below the epoch 1 of the
first holder; padded with zeros to a whole number of chunks; refused if the
volume already has a `current`, a manifest or a holder file), then the
static-PV dance of decision 4. The image's own format decides the class: a LUKS2 image whose keyslot opens
with the namespace's key imports into an encrypted class and keeps its header;
a plain ext4 image imports into a `-plain` class. Re-encrypting a plain image
is a copy through a staged volume, not an import. From a hard-mounted NFS
directory: stage a new `volume`, copy, switch the claim. The order of
consumers is a deployment's own spec.

### 5.4 Blob storage as a layout

A content-addressed store is a `volume` laid out by its application: files
named by their plaintext hash under a fan-out, written to a staging directory
and renamed into place, with the application owning the hash check. The
store's application layer (an S3 facade, for instance) is the credential gate
for its namespace and the only mounter. The driver adds nothing for it, and
the volume's generations give it a point-in-time offsite copy for free. A
store larger than a node's disk sets `cacheSize` below its size and runs
lazily; its chunk size should be small (1 to 4 MiB) to bound read
amplification on cold blobs.

## 6. Decisions

Appended to the brief's log as 36 to 59; the brief's numbering continues. 36 to
50 are the design pass; 51 to 53 were recorded while the plan ran and are folded
into section 4; 54 to 59 record where the merged code settled a point the design
left open or put differently.

36. **The `volume` kind replaces the image item.** Ext4 over dm-crypt over a
    chunk-backed nbd device whose live data is on the node and whose
    generations are on the transport. No kernel filesystem is ever stacked
    over NFS (facts 3.2 and 3.6). The brief's image item is not built.
37. **Encryption is on by default** for every `volume`; `-plain` marks the
    exception. LUKS2 with Adiantum default, keyslot rotation, key over
    `/dev/fd` (brief 4.7 unchanged in mechanism). Decision 35's `-crypt`
    token is withdrawn.
38. **No FUSE layer and no second format.** gocryptfs was measured viable over
    NFS and rejected for the local store (3.4); rclone's crypt was rejected
    for in-place writes. Lesson 8's one inherent cost, daemon lifetime, is
    recorded in 3.7 with its mitigations; FUSE is not rejected wholesale, it
    has no use left here.
39. **Generations are the backup unit**: point-in-time by freeze, incremental
    by content-addressed chunks, published by rename, retained by count.
40. **Restore needs coreutils and cryptsetup**, nothing of the driver's.
41. **The block server is a transient systemd service** executing a host copy
    of the driver binary; its upgrade is the volume's next stage; no
    `hostPID`.
42. **nbd, in Go, in the one binary**, with a no-allocation hot path and a
    throughput gate; `ublk` in Rust is the named fallback, not built.
43. **The cache is deleted at unstage**, after the final generation.
44. **No era target**: the server tracks the dirty set itself.
45. **Fencing is the holder epoch in the manifest**, checked at publish. The
    ext4 shutdown ioctl and the TCP reset are not built; `T` defaults to 5 m
    and is no longer bound to the NFS major timeout.
46. **`noacl` joins the nfs3 profile**, adopted without conflict: a transport
    mounted without it is not reported as foreign (its absence breaks nothing
    the driver fences on), the drift is logged once, and the option is picked
    up at the next mount.
47. **Backup membership is the export** (decision 12 confirmed by 3.3); the
    backed-up export is selected whole; holder churn is accepted.
48. **Blobs are a layout, not a kind** (5.4); the S3 service of brief 5.3 is
    dropped.
49. **SMB is closed** on 3.5; the transport layer stays the extension point.
50. **Milestones re-planned** (section 7): M2 `volume` RWO with generations;
    M3 RWX by re-export; M4 snapshots and clones; node-local persistent and
    `miami move` dropped; read-only generation followers, encrypted shared
    directories and lazy fetch through anything but the server itself
    unscheduled.
51. **A restarted block server re-points the same device** (supersedes the
    recovery shape of 41). A new `miami blockd` for a volume whose nbd device
    still exists (matched by backend identifier) does not connect a new
    device: it sends `NBD_CMD_RECONFIGURE` with the new socket and the same
    backend identifier, and no size attributes (a size change notifies a
    uevent). The device, the dm-crypt mapping and the ext4 mount survive, so
    a restart needs no pod deletion, no `e2fsck` and no key. Read from the
    kernel source (v6.8 and v7.0, identical here): the kernel takes a socket
    only into a slot already marked dead, which the receive thread does at
    once on EOF; a reconfigure with no dead slot returns success and drops
    the socket, so the server confirms the device is served by its own
    socket (a read through the device reaching it) before it reports ready.
    Requests arriving while the connection is dead wait up to the
    dead-connection timeout (60 s by default) and then fail with EIO; the
    unit restarts the server well inside it. The sysfs `pid` keeps the
    connecting process's pid across a reconfigure, so it is not a liveness
    signal: liveness is the unit's main process answering on the volume's
    control socket. A server stopping on request (unstage) still disconnects
    the device; a live binary upgrade is possible on this path and not built.
52. **A volume moves only after a verified final sync** (supersedes the
    timed takeover of 4.6 and brief 4.5, and narrows decision 43). The holder
    is released only by a clean unstage: unmount, close the mapping, the
    final generation published, `current` re-read from the store and found to
    name it, and the server reporting nothing dirty and no publish in flight;
    only then are the server stopped, the cache deleted and the holder file
    removed. Anything short of that fails `NodeUnstageVolume` as retryable
    and leaves the server and cache in place, retrying, so the volume stays
    with the node that holds the unsynced data; a pod scheduled elsewhere
    waits as `holder-contended`, naming the holder. No node takes over a
    holder because it is old or its node is NotReady. A node that comes back
    with a held volume it no longer stages (a crash, then the pod moved)
    drains it on its own: it serves the cache without a key or a mount,
    publishes, verifies as above and releases. The emergency detach, which
    abandons whatever the holder had not published, is an operator act: a PV
    annotation naming the exact holder (`<node>@<epoch>`, ignored when it
    does not match the holder file), or the standard
    `node.kubernetes.io/out-of-service` taint on the holder's node, which
    abandons every volume that node holds. Both raise an event naming the
    abandoned epoch. The epoch fence (45) is unchanged and is what keeps an
    abandoned node's late publishes out after it returns; a fenced node's
    unstage still discards and succeeds, since its data was given up by the
    operator.
53. **No automatic remediation in M2; failures are reported** (narrows 4.9
    and the brief's 4.6 remediation package). After 51 and 52 the only case
    left for a pod deletion is ext4 gone read-only after the cache filled
    while the store was unreachable, and that pod can only be restaged once
    the store answers again (its unstage must verify a final generation).
    The driver reports it (the `VolumeCondition`, an event, the probe
    metric) with a message saying to restart the pod once the store is back;
    it holds no pod-delete permission and ships no admission policy. A later
    milestone can add the remediation if the case proves to matter.
59. **The cache-full case is signalled by dirty bytes and the overdue condition,
    not reliably by `VolumeCondition`** (clarifies 53, which is kept). The probe
    reads one sector, which a full cache that still holds that chunk serves.
    `miami_generation_dirty_bytes` near the cache bound, the `GenerationOverdue`
    event and the `generation-overdue` condition are the signals; the probe's
    report is a bonus when reads fail.
54. **The cache and the binary copies live under the kubelet directory** (amends
    4.3, which had a hostPath the overlay chooses). The container and the host
    must see them at the same absolute path, and the kubelet directory is the
    one mount the node pod already shares with the host, so
    `<kubelet-dir>/plugins/<driver>/cache` and `.../bin` need no hostPath of
    their own. `-cache-dir`, `-bin-dir` and the repeatable `-shared-dir` move
    them; a layout outside the shared directories, or a cache directory over 88
    bytes (the control socket `<cache-dir>/<16 hex>.s` must fit 107), is
    reported as the `cache-dir` prerequisite missing. Socket names are a hash
    of the volume name so the length does not vary.
55. **"Publish now" is a PV annotation** (amends 4.5, which had the controller
    relay a PVC annotation). The node reads `miami.csi.greghaskins.com/publish`
    on the PV about once a minute; a changed value asks for one generation.
56. **Overdue is reported, and is not a `VolumeCondition`** (amends 4.5). A
    volume with unpublished writes whose newest generation is older than
    `generations.maxAge` has the `generation-overdue` condition and a
    `GenerationOverdue` event. The `VolumeCondition` is the active probe's
    alone, because the probe is the only signal that the device itself is
    failing. The probe reads the device's first sector, so a full cache that
    still has that chunk resident serves it: decision 53's report of the
    cache-full case then rests on the dirty-bytes metric and the overdue
    condition, not on the probe.
57. **A surviving cache is reused on `current`'s epoch or base** (amends 4.3).
    The witness that nothing newer was published is that `current` still names
    the cache's base manifest, or carries the cache's own epoch; never the
    epoch the node holds now, which a late holder renewal can set back. A cache
    that fails the test is discarded with its unpublished chunks reported.
58. **The mapping is opened with `--disable-keyring`** (amends 4.4's "held by
    the kernel in the mapping"). The key sits in the dm-crypt table, which is
    what lets `cryptsetup resize` and an online expand run without a
    passphrase. Related and also settled by the merged code: `DeleteVolume` is
    refused while any holder file exists (4.7), the unit restarts on failure
    after one second (4.3), and a CSI spec module pinned at v1.12.0 keeps the
    alpha `VolumeCondition` field the node answers with, until the kubelet
    adopts the volume-health API that replaced it in v1.13.0.
60. **Gate G1 judges each phase on the median of rounds 2 to N, run on a quiet
    node** (amends 3.2's "the second round is the one compared"). Round 1 is
    warm-up; the figure per phase and stack is the median of the remaining
    rounds (the mean of the middle two when the count is even), so with two
    rounds it is round 2 as before. Run-to-run noise on the Pi is about 50%,
    so the gate runs `--rounds 5` on both stacks, and `--compare` shows the
    min and max of the rounds used and refuses reports whose round counts
    differ or that have fewer than two. The 0.05 s floor applies to the
    medians.
61. **A rollback guard keeps a per-volume high-water mark outside the store**
    (adds to 4.2 and 4.7; `2026-11-07-rollback-guard-design.md`). The newest
    published generation is recorded on the PV as
    `miami.csi.greghaskins.com/high-water`, and a stage whose data would be
    older than the mark is refused with the `rollback-detected` condition, a
    `RollbackDetected` event and a message saying how to proceed, before any
    cache is discarded; a reused cache at or above the mark is not refused. The
    mark is written at a verified unstage and at most once per interval after a
    publish (recommended; per-publish is the alternative, pending the owner).
    The node role gains `patch` on persistentvolumes, which a
    ValidatingAdmissionPolicy can narrow to the annotations the driver writes.
    An absent mark is no mark. Manifest signing is deferred until the export is
    shared with an untrusted party.
62. **A deliberate rollback is accepted by exact generation** (adds to 4.5; same
    document). The PV annotation `miami.csi.greghaskins.com/accept-generation`
    names the generation `current` holds and lets that one stage proceed; the
    next publish lands as a new higher generation. Generations newer than the
    rollback point are held from pruning, recorded in
    `miami.csi.greghaskins.com/held-generations`, until an operator removes the
    annotation. Clone from a generation (M4) is the primary way to go back in
    time and shares the pin mechanism.

## 7. Milestones

1. **M2, chunk-backed volumes, RWO.** Gates first: (a) the block server's
   throughput on a Pi against a loop device on local disk, on the 3.2 phases,
   pass within 1.5× on every phase; (b) the e2e harness runs the transient
   unit, the nbd module and a NAS outage past the major timeout with a pod
   writing, and shows no hung-task message and no data loss across a restage.
   A failed gate stops the milestone and goes back to the owner with the
   measurement and the fallback's cost; the chunk store, manifests, restore
   and import do not depend on the server's data path and may proceed
   meanwhile, the server's data path does not ship.
   M1 built none of the brief's M2 machinery, so M2 builds all of it that this
   design still needs: the holder file and its renewal, the epoch and token
   check, the active probe and `VolumeCondition` for volumes (brief 4.6), the
   emergency detach and the orphan drain of decision 52 (in place of the
   brief's takeover conditions and two-clock rule, which are not built), and
   the keys riding the publish Secret so that a restage after a plugin restart
   has them (a block-server restart no longer restages, decision 51); not the
   remediation package or its pod-deletion admission policy (decision 53), nor
   the ext4 shutdown, the TCP reset or the fence timing (decision 45).
   Then: `noacl`; the chunk store and manifests; `miami blockd`; the device
   stack with LUKS2; stage, publish, unstage with the verified final sync, cache
   deletion and the reboot-survival rule; generations with all four triggers;
   online growth; `miami restore` and `miami import`; metrics and events; the
   e2e matrix extended with server kill, plugin restart under a served volume,
   node reboot with a surviving cache, an emergency detach of a stopped holder
   and an orphan drain, and a restore drill run from the test; the README
   matrix and class examples.
   **Node prerequisites**, each probed on every reconcile pass and reported as a
   named error when missing, in the style of decision 27(h): the `nbd` netlink
   family, the device-mapper control node and the Adiantum cipher (standard in
   Ubuntu-class kernels on both architectures; the latter two only for an
   encrypted volume), systemd as the host's init with its private socket
   mountable, and a writable cache directory. `FIFREEZE` is not probed: a
   freeze that fails fails that generation. A node missing one serves directory
   items only. The e2e harness runs on kind nodes, which run systemd and share
   the runner's kernel, so the runner must have the `nbd` module; verifying
   that on both runner architectures is the first task of the plan.
   First consumers, in a deployment's own spec: a budgeting application, a
   home-automation server, then an agent session workspace on the scratch
   export.
2. **M3, RWX by re-export** (4.8), with readers mounting read-only; the holder
   kill e2e with remote writers. The content-addressed store's application
   moves to a `volume` here or in M2 as its deployment prefers.
3. **M4, snapshots and clones.** A Kubernetes `VolumeSnapshot` is "publish a
   generation now and pin it from pruning"; a volume created from a snapshot
   starts from that manifest. CSI's snapshot and clone capabilities, no new
   on-disk shape.
4. **Unscheduled, shaped by this design:** read-only followers of
   generations on other nodes (pin at mount, or live switch per open, which
   needs host-to-container propagation in the consumer); the era target or
   `ublk` if the gates say so; chunk-level deduplication across volumes.
5. **Dropped:** the loop-over-NFS image; node-local persistent with `miami
   move`; the S3 service; encrypted shared directories; SMB.

## 8. Left to the builder

- Manifest header field names and the exact text format, within section 4.2's
  constraints; the chunk temporary-name convention.
- The nbd library and the netlink path for device creation; the dead-
  connection and request timeouts' values (bounded, documented, below the
  hung-task threshold is not required since nbd waits are not the plain
  uninterruptible kind, but keep them short).
- The cache index format and how a restarted server validates it.
- The transient unit's properties (`CollectMode`, `KillMode`, stdout to the
  journal, the unit name carrying the volume name).
- Concurrency of chunk uploads and fetches, and the prefetch rate limit.
- How `generations.interval` and the other class parameters are validated and
  carried in the volume context, following decision 28's pattern for `mode`.
- The e2e harness changes: nbd on kind nodes (kind nodes run systemd and share
  the host kernel, so the module is the runner's), the transient-unit path from
  inside the node pod, and the restore drill as a test.
- The content of any admission policy for the new hostPaths (the base ships
  none; it is a cluster's own).

## 9. Resuming from this document

A new session continues from here with no other context:

1. Read `README.md`, the brief (its sections stand; decisions 36 to 59 win
   where they differ), `2026-10-31-working-mode.md`, then this document.
2. Confirm the secret-scan config (`hack/secret-scan/require-config.sh`).
3. The spec is approved. The plan is `2026-11-03-m2-implementation-plan.md`
   on the same branch; its work breakdown says which tasks are merged (the
   pull request's commits and `git log` are the truth). Execute the next
   incomplete wave with one subagent per task per the working-mode document,
   on the same branch and pull request.
5. The migration of consumers is a deployment's own specification, written
   after the plan; nothing environment-specific belongs here.
