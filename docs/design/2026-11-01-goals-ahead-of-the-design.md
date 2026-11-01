# Goals ahead of the design

**Status:** Handoff input for the next design pass
**Created:** 2026-11-01
**Reads with:** `README.md`, `2026-10-30-architecture-brief.md`

The README now describes csi-miami's desired state. The architecture brief
was written before some of those goals were stated, so in places it is
narrower than the README or contradicts it. This document lists each gap so
the next design pass can close it. Where the two disagree, the README is the
intent. Nothing here is a decision about mechanism; the open questions are
for the design pass to answer.

## 1. The project goal

**README:** make bare-metal hardware behave like a proper Kubernetes storage
layer. csi-miami is responsible for:

1. Flexibility across directories, images and blobs, on local disks and a
   remote NAS.
2. Strong RWO and RWX semantics, available when a workload needs them.
3. Encryption at rest.

All of it should be mostly transparent to workloads, especially open source
projects run off the shelf in a self-hosted setup.

**Brief:** the opening states a narrower reason to exist: NFSv3 loses mode
bits and locking, so give each pod a real ext4 over a loop device. That
stays true, but it is now one case of the goal rather than the goal.

**Gap:** restate the brief's purpose and principles around the three
responsibilities and the transparency requirement.

## 2. Four independent dimensions

**README:** each StorageClass picks one option per dimension, in any
combination:

| Dimension | Options |
|---|---|
| Location | local disk, NAS (NFSv3) |
| Item type | flat files in a directory, filesystem image, opaque blobs |
| Sharing | single node (RWO), shared (RWX) |
| Encryption | off, on |

**Brief:** the dimensions are coupled. Encryption exists only for images
(4.7). Directory items are always `ReadWriteMany` with no holder and no
fencing (4.8). Local images are "single-node by construction" with no
re-export (5.2). There is no blob item type.

**Gap:** the layer model (transport, item, mount in 4.1) already separates
location from item type. Sharing and encryption need to become their own
layers or properties that apply to every item type, rather than features of
one item type. The combinations below are the ones the brief does not
cover yet.

## 3. Blobs as a third item type

**README:** item types are flat files in a directory, a filesystem image, and
opaque, content-addressed blobs. An S3 interface on top is out of scope for
csi-miami.

**Brief:** section 5.3 and milestone 5 describe an S3-compatible object
service built on an image volume. There is no blob item.

**Gap:** replace 5.3 with a blob item type and drop the S3 service from
scope. A separate project may still put an S3 interface on top.

**Open questions:**

- What a pod sees when it mounts a blob volume: a read-only tree of
  content-addressed files, a writable store with an ingest path, or
  something else.
- Where the content addressing happens (in the driver or in a client), and
  how deduplication interacts with encryption (convergent encryption leaks
  equality of content).
- How garbage collection and reference counting work across volumes that
  share blobs, if they can.

## 4. Encryption for every item type

**README:** encryption is optional on any item type.

**Brief:** LUKS2 on images only (4.7; decision 17). Directory items have no
encryption.

**Gap:** a design for encrypted directories and encrypted blobs.

**Open questions:**

- The mechanism for directories. Native filesystem encryption is not
  available on an NFSv3 export. A FUSE layer (for example gocryptfs) runs
  into lesson 8 and decision 6, which rejected FUSE-backed volumes as the
  most fragile thing a node plugin can carry.
- Whether key handling (node-stage Secret, `previousKey` rotation, keys never
  persisted) can be shared with the image design unchanged.
- What a shared encrypted directory needs: every node that mounts it needs
  the key, and the NAS sees only ciphertext.

## 5. Strong RWO and RWX for every item type

**README:** single node and shared are both options for every item type, and
the semantics should be strong.

**Brief:**

- Directories: RWX with close-to-open consistency; cross-node `flock` does
  not work on a `nolock` transport, so mutual exclusion goes through
  `mkdir`/`O_EXCL` and the `miami lock` helper (4.8). A single-node directory
  gets only attach-level exclusivity, with no holder and no fencing.
- Images: strong RWO through attach plus the holder and fencing (4.5). RWX
  through the holder node's NFSv4 re-export (4.9), which gives real
  cross-node locking.
- Local items: single node only (5.2).

**Gap:**

- Strong RWO for directories: whether directories get the holder file and
  fencing that images have.
- Strong RWX for directories: the brief's contract needs a wrapper
  (`miami lock`) for cross-node locking, which conflicts with transparency
  for off-the-shelf workloads. Options include serving shared directories
  through the same NFSv4 re-export as RWX images, so `flock` works across
  nodes.
- Shared local items: a local disk shared across nodes would need the holder
  node's re-export, which 5.2 currently uses only for `miami move`.

## 6. Transparency to workloads

**README:** all of the above should be mostly transparent to workloads,
especially off-the-shelf open source projects.

**Brief:** most of the design already keeps pod manifests unchanged (RWX
images need only a StorageClass parameter and an access mode, 4.9). The
exceptions are the `miami lock` helper for cross-node locking on directories
(4.8) and the documented NFS close-to-open contract.

**Gap:** state transparency as a principle in section 3, and treat any
semantics a workload must know about as a cost to justify.

## 7. Milestones

**README:** shows no milestones, only a matrix of what is supported today
(directories on local disk and the NAS, single node and shared, unencrypted:
in progress; everything else: not yet).

**Brief:** milestones M1 to M4 plus the S3 service (section 10). Blobs,
encrypted directories, strong RWO directories and shared local items have no
milestone.

**Gap:** once the design covers the items above, re-plan the milestones
against the full matrix and drop the S3 service.

## 8. SMB as a second NAS transport

**Owner's note (2026-11-01):** the NAS also serves SMB, which may offer
better options than NFSv3. Early experiments with SMB, before this driver
existed, did not go well.

**Brief:** the only remote transport is `nfs3` (4.2); the NFSv3 limits it
documents (no server-enforced cross-node locks, `nolock`, `ESTALE` on a
file held open across a server restart, close-to-open consistency) shape
the lock helper (4.8), the fencing design (4.5) and lesson 12.

**Gap:** evaluate an `smb3` transport as a candidate for the location
dimension. The transport layer is the extension point, so it is additive:
the same items, the same reconciler, the same e2e harness with a Samba
container in place of the NFS server.

Where SMB3 could be better:

- Locks are server-enforced and tied to the client session: byte-range
  locks and leases are released when a node dies. That is what the fencing
  design has to approximate on NFSv3 with holder files and timeouts. For an
  image item, a lock on the image file could be the holder, with no clock
  dependence.
- Reconnect is part of the protocol. Durable handles survive a server
  restart, so the `ESTALE` case of lesson 12 and the soft-mount timeout
  (decision 30) mostly stop mattering.
- An image item needs only one large file served with correct locking and
  reconnect, which SMB does well; it hides every POSIX-fidelity problem
  inside the image.

Where it is worse, and the likely reason the early experiments failed:

- Directory items get weaker POSIX semantics unless both ends speak the
  SMB3 POSIX extensions: mode bits are faked client-side (`file_mode`,
  `dir_mode`), symlinks need `mfsymlinks`, and sockets, FIFOs and hard
  links do not exist. Off-the-shelf workloads writing straight to a
  directory volume hit that quickly. NFSv3 at least stores real modes on
  the server.
- Credentials: every SMB mount needs a username and password, so the
  driver would grow a Secret reference per StorageClass and a credential
  path through the node plugin. NFSv3 has none today.
- Client robustness: the Linux `cifs` client has a longer history of hangs
  under reconnect than the NFS client, and its soft and hard timeout
  behaviour is less tunable. For this driver's standard that needs
  measuring on the real kernels, not assuming.

**Open questions:**

- The NAS's Samba version, and whether it offers the SMB3 POSIX extensions,
  durable or persistent handles, and lease breaks. This one fact decides
  whether directory items are viable over SMB at all; check it before any
  design work.
- Whether server-side locks can replace the holder-file fencing design for
  images, and what the takeover timeout becomes when the server releases a
  dead node's locks itself.
- Mode-bit fidelity for directory items against the brief's required
  capabilities (section 1).
- Whether the e2e harness can run a Samba server with the same options the
  NAS uses, so the transport is tested under CI like `nfs3`.
